"""PostgreSQL Protocol implementation

$Id$

THIS SOFTWARE IS UNDER MIT LICENSE.
(C) 2006 Perillo Manlio (manlio.perillo@gmail.com)

Read LICENSE file for more informations.
"""


from struct import pack, unpack
import md5

from twisted.python import log
from twisted.internet import protocol, defer


# protocol version
PG_VERSION_MAJOR = 3
PG_VERSION_MINOR = 0
PG_PROTO_VERSION = (PG_VERSION_MAJOR << 16) | PG_VERSION_MINOR 

# connection status
CONNECTION_STARTED = 0 # waiting for connection to be made
CONNECTION_MADE = 1 # connection ok; waiting to send
CONNECTION_AWAITING_RESPONSE = 2 # waiting for a response from the
                                 # server # XXX what response?
CONNECTION_AUTH_OK = 3 # received authetication; waitig for backend
                       # start-up finish
CONNECTION_SSL_STARTUP = 4 # negotiating SS encryption
CONNECTION_SETENV = 5 # negotiating enviroment-driven parameter
                      # settings # XXX what is this?
CONNECTION_OK = 6 # connection ok; backend ready
CONNECTION_BAD = None # connection procedure failed

# transaction status
PGTRANS_IDLE = "I" # currently idle
PGTRANS_INTRANS = "T" # idle, in a valid transaction block
PGTRANS_INERROR = "E" # idle, in a failed transaction block
PGTRANS_ACTIVE = "A" # a command is in progress (use only in the frontend)
PGTRANS_UNKNOWN = None # the connection is bad # XXX

PG_HEADER_SIZE = 5 # 1 byte opcode + 4 byte lenght



class PgConnectionOption(object):
    """Class for storing connection options.

    XXX TODO
    """

    def __init__(self, **kwargs):
        self.keywords = kwargs
        self.envvars = {}
        self.compiled = {}
        self.options = {}


# exception class hierarchy
class Error(Exception):
    pass

class PgError(Error):
    """XXX TODO
    """
    
    def __init__(self, args):
        self.args = args

    def __str__(self):
        return str(self.args)

class InvalidRequest(Error):
    pass

class AuthenticationError(Error):
    pass

class UnsupportedError(Error):
    pass

class PgResult(object):
    """XXX TODO
    """

class PgCancel(object):
    """XXX TODO
    """

class PgRequest(object):
    """A wrapper for a request to the backend.
    """
    
    def __init__(self, opcode, payload):
        self.opcode = opcode
        self.payload = payload

        self.deferred = defer.Deferred()


class PgProtocol(protocol.Protocol):
    """The PostgreSQL protocol implementation, version 3.0.

    PostgreSQL support multiple request, but we choose to send only
    one request at time, since life is much easier.

    We also choose to not support multiple result set (for simple
    query with multiple commands), but you can retrieve all the result
    by suppling your own RowConsumer.

    Whenever possible, we try to follow the interface of libpq.
    """

    _buffer = ""

    status = CONNECTION_STARTED
    transationStatus = PGTRANS_IDLE
    parameterStatus = {} 
    
    lastResult = None
    lastNotice = {}
    lastError = {}
    
    protocolVersion = 3.0 # we support only this
    serverVersion = None # XXX
    backendPID = None

    
    def __init__(self):
        # cancellation key used for cancel a query in progress
        self.cancelKey = None
        
        self._queue = [] # we queue requests to backend
        self._last = None # last request we made

    def connectionMade(self):
        self.status = CONNECTION_MADE

        self.factory.clientConnectionMade(self)

    def connectionLost(self, reason=protocol.connectionDone):
        #log.msg("connection lost", reason)
        
        self.status = CONNECTION_BAD
        self.transactionStatus = PGTRANS_UNKNOWN
    
    def dataReceived(self, data):
        """Handle raw data arrived from postgres backend.

        XXX TODO use cStringIO as in pgasync, but DO tests.
        """

        self._buffer = self._buffer + data
                                          
        while len(self._buffer) > PG_HEADER_SIZE:
            # read the message header
            opcode, size = unpack("!cI",
                                  self._buffer[:PG_HEADER_SIZE])

            size = size - 4 # the lenght count includes itself
            if len(self._buffer) < size + PG_HEADER_SIZE:
                break
            
            payload = self._buffer[PG_HEADER_SIZE:size + PG_HEADER_SIZE]
            self._buffer = self._buffer[size + PG_HEADER_SIZE:]

            self.messageReceived(opcode, payload)


    def sendMessage(self, request):
        """Send the given message to the backend.
        """
        
        self._queue.append(request)
        self._flush()

        return request.deferred

    def _flush(self):
        # send the next queued request

        if self._queue:
            request = self._queue.pop(0)
            
            self._last = request
            self.transactionStatus = PGTRANS_ACTIVE
            
            self._sendMessage(request.opcode, request.payload)

    def _sendMessage(self, opcode, payload):
        # internal helper

        header = pack("!cI", opcode, len(payload) + 4)
        self.transport.write(header + payload)

        log.msg("request sent:", opcode)
            
    def messageReceived(self, opcode, payload):
        """Handle the message.
        """

        log.msg("message received:", opcode)
        
        # dispatch the message using python introspection
        method = getattr(self, "message_" + opcode, None)
        
        if method is None:
            error = InvalidRequest(opcode)
            
            # XXX
            if self._last is not None:
                self._last.deferred.errback(error)
            else:
                log.err(error)
            
            # we close the connection, as suggested in the protocol
            # specification
            self.transport.loseConnection()
            return
 
        method(payload)

     
    #
    # backend messages handling
    #
    # Start-Up
    #
    def message_E(self, data):
        """ErrorResponse: an error occurred.

        For error message types see protocol documentation.
        """
        
        error = {}
        for item in data.split("\0")[:-2]:
            key, val = item[:1], item[1:]
            error[key] = val

        self.lastError = error
        log.msg("ERROR:", str(error))
        
        # check if we failed the authentication
        if self._last.opcode is None:
            self.status = CONNECTION_BAD
            self._last.deferred.errback(PgError(error))
            self.transport.loseConnection()
        
    def message_N(self, data):
        """NoticeResponse: a notice from the backend.

        For warning message types see protocol documentation.
        """

        notice = {}
        for item in data.split("\0")[:-2]:
            key, val = item[:1], item[1:]
            notice[key] = val

        self.lastNotice = notice
        
        log.msg("Notice:", str(notice))
        
    def message_R(self, data):
        """Authentication: authentication request.
        """

        (authtype,) = unpack("!I", data[:4])

        method = getattr(self, "_auth_%s" % authtype, None)
        if not method:
            error = UnsupportedError("Authentication",  authtype)
            
            self._last.deferred.errback(error)
            self.transport.loseConnection()

        self.status = CONNECTION_AWAITING_RESPONSE # XXX

        method(data[4:])

    def _auth_0(self, data=None):
        """AuthenticationOK: we are authenticated.
        """
        
        self.status = CONNECTION_AUTH_OK
        
    def _auth_3(self, data=None):
        """AuthenticationCleartextPassword: cleartext password is
        required.
        """
        
        log.msg("auth pass")

        if self.password is None:
            error = AuthenticationError("password is required")
            self._last.deferred.errback(error)

            self.transport.loseConnection()
            return
        
        self.passwordMessage(self.password)

    def _auth_5(self, salt):
        """AuthenticationMD5Password: an MD5-encrypted password is
        required.
        
        md5hex(md5hex(password + user) + salt)
        """
        
        log.msg("auth md5")

        if self.password is None:
            error = AuthenticationError("password is required")
            self._last.deferred.errback(error)

            self.transport.loseConnection()
            return

        hash = md5.new(self.password + self.user).hexdigest()
        password = "md5" + md5.new(hash  + salt).hexdigest()
        
        self.passwordMessage(password)

    def message_K(self, data):
        """BackendKeyData: the backend process id and secret key.
        """

        self.backendPID, self.cancelKey = unpack("!II", data)

    def message_S(self, data):
        """ParameterStatus: backed runtime parameter.
        """

        key, val, _ = data.split("\0")
        self.parameterStatus[key] = val
		
    def message_Z(self, transactionStatus):
        """ReadyForQuery: the backend is ready for a new query cycle.
        """
        
        self.transactionStatus = transactionStatus

        assert self._last
        deferred = self._last.deferred
        self._last = None
        
        if self.lastError:
            deferred.errback(PgError(self.lastError))
            self.lastError = {}
            return
        
        oldStatus = self.status
        self.status = CONNECTION_OK
        
        if oldStatus == CONNECTION_AUTH_OK:
            deferred.callback(self.parameterStatus)
        else:
            deferred.callback(self.lastResult)
        
        # send the next request
        self._flush()

    #
    # Simple Query
    #
    def message_C(self, tag):
        """CommandComplete: an SQL command completed normally.
        
        Note that a simple query can contain more than one command.
        """

        print "command complete", tag

    def message_T(self, data):
        """RowDescription: a description of row fields.
        """

        print "row description:", repr(data)

    def message_D(self, data):
        """DataRow: a row from the result.
        """

        print "data row:", repr(data)

    def message_I(self, data):
        """EmptyQueryResponse: an empty query string was recognized.
        """

        print "empty query"
        
    
    # 
    # frontend messages handling
    #
    def login(self, sslmode="prefer", **kwargs):
        """StartupMessage: login to the PostgreSQL database

        sslmode can be:
          disable
          allow
          prefer
          require
        
        The only required option is user.
        Optional parameters is database.

        In addition any run-time parameters that can be set at backend
        start time may be listed.

        XXX TODO: support default values, enviroment variables and
        password files.
        """

        self.user = kwargs.get("user", None)
        self.password = kwargs.get("password", None)
        
        if not self.user:
            raise RuntimeError("user is required")
        
        if self.password:
            del kwargs["password"]
        
        options = []
        for key, val in kwargs.iteritems():
            options.append(key)
            options.append(val)

        options = "\0".join(options) + "\0\0"

        size = len(options)
        payload = pack("!II%ds" % size, size + 8, PG_PROTO_VERSION,
                       options) 
        
        # the StartupMessage does not require the message type
        self.transport.write(payload)

        self._last = PgRequest(None, payload)
        self.status = CONNECTION_AWAITING_RESPONSE

        return self._last.deferred

    def passwordMessage(self, password):
        """PasswordMessage: send a password response.

        internal method.
        """

        self._sendMessage("p", password + "\0")

    def query(self, query):
        """Query: execute a simple query.
        """

        request = PgRequest("Q", query + "\0")
        return self.sendMessage(request)

    def terminate(self):
        """Terminate: issue a disconnection packet and disconnect.
        """
        
        request = PgRequest("X", "")
        self.sendMessage(request)
        
        self.transport.loseConnection()
	


class PgFactory(protocol.ClientFactory):
    """A simple factory that manages PgProtocol.
    """
    

    def buildProtocol(self,addr):
        protocol = PgProtocol()
        protocol.factory = self
        return protocol
    
    def clientConnectionMade(self, protocol):
        pass

    def clientConnectionFailed(self, connector, reason):
        pass

