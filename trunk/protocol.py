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
                                 # server
CONNECTION_AUTH_OK = 3 # received authetication; waitig for backend
                       # start-up finish
CONNECTION_SSL_STARTUP = 4 # negotiating SS encryption
CONNECTION_SETENV = 5 # negotiating enviroment-driven parameter
                      # settings # XXX what is this?
CONNECTION_OK = 6 # connection ok; backend ready
CONNECTION_BAD = -1 # connection procedure failed

# transaction status
PGTRANS_IDLE = "I" # currently idle
PGTRANS_INTRANS = "T" # idle, in a valid transaction block
PGTRANS_INERROR = "E" # idle, in a failed transaction block
PGTRANS_ACTIVE = 1 # a command is in progress
PGTRANS_UNKNOWN = 0 # the connection is bad

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

class PgError(Exception):
    """XXX TODO
    """
    
    def __init__(self, args):
        self.args = args

    def __str__(self):
        return str(self.args)


class PgResult(object):
    """XXX TODO
    """

class PgRequest(object):
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
    by suppling yur RowConsumer

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
        """Override this to be notified when the connection is done.
        """
        
        self.status = CONNECTION_MADE

    def connectionLost(self, reason=protocol.connectionDone):
        print "connection lost", reason
        
        self.transactionStatus = PGTRANS_UNKNOWN
    
    def dataReceived(self, data):
        """Handle raw data arrived from postgres backend.

        XXX TODO use cStringIO as in pgasync
        """

        self._buffer = self._buffer + data
                                          
        while len(self._buffer) > PG_HEADER_SIZE:
            # read the message header
            opcode, size = unpack("!cI",
                                  self._buffer[:PG_HEADER_SIZE])

            size = size - 4 # the lenght does not include the message type
            if len(self._buffer) < size + PG_HEADER_SIZE:
                break
            
            payload = self._buffer[PG_HEADER_SIZE : size + PG_HEADER_SIZE]
            self._buffer = self._buffer[size + PG_HEADER_SIZE:]

            
            self.messageReceived(opcode, payload)


    def sendMessage(self, request):
        """Send the given message to the backend.
        """
        
        if not self._queue:
            # send the message now
            self._last = request
            self._sendMessage(request.opcode, request.payload)
        else:
            self._queue.append(request)

        return request.deferred

    def _sendMessage(self, opcode, payload):
        # internal helper

        header = pack("!cI", opcode, len(payload) + 4)
        self.transport.write(header + payload)

    def messageReceived(self, opcode, payload):
        """Handle the message.
        """

        print "message received:", opcode
        
        # dispatch the message using python introspection
        method = getattr(self, "message_" + opcode, None)
        
        if method is None:
            log.err("Invalid message: " + opcode)
            self.transport.loseConnection() # XXX
            return
 
        method(payload)

     
    #
    # backend messages handling
    #
    # Basic
    #
    def message_E(self, data):
        """ErrorResponse: an error occurred.

        For error message types see protocol documentation.
        """
        
        for item in data.split("\0")[:-2]:
            key, val = item[:1], item[1:]
            self.lastError[key] = val

        # check if we failed the authentication
        if self._last.opcode is None:
            self.status = CONNECTION_BAD
            self._last.deferred.errback(PgError(self.lastError)) # XXX
            self.transport.loseConnection()
        
    def message_N(self, data):
        """NoticeResponse: a notice from the backend.

        For warning message types see protocol documentation.
        """

        for item in data.split("\0")[:-2]:
            key, val = item[:1], item[1:]
            self.lastNotice[key] = val

        print "Notice"
        print self.lastNotice
        
    def message_R(self, data):
        """Authentication: authentication request.
        """

        (authtype,) = unpack("!I", data[:4])

        method = getattr(self, "_auth_%s" % authtype, None)
        if not method:
            err = RuntimeError("Authentication not supported '%d'" % authtype) # XXX 
            
            assert self._last.payload is None
            self._last.deferred.errback(err)
        
            self.transport.loseConnection()

        method(data[4:])

    def _auth_0(self, data=None):
        """AuthenticationOK: we are authenticated.
        """
        
        self.status = CONNECTION_AUTH_OK
        
    def _auth_3(self, data=None):
        """AuthenticationCleartextPassword: cleartext password is
        required.
        """
        
        print "auth pass"

        if self.password is None:
            raise RuntimeError("password is required")

        self.passwordMessage(self.password)

    def _auth_5(self, salt):
        """AuthenticationMD5Password: an MD5-encrypted password is
        required.
        
        md5hex(md5hex(password + user) + salt)
        """
        
        print "auth md5"

        if self.password is None:
            raise RuntimeError("password is required")

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
                
        if self.lastError:
            self._last.deferred.errback(PgError(self.lastError))
            self.lastError = {}
            return
        
        if self.status == CONNECTION_AUTH_OK:
            self._last.deferred.callback(self.parameterStatus)
        else:
            self._last.deferred.callback(self.lastResult)
            self.lastResult = None
        
        self.status = CONNECTION_OK
    
    #
    # Query
    #
    def message_C(self, tag):
        """CommandComplete: an SQL command completed normally.
        
        Note that a query can contain more than one command.
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
    def login(self, **kwargs):
        """StartupMessage: login to the PostgreSQL database

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

        internal function.
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
	


class PQFactory(protocol.ClientFactory):
    """A simple factory that manages PgProtocol.
    """
    

    def buildProtocol(self,addr):
        protocol = PgProtocol()
        protocol.factory = self
        return protocol
        
    def clientConnectionFailed(self, connector, reason):
        print reason

