"""PostgreSQL Protocol implementation

$Id$

THIS SOFTWARE IS UNDER MIT LICENSE.
(C) 2006 Perillo Manlio (manlio.perillo@gmail.com)

Read LICENSE file for more informations.
"""


from struct import pack, unpack
import md5

from zope.interface import implements
from twisted.python import log
from twisted.internet import protocol, defer

import ipg


# protocol version
PG_PROTO_VERSION = (3 << 16) | 0

# cancel request code
PG_CANCEL_CODE = (1234 << 16) | 5678

# messages header size
PG_HEADER_SIZE = 5 # 1 byte opcode + 4 byte lenght

# connection status (XXX not realy useful here)
CONNECTION_STARTED = 0           # waiting for connection to be made
CONNECTION_MADE = 1              # connection ok; waiting to send
CONNECTION_AWAITING_RESPONSE = 2 # waiting for a response from the
                                 # server # XXX what response?
CONNECTION_AUTH_OK = 3           # received authetication; waitig for backend
                                 # start-up finish
CONNECTION_SSL_STARTUP = 4       # negotiating SSL encryption
CONNECTION_SETENV = 5            # negotiating enviroment-driven
                                 # parameter # settings # XXX what is this?
CONNECTION_OK = 6                # connection ok; backend ready
CONNECTION_BAD = None            # connection procedure failed

# transaction status
PGTRANS_IDLE = "I"     # currently idle
PGTRANS_INTRANS = "T"  # idle, in a valid transaction block
PGTRANS_INERROR = "E"  # idle, in a failed transaction block
PGTRANS_ACTIVE = "A"   # a command is in progress (used only in the frontend)
PGTRANS_UNKNOWN = None # the connection is bad # XXX

# error field codes
PG_DIAG_SEVERITY = "S"
PG_DIAG_SQLSTATE = "C"
PG_DIAG_MESSAGE_PRIMARY = "M"
PG_DIAG_MESSAGE_DETAIL = "D"
PG_DIAG_MESSAGE_HINT = "H"
PG_DIAG_STATEMENT_POSITION = "P"
PG_DIAG_INTERNAL_POSITION = "p"
PG_DIAG_INTERNAL_QUERY = "q"
PG_DIAG_CONTEXT = "W"
PG_DIAG_SOURCE_FILE = "F"
PG_DIAG_SOURCE_LINE = "L"
PG_DIAG_SOURCE_FUNCTION = "R"

# result status
PGRES_EMPTY_QUERY = 0      # the string sent to the server was empty
PGRES_COMMAND_OK = 1       # successful completion of a command
                           # returning no data
PGRES_TUPLES_OK = 2        # successful completion of a command
                           # returning data (such as SELECT or SHOW)
PGRES_COPY_OUT = 3         # Copy Out (from server) data transfer started
PGRES_COPY_IN = 4          # Copy In (to server) data transfer started
# XXX there are not realy useful
PGRES_BAD_RESPONSE = -1    # the server response wa not understood
PGRES_NONFATAL_ERROR = -2  # a non fatal error (a notice or warning)
                           # occurred
PGRES_FATAL_ERROR = -3     # a fatal error occurred


# exception class hierarchy
class Error(Exception):
    pass

class PgError(Error):
    """A wrapper for a dictionary with PostgreSQL error data.
    
    XXX TODO
    """
    
    def __init__(self, args):
        self.args = args

    def errorMessage(self):
        """Return the error message associated with this instance.
        """

        return self.args["M"]

    def errorField(self, field):
        """Returns an individual field of an error report, or None if
        the specified fiels is not included.
        """

        return self.args.get(field, None)
    
    def __str__(self):
        return str(self.args)

class InvalidRequest(Error):
    pass

class AuthenticationError(Error):
    pass

class UnsupportedError(Error):
    pass


class PgCancel(object):
    """A proxy object for sending cancel requests to a PostgreSQL
    backend.
    """
    
    def __init__(self, addr, backendPID, cancelKey):
        self.addr = addr
        self.backendPID = backendPID
        self.cancelKey = cancelKey

    def cancel(self, timeout=30):
        """Send a cancel request to the backend.
        """
        
        from twisted.internet import reactor
        
        
        factory = CancelFactory(self.backendPID, self.cancelKey)
        reactor.connectTCP(self.addr.host, self.addr.port, factory, timeout)
        
        # returns only when the request has been sent
        return factory.deferred


class Notification(object):
    """A wrapper for notify data.
    """
    
    def __init__(self, pid, name, extra=None):
        self.pid = pid
        self.name = name
        self.extra = extra

    def __str__(self):
        return "'%s' notification received " \
            "from backend pid %d" % (self.name, self.pid)

class PgRequest(object):
    """A wrapper for a request to the backend.
    """
    
    def __init__(self, opcode, payload):
        self.opcode = opcode
        self.payload = payload

        self.deferred = defer.Deferred()


class PgResult(object):
    """XXX TODO
    """


class PgProtocol(protocol.Protocol):
    """The PostgreSQL protocol implementation, version 3.0.

    PostgreSQL support multiple request, but we choose to send only
    one request at time, since life is much easier.

    We also choose to not support multiple result set (for simple
    query with multiple commands), but you can retrieve all the result
    by suppling your own IRowConsumer implementation.

    Whenever possible, we try to follow the interface of libpq.
    """

    implements(ipg.IFastPath)

    
    _buffer = ""

    status = CONNECTION_STARTED
    transationStatus = PGTRANS_IDLE
    parameterStatus = {} 
    
    lastResult = None
    lastNotice = {}
    lastError = {}
    lastNotify = None
    
    protocolVersion = 3 # we support only this
    serverVersion = None
    backendPID = None

    
    def __init__(self, addr, handler=None, rowConsumer=None):
        """Address is a IAddr address, handler is a IHandler object,
        rowConsumer is a IRowConsumer object.
        """
        
        self.addr = addr # used by cancel
        self.handler = handler or DefaultHandler()
        self.rowConsumer = rowConsumer or DefaultRowConsumer()
        
        # cancellation key used for cancel a query in progress
        self.cancelKey = None
        
        self._queue = [] # we queue requests to the backend
        self._last = None # last request we made

    def connectionMade(self):
        self.status = CONNECTION_MADE
        self.transactionStatus = PGTRANS_UNKNOWN
        
        self.factory.clientConnectionMade(self)

    def connectionLost(self, reason=protocol.connectionDone):
        #log.msg("connection lost", reason)
        
        # XXX there is really no need for these...
        self.status = CONNECTION_BAD
        self.transactionStatus = PGTRANS_UNKNOWN
    
    def dataReceived(self, data):
        """Handle raw data arrived from postgres backend.

        XXX TODO use cStringIO as in pgasync, but do some benchmarks.
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

        self.handler.notice(notice)
        
        # XXX we store only the last notice
        self.lastNotice = notice
        
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

        # these are no more needed
        del self._user
        del self._password
        
    def _auth_3(self, data=None):
        """AuthenticationCleartextPassword: cleartext password is
        required.
        """
        
        log.msg("auth pass")

        if self._password is None:
            error = AuthenticationError("password is required")
            self._last.deferred.errback(error)

            self.transport.loseConnection()
            return
        
        self.passwordMessage(self._password)

    def _auth_5(self, salt):
        """AuthenticationMD5Password: an MD5-encrypted password is
        required.
        
        md5hex(md5hex(password + user) + salt)
        """
        
        log.msg("auth md5")

        if self._password is None:
            error = AuthenticationError("password is required")
            self._last.deferred.errback(error)

            self.transport.loseConnection()
            return

        hash = md5.new(self._password + self._user).hexdigest()
        password = "md5" + md5.new(hash  + salt).hexdigest()
        
        self.passwordMessage(password)

    def message_K(self, data):
        """BackendKeyData: the backend process id and secret key.
        """

        self.backendPID, self.cancelKey = unpack("!II", data)

    def message_S(self, data):
        """ParameterStatus: backend runtime parameter.
        """

        key, val, _ = data.split("\0")
        self.parameterStatus[key] = val
		
    def message_Z(self, transactionStatus):
        """ReadyForQuery: the backend is ready for a new query cycle.
        """
        
        self.transactionStatus = transactionStatus

        assert self._last
        deferred = self._last.deferred
        opcode = self._last.opcode
        self._last = None
        
        if self.lastError:
            deferred.errback(PgError(self.lastError))
            self.lastError = {}
            return
        
        self.status = CONNECTION_OK
        
        if opcode is None: 
            # compute the server version, as required by the libpq
            # interface
            version = self.parameterStatus["server_version"]
            major, minor, rev = map(int, version.split("."))
            self.serverVersion = rev + minor * 100 + major * 10000
            
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

        self.lastResult = self.rowConsumer.complete(tag)

    def message_T(self, data):
        """RowDescription: a description of row fields.
        """

        self.rowConsumer.description(data)

    def message_D(self, data):
        """DataRow: a row from the result.
        """

        self.rowConsumer.row(data)

    def message_I(self, data):
        """EmptyQueryResponse: an empty query string was recognized.
        """

        self.lastResult = "empty query"
        
    
    #
    # Function Call (aka Fast Path Interface)
    #
    # Note: this seems to be obsolete, but it is still used (and is much
    # simpler) for large objects support by libpq
    def message_V(self, data):
        """FunctionCallResponse: the result from a function call.
        """

        (length,) = unpack("!I", data[:4])
        
        if length == -1:
            # NULL returned
            self.lastResult = None
        else:
            self.lastResult = data[4:]
     
    
    #
    # COPY Operations
    #
    
    #
    # Asynchronous Operations
    #
    def message_A(self, data):
        """NotificationResponse: an asyncronous notification.
        """

        (pid,) = unpack("!I", data[:4])
        name, extra, _ = data[4:].split("\0")

        notify = Notification(pid, name, extra)
        self.handler.notify(notify)
        
        # XXX store only the last notification
        self.lastNotify = notify
    
    
    # 
    # frontend messages handling
    #
    def login(self, **kwargs):
        """StartupMessage: login to the PostgreSQL database
        
        The only required option is user.
        Optional parameters is database.

        In addition any run-time parameters that can be set at backend
        start time may be listed.
        """

        parameters = kwargs.copy()
        
        self._user = parameters.get("user", None)
        self._password = parameters.get("password", None)
        
        if self._user is None:
            return defer.fail(AuthenticationError("user is required"))
        
        if self._password:
            # the password is not required now
            del parameters["password"]
        
        options = []
        for key, val in parameters.iteritems():
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

    def execute(self, query, *args, **kwargs):
        """Query: execute a simple query.
        
        XXX TODO string interpolation and escaping.
        """

        request = PgRequest("Q", query + "\0")
        return self.sendMessage(request)

    def fn(self, fnid, fformat, *args):
        """FunctionCall: execute a function.

        fformat can be 0 (text) or 1(binary)
        
        arguments must be strings
        
        XXX TODO: since I'm lazy, in the current implementation all
        arguments (and the return value) must be of the same format.
        """
        
        prefix = pack("!IHHH", fnid, 1, fformat, len(args))

        data = []
        for a in args:
            length = len(a)
            buf = pack("!I%ds" % length, length, a)
            data.append(buf)
        
        payload = prefix + ''.join(data) + pack("!H", fformat)
        request = PgRequest("F", payload)

        return self.sendMessage(request)
    
    def getCancel(self):
        """Request a cancellation object for this connection.
        """

        return PgCancel(self.addr, self.backendPID, self.cancelKey)

    def cancel(self, backendPID, cancelKey):
        """CancelRequest: request a cancellation for the current
        query.

        internal method
        """

        payload = pack("!IIII", 16, PG_CANCEL_CODE, backendPID, cancelKey)
        
        # the CancelRequest does not require the message type
        self.transport.write(payload)

    def finish(self):
        """Terminate: issue a disconnection packet and disconnect.
        """
        
        request = PgRequest("X", "")
        self.sendMessage(request)
        
        self.transport.loseConnection()
	
    def errorMessage(self):
        """Return the error message associated with th3 last command,
        or an empty string if there was no error.
        """

        if self.lastError:
            return self.lastError["M"]
        else:
            return ""


class PgFactory(protocol.ClientFactory):
    """A simple factory that manages PgProtocol.
    """
    
    def __init__(self, sslmode="prefer"):
        """sslmode can be:
          disable
          allow
          prefer
          require
        """
        
        # we store sslmode here because it is used by cancel too.
        self.sslmode = sslmode

    def buildProtocol(self, addr):
        protocol = PgProtocol(addr)
        protocol.factory = self
        return protocol
    
    def clientConnectionMade(self, protocol):
        pass


class CancelFactory(PgFactory):
    """A custom factory for cancel requests.
    """
            
    def __init__(self, backendPID, cancelKey, sslmode="prefer"):
        self.backendPID = backendPID
        self.cancelKey = cancelKey
        self.sslmode = sslmode
        
        self.deferred = defer.Deferred()
                
    def clientConnectionMade(self, protocol):
        protocol.cancel(self.backendPID, self.cancelKey)
        protocol.finish() # XXX (the specification says nothing)

    def clientConnectionLost(self, connector, reason):
        self.deferred.callback(None)
        
    def clientConnectionFailed(self, connector, reason):
        self.deferred.errback(reason)


#
# Default implementations for ipg interfaces
#

class DefaultHandler(object):
    implements(ipg.IHandler)

    def notice(self, notice):
        log.msg("Notice:", str(notice))

    def notify(self, notify):
        log.msg("Notification:", str(notify))

        
class DefaultRowConsumer(object):
    implements(ipg.IRowConsumer)
    
    def __init__(self):
        pass

    def description(self, data):
        print "description", repr(data)

    def row(self, data):
        print "row", repr(data)

    def complete(self, data):
        print "command complete", data

        return None
