"""PostgreSQL Protocol implementation

$Id$

THIS SOFTWARE IS UNDER MIT LICENSE.
Copyright (c) 2006 Perillo Manlio (manlio.perillo@gmail.com)

Read LICENSE file for more informations.

Credits: some code is adapted from pgasync
         Copyright (c) 2005 Jamie Turner.  All rights reserved.
"""


from struct import pack, unpack
import md5

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from zope.interface import implements

from twisted.python import log
from twisted.internet import reactor, protocol, defer, interfaces
from twisted.internet.address import IPv4Address, UNIXAddress 
import ipg


# protocol version
PG_PROTO_VERSION = (3 << 16) | 0

# cancel request code
PG_CANCEL_CODE = (1234 << 16) | 5678

# SSL request code
SSL_REQUEST_CODE = (1234 << 16) | 5679

# messages header size
PG_HEADER_SIZE = 5 # 1 byte opcode + 4 byte lenght

InvalidOid = 0 # XXX

# connection status (XXX not really useful)
CONNECTION_STARTED = 0           # waiting for connection to be made
CONNECTION_MADE = 1              # connection ok; waiting to send
CONNECTION_AWAITING_RESPONSE = 2 # waiting for a response from the
                                 # server # XXX what response?
CONNECTION_AUTH_OK = 3           # received authentication; waiting for backend
                                 # start-up finish
CONNECTION_SSL_STARTUP = 4       # negotiating SSL encryption
CONNECTION_SETENV = 5            # negotiating enviroment-driven
                                 # parameter settings # XXX what is this?
CONNECTION_OK = 6                # connection ok; backend ready
CONNECTION_BAD = -1              # connection procedure failed

# transaction status
PGTRANS_IDLE = "I"     # currently idle
PGTRANS_INTRANS = "T"  # idle, in a valid transaction block
PGTRANS_INERROR = "E"  # idle, in a failed transaction block
PGTRANS_ACTIVE = "A"   # a command is in progress (used only in the frontend)
PGTRANS_UNKNOWN = -1   # the connection is bad # XXX

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
# XXX these are not really useful
PGRES_BAD_RESPONSE = -1    # the server response was not understood
PGRES_NONFATAL_ERROR = -2  # a non fatal error (a notice or warning)
                           # occurred
PGRES_FATAL_ERROR = -3     # a fatal error occurred


# exception class hierarchy
class Error(Exception):
    pass

class PgError(Error):
    """A wrapper for a dictionary with PostgreSQL error data.
    
    XXX TODO derive from a dict?
    """
    
    def __init__(self, args):
        self.args = args

    def errorMessage(self):
        """Return the error message associated with this instance.
        """

        return self.args["M"]

    def errorField(self, field):
        """Return an individual field of an error report, or None if
        the specified field is not included.
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

        Return a deferred.
        """
        
        from twisted.internet import reactor
        
        
        factory = CancelFactory(self.backendPID, self.cancelKey)

        if isinstance(self.addr, IPv4Address):
            reactor.connectTCP(self.addr.host, self.addr.port,
                               factory, timeout)
        elif isinstance(self.addr, UNIXAddress):
            reactor.connectUNIX(self.addr.name, factory, timeout)
        else:
            return defer.fail(RuntimeError("invalid address"))
        
        # returns only when the request has been received by the backend
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



class PgProtocol(protocol.Protocol):
    """The PostgreSQL protocol implementation, frontend side, 
    version 3.0.

    PostgreSQL support multiple request, but we choose to send only
    one request at time, since life is much easier.

    We also choose to not support multiple result set (for simple
    query with multiple commands), but you can retrieve all the result
    by suppling your own IRowConsumer implementation.

    Whenever possible, we try to follow the interface of libpq.
    
    To simplify the interface, all columns or routines pararameters
    must be of the same format: text or binary.
    """

    implements(ipg.IFastPath)

    debug = True
    
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
        self.handler = handler or Handler()
        self.rowConsumer = rowConsumer or RowConsumer()
        
        # cancellation key used for cancel a query in progress
        self.cancelKey = None
        
        self._queue = [] # we queue requests to the backend
        self._last = None # last request we made

        self._buffer = ""

    def _getContextFactory(self):
        context = getattr(self.factory, "sslContext", None)
        if context is not None:
            return self.context
        
        try:
            from twisted.internet import ssl
        except ImportError:
            return None
        else:
            context = ssl.ClientContextFactory()
            context.method = ssl.SSL.TLSv1_METHOD
            return context

    def connectionMade(self):
        self.status = CONNECTION_MADE
        self.transactionStatus = PGTRANS_UNKNOWN # XXX
        
        # Unfortunately the response to a SSL request is not a
        # standard message: there is no lenght.
        # So we use two version of dataReceived method

        # XXX "allow" and "prefer" are not fully supported.
        # In fact if the server explicitly require or disable SSL
        # (with hostssl and hostnossl in pg_hba.conf) there can be the
        # need of a reconnection, and this is possible only in an
        # higher level interface (see fe.py).
        #
        # However "prefer" is still useful, since it allows to connect
        # to both SSL enabled and disabled servers.
        if self.factory.sslmode in ["disable", "allow"]:
            # no SSL negotiation
            self.dataReceived = self._dataReceived
            self.factory.clientConnectionMade(self)
        else: # prefer, require
            # prepare to negotiate SSL
            self.dataReceived = self._dataReceivedSSL
            
            # the SSLRequest has no type code
            self.transport.write(pack("!II", 8, SSL_REQUEST_CODE))
            self.status = CONNECTION_SSL_STARTUP

            log.msg("started SSL request")
            
            # we call clientConnectionMade only after negotiation
            # has been completed
        
    def connectionLost(self, reason=protocol.connectionDone):
        #log.msg("connection lost", reason)
        
        # XXX there is really no need for these...
        self.status = CONNECTION_BAD
        self.transactionStatus = PGTRANS_UNKNOWN
    
    def _dataReceived(self, data):
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

    def _dataReceivedSSL(self, data):
        """Handle backend response to our request to use SSL.
        """

        log.msg("SSL Response:", data)
        
        if data == "S":
            # check the support for SSL
            tls = interfaces.ITLSTransport(self.transport, None)
            if tls is None:
                err = RuntimeError(
                    "PgProtocol transport does not implements " \
                    "ITLSTRansport"
                    )
                log.err(err)
                self.transport.loseConnection()
                
                return

            context = self._getContextFactory()
            if context is None:
                err = RuntimeError(
                    "PgProtocol requires a TLS context to "
                    "initiate the SSL handshake"
                    )
                log.err(err)
                self.transport.loseConnection()
                
                return

            # ok, we can initialize TSL handshake
            tls.startTLS(context)
            self.dataReceived = self._dataReceived

            # handshake complete(?), now we can notify the factory
            log.msg("SSL handshake complete")
            self.factory.clientConnectionMade(self)
            
            return
        elif data == "N":
            if self.factory.sslmode == "require":
                err = RuntimeError(
                    "PostgreSQL backend does not support SSL, " \
                    "connection aborted"
                    )
                log.err(err)
                self.transport.loseConnection()
                    
                return
            
            self.dataReceived = self._dataReceived
            
            # ok, no SSL available; now we can notify the factory
            log.msg("no SSL available")
            self.factory.clientConnectionMade(self)
            
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

        if self.debug:
            log.msg("request sent:", opcode)
            
    def messageReceived(self, opcode, payload):
        """Handle the message.
        """

        if self.debug:
            log.msg("message received:", opcode)
        
        # dispatch the message using python introspection
        method = getattr(self, "message_" + opcode, None)
        
        if method is None:
            error = InvalidRequest(opcode)
            
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

        # parse the tag
        tags = tag[:-1].split(" ") # remove the ending \0
        n = len(tags)
        
        cmdStatus = tags[0] 
        if n == 3:
            oid  = int(tags[1])
            rows = int(tags[2]) # XXX libpq uses a string
        elif n == 2:
            oid = 0
            rows = int(tags[1])
        else:
            oid = 0
            rows = 0
        
        if cmdStatus == "COPY" and \
                self.lastResult.status in (PGRES_COPY_OUT, PGRES_COPY_IN):
            # XXX we already have a result
            assert rows == 0
            assert oid == 0
            
            self.lastResult.cmdStatus = cmdStatus
            self.lastResult.cmdTuples = rows
            self.lastResult.oidValue = oid
        else:
            self.lastResult = self.rowConsumer.complete(cmdStatus,
                                                        oid, rows)
    def message_T(self, data):
        """RowDescription: a description of row fields.
        """

        # XXX should we parse data here?
        self.rowConsumer.description(data)

    def message_D(self, data):
        """DataRow: a row from the result.
        """

        self.rowConsumer.row(data)

    def message_I(self, data):
        """EmptyQueryResponse: an empty query string was recognized.
        """

        self.lastResult = Result()
        
    
    #
    # Function Call (aka Fast-Path Interface)
    #
    # Note: this seems to be obsolete, but it is still used (and it is
    # much simpler) for large objects support by libpq
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
    def _copyData(self):
        # helper method
        
        try:
            data = self.producer.read()
            if not data:
                # COPY terminated
                self.copyDone()
                
                self.lastResult = self.producer.close()
            else:
                self.copyData(data)
                
                # continue the transfer
                reactor.callLater(0, self._copyData)
        except Exception, error:
            log.err(error)
            self.copyFail(str(error))
    
    def message_G(self, data):
        """CopyInResponse: the frontend must now send copy data.
        """

        binaryTuples, ntuples = unpack("!BH", data[:3])
        formats = unpack("!" + "H" * ntuples, data[3:])

        try:
            # XXX TODO we ignore formats, all columns have the same
            # format code
            self.producer.description(ntuples, binaryTuples)
        except Exception, error:
            log.err(error)
            self.copyFail(str(error))

        # send all data from producer to the backend
        reactor.callLater(0, self._copyData)
        
    def message_H(self, data):
        """CopyOutResponse: the frontend must now receive copy data.
        """

        binaryTuples, ntuples = unpack("!BH", data[:3])
        formats = unpack("!" + "H" * ntuples, data[3:])

        # XXX TODO we ignore formats, all columns have the same
        # format code
        # we ignore errors, since the frontend cannot abort data
        # transfer
        self.consumer.description(ntuples, binaryTuples)
    
    def message_d(self, data):
        """CopyData: data for COPY.

        The backend sends always one message per row.
        """

        # we ignore errors, since the frontend cannot abort data transfer
        self.consumer.write(data)
    
    def message_c(self, data):
        """CopyDone: COPY transfer complete.
        """

        assert not data

        self.lastResult = self.consumer.close()

    
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
        
        # XXX we store only the last notification
        self.lastNotify = notify
    
    
    # 
    # frontend messages handling
    #
    def login(self, **kwargs):
        """StartupMessage: login to the PostgreSQL database
        
        The only required option is user.
        Optional parameters is database; defaults to user name.

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
        
        XXX TODO string interpolation.
        """

        request = PgRequest("Q", query + "\0")
        return self.sendMessage(request)

    def fn(self, fnid, fformat, *args):
        """FunctionCall: execute a function.

        fformat can be 0 (text) or 1(binary)
        
        arguments must be strings
        
        XXX TODO: In the current implementation all
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
    
    def copyData(self, data):
        """Copydata: send data to backend, for COPY operations.

        data needs not to be a row.

        internal method
        """

        self._sendMessage("d", data)
    
    def copyFail(self, error):
        """CopyFail: COPY transfer failed.
        
        error is an error messages 
        (used by the backend in its ErrorResponse)
        
        internal method
        """

        self._sendMessage("f", error + "\0")
        
    def copyDone(self):
        """CopyDone: COPY transfer complete.
        
        internal method
        """

        self._sendMessage("c", "")
        
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
	

    #
    # Helper methods
    #
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
    
    sslContext = None
    
    def __init__(self, sslmode="prefer"):
        """sslmode determine whether or with what priority an SSL
        connection will be negotiated.
        
        There are four modes: "disable" will attempt only an
        unencrypted SSL connection; "allow" will negotiate, trying
        first a non-SSL connection, then if that fails, trying an
        SSL connection; "prefer" (the default) will negotiate,
        trying first an SSL connection, then if that fails, trying a
        regular non-SSL conection; "require" will try only an SSL connection.
        
        TODO: in this layer, "allow" and "prefer" cannot be fully supported.
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
# Default implementations for required ipg interfaces
#

class Handler(object):
    implements(ipg.IHandler)

    def notice(self, notice):
        log.msg("Notice:", str(notice))

    def notify(self, notify):
        log.msg("Notification:", str(notify))


class RowDescription(object):
    implements(ipg.IRowDescription)
    
    def __init__(self, fname, ftable, ftablecol, ftype, fsize, fmod,
                 fformat):
        self.fname = fname
        self.ftable = ftable
        self.ftablecol = ftablecol
        self.ftype = ftype
        self.fsize = fsize
        self.fmod = fmod
        self.fformat = fformat

class Result(object):
    implements(ipg.IResult)

    ntuples = None
    nfields = None
    binaryTuples = None
    
    status = PGRES_EMPTY_QUERY # convenient default
    
    cmdStatus = None
    cmdTuples = None
    oidvalue = None
    
    def __init__(self):
        self.descriptions = []
        self.rows = []
        
class RowConsumer(object):
    # XXX TODO write an optimized implementation in C, like pgasync.
    
    implements(ipg.IRowConsumer)
    
    def __init__(self):
        self.result = Result()

    def description(self, data):
        # parse the data
        buf = StringIO(data)

        (nfields,) = unpack("!H", buf.read(2))

        pos = 2
        for i in range(nfields):
            idx = data.find("\0", pos)
            fname = buf.read(idx - pos)
            pos = idx + 19 # leading null plus 18 of int
            
            (
                _, ftable, ftablecol, ftype, fsize, fmod, fformat
                ) = unpack("!cIHIHIH", buf.read(19))
            
            desc = RowDescription(fname, ftable, ftablecol, ftype,
                                  fsize, fmod, fformat)
            self.result.descriptions.append(desc)

    def row(self, data):
        # parse the data
        buf = StringIO(data)
        
        (ntuples,) = unpack("!H", buf.read(2))

        row = []
        for i in range(ntuples):
            (length,) = unpack("!I", buf.read(4))
            if length == -1:
                # a NULL value
                row.append(None)
            else:
                row.append(buf.read(length))
        
        self.result.rows.append(row)
    
    def complete(self, status, oid, rows):
        self.result.cmdStatus = status
        self.result.cmdTuples = rows
        self.result.oidValue = oid
        
        self.result.nfields = len(self.result.descriptions)
        self.result.ntuples = len(self.result.rows)
        self.result.binaryTuples = 0 # XXX TODO
        
        if self.result.ntuples > 0:
            # XXX what should return a SELECT with no rows?
            self.result.status = PGRES_TUPLES_OK
        else:
            self.result.status = PGRES_COMMAND_OK
        
        # prepare the next cycle XXX
        tmp = self.result
        self.result = Result()
        
        return tmp
