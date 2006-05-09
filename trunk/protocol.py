"""PostgreSQL Protocol implementation

$Id$

THIS SOFTWARE IS UNDER MIT LICENSE.
(C) 2006 Perillo Manlio (manlio.perillo@gmail.com)

Read LICENSE file for more informations.
"""


from struct import pack, unpack
import md5

from twisted.python import log
from twisted.internet import protocol


PG_VERSION_MAJOR = 3
PG_VERSION_MINOR = 0

PG_PROTO_VERSION = (PG_VERSION_MAJOR << 16) | PG_VERSION_MINOR 

PG_TRANS_IDLE = "I"
PG_TRANS_IN = "T"
PG_TRANS_ERROR = "E"

PG_HEADER_SIZE = 5 # 1 byte opcode + 4 byte lenght



class PgProtocol(protocol.Protocol):
    """The PostgreSQL protocol implementation, version 3.0.
    """

    _buffer = ""

    def __init__(self):
        # XXX better names
        
        self.transStatus = None # transaction state
        self.params = {} # # backend runtime parameters 
        
        self.pid = None
        self.cancelKey = None
        
        # Currently supported authentication methods.
        self.authMethods = {
            0 : self.auth_ok,        # AuthenticationOK
            3 : self.auth_pass,      # AuthenticationCleartextPassword
            5 : self.auth_md5        # AuthenticationMD5Password
            }

    def connectionMade(self):
        """Override this to be notified when the connection is done.
        """
        pass

    def dataReceived(self, data):
        """Handle raw data arrived from postgres backend.

        XXX TODO use cStringIO as in pgasync
        """

        self._buffer = self._buffer + data
                                          
        while len(self._buffer) > PG_HEADER_SIZE:
            # read the message header
            opcode, size = unpack("!BI",
                                  self._buffer[:PG_HEADER_SIZE])

            size = size - 4 # the lenght does not include the message type
            if len(self._buffer) < size + PG_HEADER_SIZE:
                break
            
            payload = self._buffer[PG_HEADER_SIZE : size + PG_HEADER_SIZE]
            self._buffer = self._buffer[size + PG_HEADER_SIZE:]

            
            self.messageReceived(opcode, payload)


    def sendMessage(self, opcode, payload):
        """Send the given message to the backend.
        """

        header = pack("!cI", opcode, len(payload) + 4)
        self.transport.write(header + payload)

    
    def messageReceived(self, opcode, payload):
        """Handle the message.
        """

        print "message received:", chr(opcode)
        
        # dispatch the message using python introspection

        method = getattr(self, "message_" + chr(opcode), None)
        
        if method is None:
            log.err("Invalid message: %c" % opcode)
            self.transport.loseConnection() # XXX
            return
 
        try:
            method(payload)
        except:
            log.err()
            self.transport.loseConnection() # XXX

     
    #
    # backend messages handling
    #
    def message_E(self, data):
        """ErrorResponse: an error occurred, connection is closed.

        For error message types see protocol documentation.
        """
        
        params = {}
        for item in data.split("\0")[:-2]:
            key, val = item[:1], item[1:]
            params[key] = val

        print "Error"
        print params

    def message_N(self, data):
        """NoticeResponse: a notice from the backend.

        For warning message types see protocol documentation.
        """

        params = {}
        for item in data.split("\0")[:-2]:
            key, val = item[:1], item[1:]
            params[key] = val

        print "Warning"
        print params
        
    def message_R(self, data):
        """Authentication: authentication request.
        """

        (authtype,) = unpack("!I", data[:4])

        try:
            method = self.authMethods[authtype]
        except:
            raise RuntimeError("Authentication not supported '%d'" % authtype)
    

        method(data[4:])

    def auth_ok(self, data=None):
        """AuthenticationOK: we are authenticated.
        """
        
        print "auth ok"

    def auth_pass(self, data=None):
        """AuthenticationCleartextPassword: cleartext password is
        required.
        """
        
        print "auth pass"

        if self.password is None:
            raise RuntimeError("password is required")

        self.passwordMessage(self.password)

    def auth_md5(self, salt):
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

        self.pid, self.cancelKey = unpack("!II", data)

        print "pid, cancel key:", self.pid, self.cancelKey

    def message_S(self, data):
        """ParameterStatus: backed runtime parameter.
        """

        key, val, _ = data.split("\0")
        self.params[key] = val
		
        print "parameter", key, val

    def message_Z(self, transStatus):
        """ReadyForQuery: the backend is ready for a new query cycle.
        """

        self.transStatus = transStatus
        
        print "transaction status:", transStatus
    
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

        
    def passwordMessage(self, password):
        """PasswordMessage: send a password response.

        internal function.
        """

        self.sendMessage("p", password + "\0")

    def query(self, query):
        """Query: execute a simple query.
        """

        self.sendMessage("Q", query + "\0")

    def terminate(self):
        """Terminate: issue a disconnection packet and disconnect.
        """
        
        self.sendMessage("X", "")
        self.transport.loseConnection()
	


class PgFactory(protocol.ClientFactory):
    """A simple factory that manages PgProtocol.
    """
    

    def buildProtocol(self,addr):
        protocol = PgProtocol()
        protocol.factory = self
        return protocol
        
    def clientConnectionFailed(self, connector, reason):
        print reason

