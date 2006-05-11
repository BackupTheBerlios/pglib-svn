"""Test suite for login

$Id$

THIS SOFTWARE IS UNDER MIT LICENSE.
(C) 2006 Perillo Manlio (manlio.perillo@gmail.com)

Read LICENSE file for more informations.
"""


import sys
sys.path.append("../")

from twisted.python import log
from twisted.internet import reactor
from twisted.internet.protocol import ClientFactory

import protocol



# XXX TODO use one user for each authentication method supported
host = "localhost"
port = 5432
user = "pqlib"
password = "test"


def cbLogin(result, proto):
    print "OK"
    print result
    proto.terminate()
    
    reactor.stop()

def ebLogin(reason):
    print "ERROR"
    reason.raiseException()

    reactor.stop()



class Test(protocol.PgProtocol):
    def connectionMade(self):
        print "connection made"
        self.login(user=user, password=password
                   ).addCallback(cbLogin, self
                                 ).addErrback(ebLogin)


class TestFactory(ClientFactory):
    protocol = Test

    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason
        
    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed. Reason:', reason


factory = TestFactory()
factory.protocol = Test

log.startLogging(sys.stdout, False)
reactor.connectTCP(host, port, factory) 

reactor.run()


