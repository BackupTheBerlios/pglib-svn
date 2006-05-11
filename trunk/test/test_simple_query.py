"""Test suite for simple query

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



host = "localhost"
port = 5432
user = "pqlib"
password = "test"


def cbLogin(result, proto):
    print "OK"
    print result
        
    proto.query("BEGIN; SELECT x"
                ).addCallback(cbQuery, proto
                              ).addErrback(ebQuery, proto)

def ebLogin(reason):
    print "ERROR"
    print reason.raiseException()

    reactor.stop()


def cbQuery(result, proto):
    print "QUERY OK"
    print result

    
    

def ebQuery(reason, proto):
    print "QUERY ERROR"
    print reason

    if 0:
        query = "ROLLBACK"
    else:
        query = "SELECT 1" # + "; COMMIT"

    proto.query(query
                ).addCallback(cbQuery2, proto
                              ).addErrback(ebQuery2, proto)
    
def cbQuery2(result, proto):
    print "QUERY OK"
    print result

    
    reactor.stop()


def ebQuery2(reason, proto):
    print "QUERY ERROR"
    print reason

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


