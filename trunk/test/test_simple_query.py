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


class Test(protocol.PgProtocol):
    def connectionMade(self):
        print "connection made"
        self.login(user=user, password=password)

        self.count = 0

    def message_Z(self, transStatus):
        print "transaction status:", transStatus
    
        if self.count == 0:
            self.query("BEGIN; SELECT x")
        elif self.count == 1:
            self.query("ROLLBACK") #"SELECT 1")#COMMIT")
        elif self.count == 2:
            self.query("CREATE TABLE test (x INTEGER)")
        
        self.count += 1

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


