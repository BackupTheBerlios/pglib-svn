"""Test suite for pglib

$Id$

THIS SOFTWARE IS UNDER MIT LICENSE.
(C) 2006 Perillo Manlio (manlio.perillo@gmail.com)

Read LICENSE file for more informations.
"""


import sys
sys.path.append("../")

from twisted.python import log
from twisted.internet import reactor, defer
from twisted.trial import unittest

import protocol



host = "localhost"
port = 5432


class TestFactory(protocol.PgFactory):
    def __init__(self):
        self.deferred = defer.Deferred()

    def clientConnectionMade(self, protocol):
        self.deferred.callback(protocol)
        
    def clientConnectionLost(self, connector, reason):
        log.msg("Lost connection.  Reason:", reason)
        
    def clientConnectionFailed(self, connector, reason):
        log.msg("Connection failed. Reason:", reason)


class TestCaseCommon(unittest.TestCase):
    """Common methods for our Test Case
    """
    
    def setUp(self):
        def setup(protocol):
            self.protocol = protocol
    
        factory = TestFactory()
        self.connector = reactor.connectTCP(host, port, factory)
        
        return factory.deferred.addCallback(setup)

    def tearDown(self):
        self.protocol.transport.loseConnection()
    
    
    def login(self):
        return self.protocol.login(
            user="pglib_md5", password="test", database="pglib"
            )
    

class TestLogin(TestCaseCommon):
    def testTrust(self):
        def callback(params):
            print params
            self.failUnless(isinstance(params, dict))
        
        d = self.protocol.login(
            user="pglib", database="pglib"
            )
        
        return d.addCallback(callback)
    
    def testClearText(self):
        def callback(params):
            print params
            self.failUnless(isinstance(params, dict))
        
        d = self.protocol.login(
            user="pglib_clear", password="test", database="pglib"
            )
        
        return d.addCallback(callback)

    def testClearTextNoPassword(self):
        d = self.protocol.login(
            user="pglib_clear", database="pglib"
            )
        
        
        return self.failUnlessFailure(d, protocol.AuthenticationError)
    
    def testClearTextFail(self):
        d = self.protocol.login(
            user="pglib_clear", password="xxx", database="pglib"
            )
        
        
        self.failUnlessFailure(d, protocol.PgError)
        return d
    
    
    def testMD5(self):
        def callback(params):
            print params
            self.failUnless(isinstance(params, dict))
        
        d = self.protocol.login(
            user="pglib_md5", password="test", database="pglib"
            )
        
        return d.addCallback(callback)

    
class TestSimpleQuery(TestCaseCommon):
    def testQuery(self):
        def cbLogin(params):
            return self.protocol.query("SELECT 1")
            
        def cbQuery(result):
            print self.protocol.status
            print self.protocol.transactionStatus
            self.failUnlessEqual(self.protocol.status,
                                 protocol.CONNECTION_OK)

            self.failUnlessEqual(self.protocol.transactionStatus,
                                 protocol.PGTRANS_IDLE)
        
        d = self.login()
        return d.addCallback(cbLogin
                             ).addCallback(cbQuery
                                           )

    def testQueryFail(self):
        def cbLogin(params):
            return self.protocol.query("SELECT xxx")
            
        d = self.login().addCallback(cbLogin)
        return self.failUnlessFailure(d, protocol.PgError)

    def testTransaction(self):
        def cbLogin(params):
            return self.protocol.query("BEGIN; SELECT 1")
            
        def cbQuery(result):
            print self.protocol.status
            print self.protocol.transactionStatus
            self.failUnlessEqual(self.protocol.status,
                                 protocol.CONNECTION_OK)

            self.failUnlessEqual(self.protocol.transactionStatus,
                                 protocol.PGTRANS_INTRANS)
        
        d = self.login()
        return d.addCallback(cbLogin
                             ).addCallback(cbQuery
                                           )

    def testTransactionFail(self):
        def cbLogin(params):
            return self.protocol.query("BEGIN; SELECT xxx")
            
        def ebQuery(reason):
            print self.protocol.status
            print self.protocol.transactionStatus
            self.failUnlessEqual(self.protocol.status,
                                 protocol.CONNECTION_OK)

            self.failUnlessEqual(self.protocol.transactionStatus,
                                 protocol.PGTRANS_INERROR)

            return reason
        
        d = self.login().addCallback(cbLogin
                                     ).addErrback(ebQuery
                                                  )
        return self.failUnlessFailure(d, protocol.PgError)