"""Test suite for pglib

$Id$

THIS SOFTWARE IS UNDER MIT LICENSE.
(C) 2006 Perillo Manlio (manlio.perillo@gmail.com)

Read LICENSE file for more informations.
"""


import sys
sys.path.append("../")

from twisted.python import log
from twisted.internet import reactor, defer, error
from twisted.trial import unittest

import protocol



host = "localhost"
port = 5432

# test functions oids (you have to know this) # XXX
# SELECT oid FROM pg_proc WHERE proname = 'echo';
# XXX TODO retrieve these with a query
echoOid = 19196
loopOid = 19197

# error code for a cancelled request
CANCEL_ERROR_CODE = 57014


# utility function
def waitFor(secs):
    """Return a deferred that will fire after the specified amount of
    seconds.
    """
    
    from twisted.internet import reactor
    
    d = defer.Deferred()
    reactor.callLater(secs, lambda: d.callback(None))
    
    return d


class TestFactory(protocol.PgFactory):
    def __init__(self, sslmode="prefer"):
        self.deferred = defer.Deferred()
        protocol.PgFactory.__init__(self, sslmode)
    
    def clientConnectionMade(self, protocol):
        self.deferred.callback(protocol)
        
    def clientConnectionLost(self, connector, reason):
        log.msg("Lost connection.  Reason:", reason)
        
    def clientConnectionFailed(self, connector, reason):
        log.msg("Connection failed. Reason:", reason)


class TestCaseCommon(unittest.TestCase):
    """Common methods for our Test Case
    """
    
    timeout = 5
    
    def setUp(self):
        def setup(protocol):
            self.protocol = protocol
    
        factory = TestFactory()
        self.connector = reactor.connectTCP(host, port, factory)
        
        return factory.deferred.addCallback(setup)

    def tearDown(self):
        self.protocol.finish()
    
    
    def login(self):
        return self.protocol.login(
            user="pglib_md5", password="test", database="pglib"
            )
    
    # XXX TODO
    def getFnOid(self):
        pass


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
            self.failUnless(isinstance(params, dict))
        
        d = self.protocol.login(
            user="pglib_md5", password="test", database="pglib"
            )
        
        return d.addCallback(callback)

    def testNoUser(self):
        d = self.protocol.login(database="pglib")
        return self.failUnlessFailure(d, protocol.AuthenticationError)
    

class TestSimpleQuery(TestCaseCommon):
    def testQuery(self):
        def cbLogin(params):
            return self.protocol.execute("SELECT 1")
            
        def cbQuery(result):
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
            return self.protocol.execute("SELECT xxx")
            
        d = self.login().addCallback(cbLogin)
        return self.failUnlessFailure(d, protocol.PgError)

    def testTransaction(self):
        def cbLogin(params):
            return self.protocol.execute("BEGIN; SELECT 1")
            
        def cbQuery(result):
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
            return self.protocol.execute("BEGIN; SELECT xxx")
            
        def ebQuery(reason):
            self.failUnlessEqual(self.protocol.status,
                                 protocol.CONNECTION_OK)

            self.failUnlessEqual(self.protocol.transactionStatus,
                                 protocol.PGTRANS_INERROR)

            return reason
        
        d = self.login().addCallback(cbLogin
                                     ).addErrback(ebQuery
                                                  )
        return self.failUnlessFailure(d, protocol.PgError)

    def testMultipleQuery(self):
        def query(result, i):
            print "i =", i
            return self.protocol.execute("SELECT %d" % i)
        
        
        d = self.login().addCallback(query, 0)
        for i in range(1, 5):
            d.addCallback(query, i)

        return d

    def testEmptyQuery(self):
        def cbLogin(params):
            return self.protocol.execute("")
            
        def cbQuery(result):
            self.failUnlessEqual(self.protocol.status,
                                 protocol.CONNECTION_OK)

            self.failUnlessEqual(result, "empty query")
        
        d = self.login()
        return d.addCallback(cbLogin
                             ).addCallback(cbQuery
                                           )


class TestFunctionCall(TestCaseCommon):
    def testFunction(self):
        def cbLogin(params):
            # call the echo function
            return self.protocol.fn(echoOid, 0, 'echo')
            
        def cbCall(result):
            self.failUnlessEqual(result, 'echo')

        
        d = self.login()
        return d.addCallback(cbLogin
                             ).addCallback(cbCall
                                           )


class TestNotification(TestCaseCommon):
    def testNotification(self):
        def cbLogin(params):
            # register oursel for listening a notification, and raise it
            return self.protocol.execute(
                "LISTEN pglib; NOTIFY pglib;"
                )
            
        def cbQuery(result):
            return waitFor(0.5)


        def cbNotify(result):
            notification = self.protocol.lastNotify
            self.failUnlessEqual(notification.name, "pglib")
        
        d = self.login()
        return d.addCallback(cbLogin
                             ).addCallback(cbQuery
                                           ).addCallback(cbNotify
                                                         )

class TestCancel(TestCaseCommon):
    def testCancelVoid(self):
        def cbLogin(params):
            cancelObj = self.protocol.getCancel()
            return cancelObj.cancel()
            
                
        d = self.login().addCallback(cbLogin)
        return d

    def _testCancelVoidFail(self):
        # XXX TODO
        def cbLogin(params):
            cancelObj = self.protocol.getCancel()
            # force a timeout error for the cancel connector
            return cancelObj.cancel(0.01) # XXX the value is critical
            
                
        d = self.login().addCallback(cbLogin)
        return self.failUnlessFailure(d, error.TimeoutError)

    def testCancel(self):
        def cbLogin(params):
            return self.protocol.fn(loopOid, 0)

        def ebCall(reason):
            code = int(reason.value.args["C"])
            self.failUnlessEqual(code, CANCEL_ERROR_CODE)

            return reason
        
        def cancel():
            cancelObj = self.protocol.getCancel()
            cancelObj.cancel().addCallback(lambda _: None)
            
                
        d = self.login().addCallback(cbLogin
                                     ).addErrback(ebCall
                                                  )

        reactor.callLater(2, cancel)
        return self.failUnlessFailure(d, protocol.PgError)


class TestSSL(TestCaseCommon):
    def setUp(self):
        def setup(protocol):
            self.protocol = protocol
    
        factory = TestFactory("disable")
        self.connector = reactor.connectTCP(host, port, factory)
        
        return factory.deferred.addCallback(setup)

    def testClearText(self):
        def callback(params):
            self.failUnless(isinstance(params, dict))
        
        d = self.protocol.login(
            user="pglib_ssl", password="test", database="pglib"
            )
        
        return d.addCallback(callback)
