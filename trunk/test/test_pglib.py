"""Test suite for pglib

$Id$

THIS SOFTWARE IS UNDER MIT LICENSE.
Copyright (c) 2006 Perillo Manlio (manlio.perillo@gmail.com)

Read LICENSE file for more informations.
"""


import os
import sys
sys.path.append("../")

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from zope.interface import implements

from twisted.python import log
from twisted.internet import reactor, defer, error
from twisted.trial import unittest

from pglib import ipg
from pglib import protocol



host = "localhost"
port = 5432

# set to False if SSL is not enabled on PostgreSQL server
SSL = False


# some error codes
CANCEL_ERROR_CODE = "57014" # canceling statement due to user request'
AUTHENTICATION_ERROR_CODE = "28000" # password authentication failed
                                    # for user "user-name"
# 28000 is also used for:
# no pg_hba.conf entry for host "host-address", user "user-name",
# database "database-name", SSL on/off
QUERY_ERROR_CODE = "42703" # column "column-name" does not exist'
COPY_ERROR_CODE = "42P01"  # relation "table-name" does not exist'

# some type's oid
INT_OID = 23
TEXT_OID = 25


# database setup
cmd = "psql -h %s -p %d -U pglib -d pglib -f postsetup.sql" % (host, port)
status = os.system(cmd)
if status != 0:
    raise Exception("database setup failed")

# XXX find the test functions oids
query = '"SELECT oid FROM pg_proc WHERE proname = %s"'
cmd = "psql -t --set format=unaligned -h " \
    "%s -p %d -U pglib -d pglib -c " % (host, port)

echoOid = os.popen(cmd + query % "'echo'").read()
loopOid = os.popen(cmd + query % "'loop'").read()

echoOid = int(echoOid.strip())
loopOid = int(loopOid.strip())


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
        self.closeDeferred = defer.Deferred()
        
        protocol.PgFactory.__init__(self, sslmode)
    
    def clientConnectionMade(self, protocol):
        self.deferred.callback(protocol)
        
    def clientConnectionLost(self, connector, reason):
        # connection close can require some time (as with TSL)
        self.closeDeferred.callback(None)
        
        log.msg("Lost connection.  Reason:", reason)
        
    def clientConnectionFailed(self, connector, reason):
        log.msg("Connection failed. Reason:", reason)


copyData = "1|pglib\n2|manlio\n3|perillo\n"

# implementation of required interfaces from ipg
class Producer(object):
    implements(ipg.IProducer)

    def __init__(self, fail=False):
        self.fail = fail
        
        self.fp = StringIO(copyData)
        
    def description(self, ntuples, binaryTuples):
        self.ntuples = ntuples
        self.binaryTuples = binaryTuples
        
    def read(self):
        if self.fail:
            raise Exception("copy failed")
        
        return self.fp.readline()

    def close(self):
        result = protocol.Result()
        
        # XXX
        result.ntuples = self.ntuples
        result.binaryTuples = self.binaryTuples

        result.status = protocol.PGRES_COPY_IN
        
        return result

class Consumer(object):
    implements(ipg.IConsumer)

    def __init__(self):
        self.fp = StringIO()
        
    def description(self, ntuples, binaryTuples):
        self.ntuples = ntuples
        self.binaryTuples = binaryTuples
        
    def write(self, data):
        self.fp.write(data)

    def close(self):
        self.data = self.fp.getvalue()

        result = protocol.Result()
        
        # XXX
        result.ntuples = self.ntuples
        result.binaryTuples = self.binaryTuples
        
        result.status = protocol.PGRES_COPY_OUT
        
        return result

        

class TestCaseCommon(unittest.TestCase):
    """Common methods for our Test Case
    """
    
    timeout = 5
    
    def setUp(self):
        return self.connect()
    
    def tearDown(self):
        self.protocol.finish()
        
        # make sure to wait for connection close
        return self.closeDeferred
    
    def connect(self, sslmode="prefer"):
        def setup(protocol):
            self.protocol = protocol
    
        factory = TestFactory(sslmode)
        self.closeDeferred = factory.closeDeferred
        self.connector = reactor.connectTCP(host, port, factory)
        
        return factory.deferred.addCallback(setup)

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
        def ebLogin(reason):
            code = reason.value.args["C"]
            self.failUnlessEqual(code, AUTHENTICATION_ERROR_CODE)

            return reason
        
        d = self.protocol.login(
            user="pglib_clear", password="xxx", database="pglib"
            )
        
        d.addErrback(ebLogin)
        return self.failUnlessFailure(d, protocol.PgError)
    
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
        
        def ebQuery(reason):
            code = reason.value.args["C"]
            self.failUnlessEqual(code, QUERY_ERROR_CODE)

            return reason
        
        d = self.login().addCallback(cbLogin
                                     ).addErrback(ebQuery)
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
            self.failUnlessEqual(result.status, protocol.PGRES_EMPTY_QUERY)
        
        d = self.login()
        return d.addCallback(cbLogin
                             ).addCallback(cbQuery
                                           )

    def testSelect(self):
        def cbLogin(params):
            return self.protocol.execute("""
            SELECT x, s FROM TestR ORDER BY x
            """)
            
        def cbQuery(result):
            desc1, desc2 = result.descriptions
            
            self.failUnlessEqual(result.status,
                                 protocol.PGRES_TUPLES_OK)
            self.failUnlessEqual(result.nfields, 2)
            self.failUnlessEqual(result.ntuples, 2)
            
            self.failUnlessEqual(desc1.ftype, INT_OID)
            self.failUnlessEqual(desc2.ftype, TEXT_OID)
            
            self.failUnlessEqual(desc1.fname, "x")
            self.failUnlessEqual(desc2.fname, "s")
            
            self.failUnlessEqual(result.rows, 
                                 [["1", "A"], ["2", "B"]])
            
        d = self.login()
        return d.addCallback(cbLogin
                             ).addCallback(cbQuery
                                           )

    def testInsert(self):
        def cbLogin(params):
            return self.protocol.execute("""
            INSERT INTO TestRW VALUES (3, 'C')
            """)
            
        def cbQuery(result):
            self.failUnlessEqual(result.status,
                                 protocol.PGRES_COMMAND_OK)
            self.failUnlessEqual(result.nfields, 0)
            self.failUnlessEqual(result.ntuples, 0)
            
            self.failUnlessEqual(result.descriptions, [])
            self.failUnlessEqual(result.rows, [])
            self.failUnlessEqual(result.cmdTuples, 1)
            
        d = self.login()
        return d.addCallback(cbLogin
                             ).addCallback(cbQuery
                                           )

    def testUpdate(self):
        def cbLogin(params):
            return self.protocol.execute("""
            UPDATE TestRW SET s = 'Z' WHERE x = 2
            """)
            
        def cbQuery(result):
            self.failUnlessEqual(result.status,
                                 protocol.PGRES_COMMAND_OK)
            self.failUnlessEqual(result.nfields, 0)
            self.failUnlessEqual(result.ntuples, 0)
            
            self.failUnlessEqual(result.descriptions, [])
            self.failUnlessEqual(result.rows, [])
            self.failUnlessEqual(result.cmdTuples, 1)
            
        d = self.login()
        return d.addCallback(cbLogin
                             ).addCallback(cbQuery
                                           )
    
    def testDelete(self):
        def cbLogin(params):
            return self.protocol.execute("""
            DELETE FROM TestRW WHERE x = 1
            """)
            
        def cbQuery(result):
            self.failUnlessEqual(result.status,
                                 protocol.PGRES_COMMAND_OK)
            self.failUnlessEqual(result.nfields, 0)
            self.failUnlessEqual(result.ntuples, 0)
            
            self.failUnlessEqual(result.descriptions, [])
            self.failUnlessEqual(result.rows, [])
            self.failUnlessEqual(result.cmdTuples, 1)
            
        d = self.login()
        return d.addCallback(cbLogin
                             ).addCallback(cbQuery
                                           )
    
    

class TestFunctionCall(TestCaseCommon):
    def testFunction(self):
        def cbLogin(params):
            # call the echo function
            return self.protocol.fn(echoOid, 0, "echo")
            
        def cbCall(result):
            self.failUnlessEqual(result, "echo")

        
        d = self.login()
        return d.addCallback(cbLogin
                             ).addCallback(cbCall
                                           )


class TestNotification(TestCaseCommon):
    def testNotification(self):
        def cbLogin(params):
            # register ourself for listening a notification, and raise it
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
            # call the loop function
            return self.protocol.fn(loopOid, 0)

        def ebCall(reason):
            code = reason.value.args["C"]
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
    # XXX sslmode "prefer" and "allow" cannot be tested
    def setUp(self):
        pass


    if SSL:
        def testSSLRequire(self):
            def cbConnect(result):
                return self.protocol.login(
                    user="pglib_ssl", password="test", database="pglib"
                    )
        
            def cbLogin(params):
                self.failUnless(isinstance(params, dict))
        

            d = self.connect("require")
            return d.addCallback(cbConnect
                                 ).addCallback(cbLogin
                                               )

        def testSSLRequireFail(self):
            def cbConnect(result):
                return self.protocol.login(
                    user="pglib_nossl", password="test", database="pglib"
                    )
        
            def ebLogin(reason):
                code = reason.value.args["C"]
                self.failUnlessEqual(code, AUTHENTICATION_ERROR_CODE)
                
                return reason
        
            d = self.connect("require")
            d.addCallback(cbConnect
                          ).addErrback(ebLogin
                                       )
            
            return self.failUnlessFailure(d, protocol.PgError)
    else:
        def tearDown(self):
            pass
        
        def testSSLRequireFail(self):
            factory = TestFactory("require")
            self.closeDeferred = factory.closeDeferred
            self.connector = reactor.connectTCP(host, port, factory)
        
            # the connection is aborted
            return factory.closeDeferred.addCallback(lambda _: None)

            
    def testSSLDisable(self):
        def cbConnect(result):
            return self.protocol.login(
                user="pglib_nossl", password="test", database="pglib"
                )
        
        def cbLogin(params):
            self.failUnless(isinstance(params, dict))
        
            self.protocol.finish()
        
            # make sure to wait for connection close
            return self.closeDeferred
    
        d = self.connect("disable")
        return d.addCallback(cbConnect
                             ).addCallback(cbLogin
                                           )

    def testSSLDisableFail(self):
        def cbConnect(result):
            return self.protocol.login(
                user="pglib_ssl", password="test", database="pglib"
                )
        
        def ebLogin(reason):
            code = reason.value.args["C"]
            self.failUnlessEqual(code, AUTHENTICATION_ERROR_CODE)
        
            return reason
        
        d = self.connect("disable")
        d.addCallback(cbConnect
                      ).addErrback(ebLogin
                                   )
        
        return self.failUnlessFailure(d, protocol.PgError)


class TestCopy(TestCaseCommon):
    def testCopyIn(self):
        def cbLogin(params):
            self.protocol.producer = Producer()
            
            return self.protocol.execute("""
            COPY TestCopyRW FROM STDIN WITH delimiter '|'
            """)
                
        def cbCopy(result):
            self.failUnlessEqual(result.status, protocol.PGRES_COPY_IN)
            self.failUnlessEqual(result.cmdStatus, "COPY")
#            self.failUnlessEqual(result.cmdTuples, 3)
                                 
        d = self.login().addCallback(cbLogin
                                     ).addCallback(cbCopy)
        return d

    def testCopyOut(self):
        def cbLogin(params):
            self.protocol.consumer = Consumer()
            
            return self.protocol.execute("""
            COPY TestCopyR TO STDOUT WITH delimiter '|'
            """)
        
        def cbCopy(result):
            data = self.protocol.consumer.data

            self.failUnlessEqual(result.status, protocol.PGRES_COPY_OUT)
            self.failUnlessEqual(result.cmdStatus, "COPY")
            self.failUnlessEqual(data, copyData)
                
        d = self.login().addCallback(cbLogin
                                     ).addCallback(cbCopy)
        return d

    def testCopyInFail(self):
        def cbLogin(params):
            self.protocol.producer = Producer(fail=True)
            
            return self.protocol.execute("""
            COPY TestCopy FROM STDIN WITH delimiter '|'
            """)
                
        def ebCopy(reason):
            message = reason.value.args["M"]
            
            # XXX the message should be:
            # COPY from stdin failed: copy failed
            self.failUnlessIn("copy failed", message)
            
            return reason
        
        d = self.login().addCallback(cbLogin
                                     ).addErrback(ebCopy)
        
        return self.failUnlessFailure(d, protocol.PgError)

    def testCopyOutFail(self):
        def cbLogin(params):
            self.protocol.consumer = Consumer()
            
            return self.protocol.execute("""
            COPY XXX TO STDOUT WITH delimiter '|'
            """)
        
        def ebCopy(reason):
            code = reason.value.args["C"]
            self.failUnlessEqual(code, COPY_ERROR_CODE)
            
            return reason
        
        d = self.login().addCallback(cbLogin
                                     ).addErrback(ebCopy)
        
        return self.failUnlessFailure(d, protocol.PgError)
