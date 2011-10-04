import os
import logging
import unittest
import time

from TestUtils import TestUtilsMixin

log = logging.getLogger('test.auto')

class WriteLots(unittest.TestCase, TestUtilsMixin):
    """Start a clean accumulo, ingest some data using lots of clients at once,
    and verify it was stored properly"""

    order = 30
    settings = TestUtilsMixin.settings.copy()
    settings['instance.zookeeper.timeout'] = '40s'

    def ingest(self, host, start, count, **kwargs):
        klass = 'org.apache.accumulo.server.test.TestIngest'
        args = '-tsbw -size 50 -random 56 %d %d 1' % (count, start)
        return self.runClassOn(host, klass, args.split(), **kwargs)

    def setUp(self):
        TestUtilsMixin.setUp(self);
        
        # initialize the database
        self.createTable("test_ingest")

    def tearDown(self):
        TestUtilsMixin.tearDown(self)
        self.pkill(self.masterHost(), 'TestIngest')

    def runTest(self):
        N = 10*len(self.hosts)
        waitTime = 60 * N * self.options.rows / 200000 + 30

        log.info("Starting %d clients", N)
        handles = []
        for i in range(N):
            # start test ingestion
            handle = self.ingest(self.hosts[i % len(self.hosts)],
                                 i * self.options.rows,
				 self.options.rows)
            handles.append(handle)

        end = time.time() + waitTime
        for handle in handles:
            waitTime = end - time.time()
            log.debug("Waiting %s seconds", waitTime)
            self.waitForStop(handle, waitTime)

        log.info("Verifying Ingestion")
        self.waitForStop(self.verify(self.masterHost(), self.options.rows * N),
                         waitTime)
        self.shutdown_accumulo()
        

def suite():
    result = unittest.TestSuite()
    result.addTest(WriteLots())
    return result
