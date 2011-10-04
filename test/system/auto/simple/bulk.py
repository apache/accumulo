import os
import logging
import unittest
import time

from TestUtils import TestUtilsMixin, ROOT, ROOT_PASSWORD

log = logging.getLogger('test.auto')

N = 100000
COUNT = 5

class SimpleBulkTest(TestUtilsMixin, unittest.TestCase):
    "Start a clean accumulo, make some bulk data and import it"

    order = 25

    def testIngest(self, host, args, **kwargs):
        return self.runClassOn(host,
                             'org.apache.accumulo.server.test.TestIngest',
                             args,
                             **kwargs)

    def bulkLoad(self, host):
        handle = self.runClassOn(
            self.masterHost(),
            'org.apache.accumulo.server.test.BulkImportDirectory',
            [ROOT, ROOT_PASSWORD,
             'test_ingest', '/testmf', '/testmfFail'])
        self.wait(handle)
        self.assert_(handle.returncode == 0)
        

    def createMapFiles(self):
        args = '-mapFile /testmf/mf%02d -timestamp 1 -size 50 -random 56 %1d %ld 1'
        log.info('creating map files')
        handles = []
        for i in range(COUNT):
            handles.append(self.testIngest(
                self.hosts[i%len(self.hosts)],
                (args % (i, N, (N * i))).split()))
        
        #create a map file with one entry, there was a bug with this
        handles.append(self.testIngest(self.hosts[0], (args % (COUNT, 1, COUNT * N)).split()))
        log.info('waiting to finish')
        for h in handles:
            h.communicate()
            self.assert_(h.returncode == 0)
        log.info('done')

    def execute(self, host, cmd, **opts):
        handle = self.runOn(host, cmd, **opts)
        out, err = handle.communicate()
        return out, err, handle.returncode

    def runTest(self):

        # initialize the database
        self.createTable('test_ingest')
        self.execute(self.masterHost(), 'hadoop dfs -rmr /testmf'.split())
        self.execute(self.masterHost(), 'hadoop dfs -rmr /testmfFail'.split())
        self.execute(self.masterHost(), 'hadoop dfs -mkdir /testmfFail'.split())

        # insert some data
        self.createMapFiles()
        self.bulkLoad(self.masterHost())

        log.info("Verifying Ingestion")
        handles = []
        for i in range(COUNT):
            handles.append(self.verify(self.hosts[i%len(self.hosts)], N, i * N))
        handles.append(self.verify(self.hosts[0], 1, COUNT * N))
        for h in handles:
            out, err = h.communicate()
            self.assert_(h.returncode == 0)

        self.shutdown_accumulo()

def suite():
    result = unittest.TestSuite()
    result.addTest(SimpleBulkTest())
    return result
