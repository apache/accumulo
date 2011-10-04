import os
import logging
import unittest
from simple.bulk import SimpleBulkTest

N = 100000
COUNT = 5

log = logging.getLogger('test.auto')

class CompactionTest(SimpleBulkTest):
    "Start a clean accumulo, bulk import a lot of map files, read while a multi-pass compaction is happening"

    order = 26

    tableSettings = SimpleBulkTest.tableSettings.copy()
    tableSettings['test_ingest'] = { 
    	'table.compaction.major.ratio': 1.0
        }
    settings = SimpleBulkTest.settings.copy()
    settings.update({
        'tserver.compaction.major.files.open.max':4,
        'tserver.compaction.major.delay': 1,
        'tserver.compaction.major.concurrent.max':1,
        'tserver.files.open.max': 100
        })

    def createMapFiles(self, host):
        handle = self.runClassOn(
            self.masterHost(),
            'org.apache.accumulo.server.test.CreateMapFiles',
            "testmf 4 0 500000 59".split())
        out, err = handle.communicate()
        self.assert_(handle.returncode == 0)

    def runTest(self):

        # initialize the database
        self.createTable('test_ingest')
        self.execute(self.masterHost(), 'hadoop dfs -rmr /testmf'.split())
        self.execute(self.masterHost(), 'hadoop dfs -rmr /testmfFail'.split())

        # insert some data
        self.createMapFiles(self.masterHost())
        self.bulkLoad(self.masterHost())

        out, err, code = self.shell(self.masterHost(), "table !METADATA\nscan -b ! -c ~tab,file\n")
        self.assert_(code == 0)

        beforeCount = len(out.split('\n'))

        log.info("Verifying Ingestion")
        for c in range(5):
            handles = []
            for i in range(COUNT):
                handles.append(self.verify(self.hosts[i%len(self.hosts)], N, i * N))
            for h in handles:
                out, err = h.communicate()
                self.assert_(h.returncode == 0)

        out, err, code = self.shell(self.masterHost(), "table !METADATA\nscan -b ! -c ~tab,file\n")
        self.assert_(code == 0)

        afterCount = len(out.split('\n'))

        self.assert_(afterCount < beforeCount)

        self.shutdown_accumulo()

def suite():
    result = unittest.TestSuite()
    result.addTest(CompactionTest())
    return result
