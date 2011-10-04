import os
import logging
import unittest
import time

from TestUtils import TestUtilsMixin

log = logging.getLogger('test.auto')

class AggregationTest(TestUtilsMixin, unittest.TestCase):
    "Start a clean accumulo, use an aggregator, verify the data is aggregated"

    order = 25

    def checkSum(self):
        # check the scan
        out, err, code = self.shell(self.masterHost(),"table test\nscan\n")
        self.assert_(code == 0)
        for line in out.split('\n'):
            if line.find('row1') == 0:
                self.assert_(int(line.split()[-1]) == sum(range(10)))
                break
        else:
            self.fail("Unable to find needed output in %r" % out)
        
    def runTest(self):

        # initialize the database
        aggregator = 'org.apache.accumulo.core.iterators.aggregation.StringSummation'
        cmd = 'createtable test -a cf=' + aggregator
        out, err, code = self.rootShell(self.masterHost(),"%s\n" % cmd)
        self.assert_(code == 0)

        # insert some rows
        log.info("Starting Test Ingester")
        cmd = 'table test\n';
        for i in range(10):
            cmd += 'insert row1 cf col1 %d\n' % i
        out, err, code = self.rootShell(self.masterHost(), cmd)
        self.assert_(code == 0)
        self.checkSum()
        self.shutdown_accumulo()
        self.start_accumulo()
        self.checkSum()

def suite():
    result = unittest.TestSuite()
    result.addTest(AggregationTest())
    return result
