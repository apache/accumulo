import unittest
import os
import logging

from TestUtils import ACCUMULO_DIR
from simple.binary import BinaryTest

log = logging.getLogger('test.auto')

class BinaryStressTest(BinaryTest) :
    order = 80

    tableSettings = BinaryTest.tableSettings.copy()
    tableSettings['bt'] = { 
    	'table.split.threshold': '10K',
        }
    settings = BinaryTest.settings.copy()
    settings.update({
        'tserver.memory.maps.max':'50K',
        'tserver.compaction.major.delay': 0,
        })

    def runTest(self):
        BinaryTest.runTest(self)
        handle = self.runOn(self.masterHost(), [
            'hadoop', 'fs', '-ls', os.path.join(ACCUMULO_DIR,'tables',self.getTableId('bt'))
            ])
        out, err = handle.communicate()
        if len(out.split('\n')) < 8:
            log.debug(out)
        self.assert_(len(out.split('\n')) > 7)

def suite():
    result = unittest.TestSuite()
    result.addTest(BinaryStressTest())
    return result
