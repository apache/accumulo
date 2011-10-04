import os
import logging
import unittest
import time

from TestUtils import TestUtilsMixin

log = logging.getLogger('test.auto')

import readwrite

class RenameTest(readwrite.SunnyDayTest):
    "Injest some data, rename the table, verify the data is still there"

    order = 28

    def runTest(self):
        self.waitForStop(self.ingester, 100)

        log.info("renaming table")
        self.shell(self.masterHost(),
                   'renametable test_ingest renamed\n'
                   'createtable test_ingest\n')
        self.waitForStop(self.ingest(self.masterHost(),
                                     self.options.rows,
                                     self.options.rows), 100)
        self.waitForStop(self.verify(self.masterHost(),
                                     self.options.rows,
                                     self.options.rows), 100)
        self.shell(self.masterHost(),
                   'deletetable test_ingest\n'
                   'renametable renamed test_ingest\n')
        self.waitForStop(self.verify(self.masterHost(), self.options.rows), 100)

def suite():
    result = unittest.TestSuite()
    result.addTest(RenameTest())
    return result

        
        
