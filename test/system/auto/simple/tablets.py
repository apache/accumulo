import os
import logging
import unittest
import time
from subprocess import PIPE

from TestUtils import TestUtilsMixin

log = logging.getLogger('test.auto')

N = 10000    # rows to insert, 100 rows per tablet
WAIT = (N / 1000. + 1) * 60

class LotsOfTablets(TestUtilsMixin, unittest.TestCase):

    order = 80

    settings = TestUtilsMixin.settings.copy()
    settings.update({
    	'table.split.threshold':200,
        'tserver.memory.maps.max':'128M'
        })

    def runTest(self):

        # initialize the database
        handle = self.runClassOn(self.masterHost(), 
		               'org.apache.accumulo.server.test.CreateTestTable', 
		               [str(N)])
	self.waitForStop(handle, WAIT)
        handle = self.runClassOn(self.masterHost(), 
		               'org.apache.accumulo.server.test.CreateTestTable', 
		               ['-readonly', str(N)])
        self.waitForStop(handle, WAIT)

def suite():
    result = unittest.TestSuite()
    result.addTest(LotsOfTablets())
    return result
