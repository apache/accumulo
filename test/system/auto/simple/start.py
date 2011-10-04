import unittest
import os
from TestUtils import TestUtilsMixin, ROOT, ROOT_PASSWORD, ACCUMULO_DIR
from subprocess import PIPE

class Start(TestUtilsMixin, unittest.TestCase):

    order = 21

    def start(self, *args):
        handle = self.runOn(self.masterHost(),
                            [self.accumulo_sh(), 'org.apache.accumulo.start.TestMain'] + list(args), stdin=PIPE)
        out, err = handle.communicate('')
        return handle.returncode

    def runTest(self):
        assert self.start() != 0
        assert self.start('success') == 0
        assert self.start('exception') != 0
        
def suite():
    result = unittest.TestSuite()
    result.addTest(Start())
    return result
