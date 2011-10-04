import os
import unittest
import time

from TestUtils import TestUtilsMixin

class NativeMapTest(TestUtilsMixin, unittest.TestCase):
    "Native Map Unit Test"

    order = 21
    testClass=""

    def setUp(self):
        pass
        
    def runTest(self):
        handle = self.runClassOn('localhost', 'org.apache.accumulo.server.test.functional.NativeMapTest', [])
        self.waitForStop(handle, 20)

    def tearDown(self):
        pass

def suite():
    result = unittest.TestSuite()
    result.addTest(NativeMapTest())
    return result
