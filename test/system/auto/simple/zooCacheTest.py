import os
import unittest
import time

from TestUtils import TestUtilsMixin, ACCUMULO_HOME, SITE, ZOOKEEPERS

class ZooCacheTest(TestUtilsMixin, unittest.TestCase):
    "Zoo Cache Test"

    order = 21
    testClass=""

    def setUp(self):
        self.create_config_file(self.settings.copy())
        
    def runTest(self):
        handleCC = self.runClassOn('localhost', 'org.apache.accumulo.server.test.functional.CacheTestClean', ['/zcTest-42','/tmp/zcTest-42'])
        self.waitForStop(handleCC, 10)
        handleR1 = self.runClassOn('localhost', 'org.apache.accumulo.server.test.functional.CacheTestReader', ['/zcTest-42','/tmp/zcTest-42', ZOOKEEPERS])
        handleR2 = self.runClassOn('localhost', 'org.apache.accumulo.server.test.functional.CacheTestReader', ['/zcTest-42','/tmp/zcTest-42', ZOOKEEPERS])
        handleR3 = self.runClassOn('localhost', 'org.apache.accumulo.server.test.functional.CacheTestReader', ['/zcTest-42','/tmp/zcTest-42', ZOOKEEPERS])
        handleW = self.runClassOn('localhost', 'org.apache.accumulo.server.test.functional.CacheTestWriter', ['/zcTest-42','/tmp/zcTest-42','3','500'])
        self.waitForStop(handleW, 60)
        self.waitForStop(handleR1, 1)
        self.waitForStop(handleR2, 1)
        self.waitForStop(handleR3, 1)

    def tearDown(self):
        os.unlink(os.path.join(ACCUMULO_HOME, 'conf', SITE))

def suite():
    result = unittest.TestSuite()
    result.addTest(ZooCacheTest())
    return result
