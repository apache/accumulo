# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
        self.waitForStop(handleW, 90)
        self.waitForStop(handleR1, 1)
        self.waitForStop(handleR2, 1)
        self.waitForStop(handleR3, 1)

    def tearDown(self):
        os.unlink(os.path.join(ACCUMULO_HOME, 'conf', SITE))

def suite():
    result = unittest.TestSuite()
    result.addTest(ZooCacheTest())
    return result
