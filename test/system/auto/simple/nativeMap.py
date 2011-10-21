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
