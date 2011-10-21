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

testClass = "org.apache.accumulo.server.test.TestBinaryRows"

class BinaryTest(unittest.TestCase, TestUtilsMixin):
    "Test inserting binary data into accumulo"

    order = 21

    def setUp(self):
        TestUtilsMixin.setUp(self);
        
        # initialize the database
        self.createTable("bt")
    def test(self, *args):
        handle = self.runClassOn(self.masterHost(), testClass, list(args))
        self.waitForStop(handle, 200)
        
    def tearDown(self):
        TestUtilsMixin.tearDown(self)

    def runTest(self):
        self.test("ingest","bt","0","100000")
        self.test("verify","bt","0","100000")
        self.test("randomLookups","bt","0","100000")
        self.test("delete","bt","25000","50000")
        self.test("verify","bt","0","25000")
        self.test("randomLookups","bt","0","25000")
        self.test("verify","bt","75000","25000")
        self.test("randomLookups","bt","75000","25000")
        self.test("verifyDeleted","bt","25000","50000")
        self.shutdown_accumulo()

class BinaryPreSplitTest(BinaryTest):
    "Test inserting binary data into accumulo with a presplit table (this will place binary data in !METADATA)"

    def setUp(self):
        TestUtilsMixin.setUp(self);
        self.test("split","bt","8","256")


def suite():
    result = unittest.TestSuite()
    result.addTest(BinaryTest())
    result.addTest(BinaryPreSplitTest())
    return result
