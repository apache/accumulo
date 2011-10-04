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
