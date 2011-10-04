from JavaTest import JavaTest
import unittest

class RangeTest(JavaTest):
    "Test scanning different ranges in accumulo"

    order = 21
    testClass = "org.apache.accumulo.server.test.functional.ScanRangeTest"

def suite():
    result = unittest.TestSuite()
    result.addTest(RangeTest())
    return result
