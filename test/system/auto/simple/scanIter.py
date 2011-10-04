from JavaTest import JavaTest
import unittest

class ScanIteratorTest(JavaTest):
    "Test setting iterators at scan time"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.ScanIteratorTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(ScanIteratorTest())
    return result
