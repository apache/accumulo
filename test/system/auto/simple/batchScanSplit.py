from JavaTest import JavaTest
import unittest

class BatchScanSplitTest(JavaTest):
    "Test batch scanning a table that is splitting"

    order = 30
    testClass="org.apache.accumulo.server.test.functional.BatchScanSplitTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(BatchScanSplitTest())
    return result
