from JavaTest import JavaTest
import unittest

class BulkSplitOptimizationTest(JavaTest):
    "Bulk Import Split Optimization Test"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.BulkSplitOptimizationTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(BulkSplitOptimizationTest())
    return result
