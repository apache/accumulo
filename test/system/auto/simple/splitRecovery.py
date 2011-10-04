from JavaTest import JavaTest
import unittest

class SplitRecoveryTest(JavaTest):
    "Test recovery of partial splits"

    order = 20
    testClass="org.apache.accumulo.server.test.functional.SplitRecoveryTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(SplitRecoveryTest())
    return result
