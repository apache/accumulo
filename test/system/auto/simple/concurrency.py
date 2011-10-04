from JavaTest import JavaTest
import unittest

class ConcurrencyTest(JavaTest):
    "Test concurrency"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.ConcurrencyTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(ConcurrencyTest())
    return result
