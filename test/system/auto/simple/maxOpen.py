from JavaTest import JavaTest
import unittest

class MaxOpenTest(JavaTest):
    "Test doing lookups that exceed max files open"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.MaxOpenTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(MaxOpenTest())
    return result
