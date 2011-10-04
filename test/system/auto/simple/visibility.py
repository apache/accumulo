from JavaTest import JavaTest
import unittest

class VisibilityTest(JavaTest):
    "Test Column Visibility"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.VisibilityTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(VisibilityTest())
    return result
