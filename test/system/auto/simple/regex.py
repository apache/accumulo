from JavaTest import JavaTest
import unittest

class RegExTest(JavaTest):
    "Test accumulo scanner regex"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.RegExTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(RegExTest())
    return result
