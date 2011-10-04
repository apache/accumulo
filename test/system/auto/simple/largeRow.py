from JavaTest import JavaTest
import unittest

class LargeRowTest(JavaTest):
    "Test large rows"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.LargeRowTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(LargeRowTest())
    return result
