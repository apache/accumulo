from JavaTest import JavaTest
import unittest

class BadIterMincTest(JavaTest):
    "Test bad iterator and minc"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.BadIteratorMincTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(BadIterMincTest())
    return result
