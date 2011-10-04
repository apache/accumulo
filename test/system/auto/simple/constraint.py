from JavaTest import JavaTest
import unittest

class ConstraintTest(JavaTest):
    "Test accumulo constraints"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.ConstraintTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(ConstraintTest())
    return result
