from JavaTest import JavaTest
import unittest

class CreateAndUseTest(JavaTest):
    "Test creating and immediately using a table"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.CreateAndUseTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(CreateAndUseTest())
    return result
