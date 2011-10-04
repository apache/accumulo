from JavaTest import JavaTest
import unittest

class DeleteRowsTest(JavaTest):
    "Test Delete Rows"

    order = 91
    testClass="org.apache.accumulo.server.test.functional.DeleteRowsTest"

def suite():
    result = unittest.TestSuite()
    result.addTest(DeleteRowsTest())
    return result
