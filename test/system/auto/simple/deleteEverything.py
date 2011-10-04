from JavaTest import JavaTest
import unittest

class DeleteEverythingTest(JavaTest):
    "A test that deletes everything in a table"

    order = 30
    testClass="org.apache.accumulo.server.test.functional.DeleteEverythingTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(DeleteEverythingTest())
    return result
