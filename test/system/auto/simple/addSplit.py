from JavaTest import JavaTest
import unittest

class AddSplitTest(JavaTest):
    "Test adding splits to a table"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.AddSplitTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(AddSplitTest())
    return result
