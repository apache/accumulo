from JavaTest import JavaTest
import unittest

class RowDeleteTest(JavaTest):
    "Row Deletion Test"

    order = 22
    testClass="org.apache.accumulo.server.test.functional.RowDeleteTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(RowDeleteTest())
    return result
