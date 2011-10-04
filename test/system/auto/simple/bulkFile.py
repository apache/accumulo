from JavaTest import JavaTest
import unittest

class BulkFileTest(JavaTest):
    "Test bulk import of different file types"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.BulkFileTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(BulkFileTest())
    return result
