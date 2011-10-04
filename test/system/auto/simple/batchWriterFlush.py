from JavaTest import JavaTest
import unittest

class BatchWriterFlushTest(JavaTest):
    "Test batch writer flush"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.BatchWriterFlushTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(BatchWriterFlushTest())
    return result
