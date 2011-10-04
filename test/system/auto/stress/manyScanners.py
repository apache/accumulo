from JavaTest import JavaTest
import unittest

class CreateManyScannersTest(JavaTest):
    "Test creating a lot of scanners"

    order = 9999
    testClass="org.apache.accumulo.server.test.functional.CreateManyScannersTest"
    maxRuntime = 60


def suite():
    result = unittest.TestSuite()
    result.addTest(CreateManyScannersTest())
    return result
