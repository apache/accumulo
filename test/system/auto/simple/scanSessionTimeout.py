from JavaTest import JavaTest
import unittest

class ScanSessionTimeOutTest(JavaTest):
    "Test a scan session that times out midway through"

    order = 20
    testClass="org.apache.accumulo.server.test.functional.ScanSessionTimeOutTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(ScanSessionTimeOutTest())
    return result
