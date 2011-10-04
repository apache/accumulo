from JavaTest import JavaTest
import unittest

class ServerSideErrorTest(JavaTest):
    "Verify clients throw exception when there is unexpected exception on server side"

    order = 30
    testClass="org.apache.accumulo.server.test.functional.ServerSideErrorTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(ServerSideErrorTest())
    return result
