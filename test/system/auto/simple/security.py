from JavaTest import JavaTest
import unittest

class SystemPermissionsTest(JavaTest):
    "Tests accumulo system permissions"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.PermissionsTest$SystemPermissionsTest"


class TablePermissionsTest(JavaTest):
    "Test accumulo table permissions"

    order = 21
    testClass="org.apache.accumulo.server.test.functional.PermissionsTest$TablePermissionsTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(SystemPermissionsTest())
    result.addTest(TablePermissionsTest())
    return result

