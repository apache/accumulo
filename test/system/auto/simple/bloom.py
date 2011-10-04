from JavaTest import JavaTest
import unittest, os

class BloomFilterTest(JavaTest):
    "Test accumulo bloom filters"

    order = 22
    testClass="org.apache.accumulo.server.test.functional.BloomFilterTest"


def suite():
    os.environ['ACCUMULO_OTHER_OPTS']='-Xmx500m -Xms500m'
    result = unittest.TestSuite()
    result.addTest(BloomFilterTest())
    return result
