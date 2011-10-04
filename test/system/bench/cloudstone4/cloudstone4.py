import unittest

from lib import cloudshell
from lib.TeraSortBenchmark import TeraSortBenchmark

class CloudStone4(TeraSortBenchmark):
    "TestCloudIngest one terabyte of data"

def suite():
    result = unittest.TestSuite([
        CloudStone4(),
        ])
    return result
