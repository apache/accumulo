import unittest

from lib import cloudshell
from lib.RowHashBenchmark import RowHashBenchmark

class CloudStone7(RowHashBenchmark):
    "Hashes all the rows in a accumulo table and outputs them to another table"
    
def suite():
    result = unittest.TestSuite([
        CloudStone7(),
        ])
    return result
