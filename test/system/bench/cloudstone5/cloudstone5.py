import unittest

from lib import cloudshell
from lib.TableSplitsBenchmark import TableSplitsBenchmark

class CloudStone5(TableSplitsBenchmark):
    "Creates a table with many splits"
    
def suite():
    result = unittest.TestSuite([
        CloudStone5(),
        ])
    return result
