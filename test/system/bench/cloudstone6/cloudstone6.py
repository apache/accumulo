import unittest

from lib import cloudshell
from lib.CreateTablesBenchmark import CreateTablesBenchmark

class CloudStone6(CreateTablesBenchmark):
    "Creates many tables and then deletes them"
    
def suite():
    result = unittest.TestSuite([
        CloudStone6(),
        ])
    return result
