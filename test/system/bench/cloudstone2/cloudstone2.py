import unittest

from lib import cloudshell
from lib.IngestBenchmark import IngestBenchmark

class CloudStone2(IngestBenchmark):
    "TestIngest one million small records on each slave"
    
    _size = 50
    _count = 1000000

    def size(self):
        return self._size

    def count(self):
        return self._count
    
    def setSpeed(self, speed):
        if speed == "fast":
            self._size = 50
            self._count = 10000
        elif speed == "medium":
            self._size = 50
            self._count = 100000         
        elif speed == "slow":
            self._size = 50
            self._count = 1000000

def suite():
    result = unittest.TestSuite([
        CloudStone2(),
        ])
    return result
