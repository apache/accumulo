import unittest

from lib import cloudshell
from lib.IngestBenchmark import IngestBenchmark

class CloudStone3(IngestBenchmark):
    "TestIngest one thousand chunky records on each slave"

    _size = 65535
    _count = 10000

    def size(self):
        return self._size

    def count(self):
        return self._count
        
    def setSpeed(self, speed):
        if speed == "fast":
            self._size = 2**10
            self._count = 1000
        elif speed == "medium":
            self._size = 2**13
            self._count = 5000            
        elif speed == "slow":
            self._size = 2**16
            self._count = 10000
        

def suite():
    result = unittest.TestSuite([
        CloudStone3(),
        ])
    return result
