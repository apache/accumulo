# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import unittest

from lib import cloudshell
from lib.IngestBenchmark import IngestBenchmark

class CloudStone3(IngestBenchmark):
    "TestIngest one thousand chunky records on each tserver"

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
