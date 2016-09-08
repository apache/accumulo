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

class CloudStone2(IngestBenchmark):
    "TestIngest one million small records on each tserver"
    
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
