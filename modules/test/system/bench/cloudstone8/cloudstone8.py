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
from lib.TeraSortBenchmark import TeraSortBenchmark

class CloudStone8(TeraSortBenchmark):
    "Tests variable length input keys and values"
    
    keymin = 10
    keymax = 50
    valmin = 100
    valmax = 500
    rows = 1000000
    tablename = 'VariableLengthIngestTable'
    
    
    def shortDescription(self):
        return 'Ingests %d rows of variable key and value length to be sorted. '\
               'Lower score is better.' % (self.numrows())
    
    def setSpeed(self, speed):
        if speed == "slow":
            self.rows = 1000000
            self.keymin = 60
            self.keymax = 100
            self.valmin = 200
            self.valmax = 300
            self.numsplits = 400
        elif speed == "medium":
            self.rows = 100000
            self.keymin = 40
            self.keymax = 70
            self.valmin = 130
            self.valmax = 170
            self.numsplits = 40
        elif speed == "fast":
            self.rows = 10000 
            self.keymin = 30
            self.keymax = 50
            self.valmin = 80
            self.valmax = 100 
            self.numsplits = 4

def suite():
    result = unittest.TestSuite([
        CloudStone8(),
        ])
    return result
