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

import time

from lib import cloudshell
from lib.Benchmark import Benchmark
from lib.tservers import runAll
from lib.path import accumulo

class CloudStone1(Benchmark):

    def shortDescription(self):
        return 'Test the speed at which we can check that accumulo is up '\
               'and we can reach all the tservers. Lower is better.'

    def runTest(self):
        code, out, err = cloudshell.run(self.username, self.password, 'table accumulo.metadata\nscan\n')
        self.assertEqual(code, 0, "Could not scan the metadata table. %s %s" % (out, err))
        results = runAll('echo help | %s shell -u %s -p %s' %
                         (accumulo('bin', 'accumulo'), self.username, self.password))
                         
    def setSpeed(self, speed):
        "We want to override this method but no speed can be set"

def suite():
    result = unittest.TestSuite([
        CloudStone1(),
        ])
    return result
