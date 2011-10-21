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

import os

import logging
import unittest
import time
from subprocess import PIPE

from TestUtils import TestUtilsMixin

log = logging.getLogger('test.auto')

N = 10000    # rows to insert, 100 rows per tablet
WAIT = (N / 1000. + 1) * 60

class LotsOfTablets(TestUtilsMixin, unittest.TestCase):

    order = 80

    settings = TestUtilsMixin.settings.copy()
    settings.update({
    	'table.split.threshold':200,
        'tserver.memory.maps.max':'128M'
        })

    def runTest(self):

        # initialize the database
        handle = self.runClassOn(self.masterHost(), 
		               'org.apache.accumulo.server.test.CreateTestTable', 
		               [str(N)])
	self.waitForStop(handle, WAIT)
        handle = self.runClassOn(self.masterHost(), 
		               'org.apache.accumulo.server.test.CreateTestTable', 
		               ['-readonly', str(N)])
        self.waitForStop(handle, WAIT)

def suite():
    result = unittest.TestSuite()
    result.addTest(LotsOfTablets())
    return result
