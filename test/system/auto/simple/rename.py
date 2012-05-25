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

from TestUtils import TestUtilsMixin

log = logging.getLogger('test.auto')

import readwrite

class RenameTest(readwrite.SunnyDayTest):
    "Injest some data, rename the table, verify the data is still there"

    order = 28

    def runTest(self):
        self.waitForStop(self.ingester, 100)

        log.info("renaming table")
        self.shell(self.masterHost(),
                   'renametable test_ingest renamed\n'
                   'createtable test_ingest\n')
        self.waitForStop(self.ingest(self.masterHost(),
                                     self.options.rows,
                                     self.options.rows), 100)
        self.waitForStop(self.verify(self.masterHost(),
                                     self.options.rows,
                                     self.options.rows), 100)
        self.shell(self.masterHost(),
                   'deletetable test_ingest\nyes\n'
                   'renametable renamed test_ingest\n')
        self.waitForStop(self.verify(self.masterHost(), self.options.rows), 100)

def suite():
    result = unittest.TestSuite()
    result.addTest(RenameTest())
    return result

        
        
