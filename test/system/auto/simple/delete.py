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

class DeleteTest(readwrite.SunnyDayTest):
    "Injest some data, delete it, re-injest it, look for it"

    order = 25

    def setUp(self):
        TestUtilsMixin.setUp(self);
        
        # initialize the database
        self.createTable('test_ingest')
        
        # start test ingestion
        log.info("Starting Test Ingester")
        self.ingester = self.ingest(self.masterHost(),
                                    self.options.rows,
                                    timestamp=time.time() * 1000)

    def runTest(self):

        self.waitForStop(self.ingester, 200)

        log.info("Deleting data")
        self.waitForStop(self.runClassOn(self.masterHost(), "org.apache.accumulo.server.test.TestRandomDeletes", []),
                         400)

        log.info("Inserting data")
        now = time.time() * 1000
        self.waitForStop(self.ingest(self.masterHost(),
                                     self.options.rows,
                                     timestamp=now),
                         60)

        log.info("Verifying Ingestion")
        self.waitForStop(self.verify(self.masterHost(),
                                     self.options.rows,
                                     timestamp=now),
                         60)
        self.shutdown_accumulo()
        

def suite():
    result = unittest.TestSuite()
    result.addTest(DeleteTest())
    return result
