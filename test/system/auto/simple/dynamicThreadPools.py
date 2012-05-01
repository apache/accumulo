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
import sys

from simple.readwrite import SunnyDayTest
from TestUtils import TestUtilsMixin

log = logging.getLogger('test.auto')

class DynamicThreadPools(SunnyDayTest):
    'Verify we can change thread pool sizes after the servers have run'
    order = 50

    settings = SunnyDayTest.settings.copy()
    settings.update({
        'tserver.compaction.major.delay': 1,
        })

    def setUp(self):
        TestUtilsMixin.setUp(self);

        # initialize the database
        self.createTable('test_ingest')

        # start test ingestion
        log.info("Starting Test Ingester")
        self.ingester = self.ingest(self.masterHost(),
                                    self.options.rows*10,
                                    size=self.options.size)

    def runTest(self):
        self.waitForStop(self.ingester, self.waitTime())
        # make a bunch of work for compaction
        self.shell(self.masterHost(), 
		   'clonetable test_ingest test_ingest1\n'
		   'clonetable test_ingest test_ingest2\n'
 		   'clonetable test_ingest test_ingest3\n'
 		   'clonetable test_ingest test_ingest4\n'
 		   'clonetable test_ingest test_ingest5\n'
 		   'clonetable test_ingest test_ingest6\n'
                   'config -s tserver.compaction.major.concurrent.max=1\n'
		   'sleep 10\n'
		   'compact -p .*\n'
		   'sleep 2\n')
	handle = self.runOn(self.masterHost(), [self.accumulo_sh(), 'org.apache.accumulo.server.test.GetMasterStats'])
        out, err = self.waitForStop(handle, 120)
        count = 0
        for line in out.split('\n'):
	   if line.find('Major Compacting') >= 0:
		count += int(line.split()[2])
        self.assert_(count == 1)

def suite():
    result = unittest.TestSuite()
    result.addTest(DynamicThreadPools())
    return result
