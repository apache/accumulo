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
import sleep

from TestUtils import TestUtilsMixin, ACCUMULO_DIR

log = logging.getLogger('test.auto')

from simple.readwrite import SunnyDayTest, Interleaved
from simple.delete import DeleteTest

class ChaoticBalancerIntegrity(SunnyDayTest):
    """Start a new table, create many splits, and attempt ingest while running a crazy load balancer"""

    order = 90

    settings = TestUtilsMixin.settings.copy()
    settings.update({
      'tserver.memory.maps.max':'10K',
      'tserver.compaction.major.delay': 0,
      'table.balancer':'org.apache.accumulo.server.master.balancer.ChaoticLoadBalancer',
      })

    tableSettings = SunnyDayTest.tableSettings.copy()
    tableSettings['test_ingest'] = { 
    	'table.split.threshold': '10K',
        }
    def setUp(self):
        # ensure we have two servers
        if len(self.options.hosts) == 1:
            self.options.hosts.append('localhost')
        self.options.hosts = self.options.hosts[:2]
        
        TestUtilsMixin.setUp(self);

        # create a table with 200 splits
        import tempfile
        fileno, filename = tempfile.mkstemp()
        fp = os.fdopen(fileno, "wb")
        try:
            for i in range(200):
                fp.write("%08x\n" % (i * 1000))
        finally:
            fp.close()
        self.createTable('unused', filename)

        # create an empty table
        self.createTable('test_ingest')

    def runTest(self):

        # start test ingestion
        log.info("Starting Test Ingester")
        self.ingester = self.ingest(self.masterHost(),
                                    200000,
                                    size=self.options.size)
        self.waitForStop(self.ingester, 120)
        self.shell(self.masterHost(), 'flush -t test_ingest')
        self.waitForStop(self.verify(self.masterHost(), self.options.rows), 60)

def suite():
    result = unittest.TestSuite()
    result.addTest(ChaoticBalancerIntegrity())
    return result
