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

# Check for ACCUMULO-2694
class BalanceInPresenceOfOfflineTable(SunnyDayTest):
    """Start a new table, create many splits, and offline before they can rebalance. Then try to have a different table balance"""

    order = 98

    settings = TestUtilsMixin.settings.copy()
    settings.update({
        'tserver.memory.maps.max':'10K',
        'tserver.compaction.major.delay': 0,
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
        out,err,code = self.shell(self.masterHost(), 'offline -t unused\n')
        self.processResult(out,err,code);

        # create an empty table
        self.createTable('test_ingest')

    def runTest(self):

        # start test ingestion
        log.info("Starting Test Ingester")
        self.ingester = self.ingest(self.masterHost(),
                                    200000,
                                    size=self.options.size)
        self.waitForStop(self.ingester, self.timeout_factor * 120)
        self.shell(self.masterHost(), 'flush -t test_ingest\n')
        self.waitForStop(self.verify(self.masterHost(), self.options.rows), 60)

        # let the server split tablets and move them around
        # Keep retrying until the wait period for migration cleanup has passed
        # which is hard coded to 5 minutes. :/
        startTime = time.clock()
        currentWait = 10
        balancingWorked = False
        while ((time.clock() - startTime) < 5*60+15):
            self.sleep(currentWait)
            # If we end up needing to sleep again, back off.
            currentWait *= 2

            # fetch the list of tablets from each server
            h = self.runOn(self.masterHost(),
                           [self.accumulo_sh(),
                            'org.apache.accumulo.test.GetMasterStats'])
            out, err = h.communicate()
            servers = {}
            server = None
            # if balanced based on ingest, the table that split due to ingest
            # will be split evenly on both servers, not just one
            table = ''
            tableId = self.getTableId('test_ingest');
            for line in out.split('\n'):
                if line.find(' Name: ') == 0:
                    server = line[7:]
                    servers.setdefault(server, 0)
                if line.find('Table: ') >= 0:
                    table = line.split(' ')[-1]
                if line.find('    Tablets: ') == 0:
                    if table == tableId:
                       servers[server] += int(line.split()[-1])
            log.info("Tablet counts " + repr(servers))

            # we have two servers
            if len(servers.values()) == 2:
              servers = servers.values()
              # a server has more than 10 splits
              if servers[0] > 10:
                # the ratio is roughly even
                ratio = min(servers) / float(max(servers))
                if ratio > 0.5:
                  balancingWorked = True
                  break
            log.debug("tablets not balanced, sleeping for %d seconds" % currentWait)

        self.assert_(balancingWorked)

def suite():
    result = unittest.TestSuite()
    result.addTest(ChaoticBalancerIntegrity())
    result.addTest(BalanceInPresenceOfOfflineTable())
    return result
