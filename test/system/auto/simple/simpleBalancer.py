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

from readwrite import SunnyDayTest, Interleaved
from delete import DeleteTest

class SimpleBalancerFairness(SunnyDayTest):
    """Start a new table and make sure that active splits
    are moved onto other servers"""

    order = 80

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

        # let the server split tablets and move them around
        self.sleep(15)

        # fetch the list of tablets from each server 
        h = self.runOn(self.masterHost(),
                       [self.accumulo_sh(),
                        'org.apache.accumulo.server.test.GetMasterStats'])
        out, err = h.communicate()
        servers = {}
        server = None
        # if balanced based on ingest, the table that split due to ingest
        # will be split evenly on both servers, not just one
        table = ''
        for line in out.split('\n'):
            if line.find(' Name ') == 0:
                server = line[6:]
                servers.setdefault(server, 0)
            if line.find('Table ') >= 0:
                table = line.split(' ')[-1]
            if line.find('    Tablets ') == 0:
                if table == '1':
                   servers[server] += int(line.split()[-1])
        log.info("Tablet counts " + repr(servers))

        # we have two servers
        self.assert_(len(servers.values()) == 2)
        servers = servers.values()

        # a server has more than 10 splits
        self.assert_(servers[0] > 10)

        # the ratio is roughly even
        ratio = min(servers) / float(max(servers))
        self.assert_(ratio > 0.5)
        
def suite():
    result = unittest.TestSuite()
    result.addTest(SimpleBalancerFairness())
    return result
