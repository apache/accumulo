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

from TestUtils import TestUtilsMixin, ROOT, ROOT_PASSWORD

log = logging.getLogger('test.auto')

N = 100000
COUNT = 5

class SimpleBulkTest(TestUtilsMixin, unittest.TestCase):
    "Start a clean accumulo, make some bulk data and import it"

    order = 25

    def testIngest(self, host, args, **kwargs):
        return self.runClassOn(host,
                             'org.apache.accumulo.server.test.TestIngest',
                             args,
                             **kwargs)

    def bulkLoad(self, host, dir):
        handle = self.runClassOn(
            self.masterHost(),
            'org.apache.accumulo.server.test.BulkImportDirectory',
            [ROOT, ROOT_PASSWORD,
             'test_ingest', dir, '/testBulkFail'])
        self.wait(handle)
        self.assert_(handle.returncode == 0)
        

    def createRFiles(self):
        args = '-rFile /testrf/rf%02d -timestamp 1 -size 50 -random 56 %1d %ld 1'
        log.info('creating rfiles')
        handles = []
        for i in range(COUNT):
            handles.append(self.testIngest(
                self.hosts[i%len(self.hosts)],
                (args % (i, N, (N * i))).split()))
        
        #create a rfile with one entry, there was a bug with this
        handles.append(self.testIngest(self.hosts[0], (args % (COUNT, 1, COUNT * N)).split()))
        log.info('waiting to finish')
        for h in handles:
            h.communicate()
            self.assert_(h.returncode == 0)
        log.info('done')
        
    def execute(self, host, cmd, **opts):
        handle = self.runOn(host, cmd, **opts)
        out, err = handle.communicate()
        return out, err, handle.returncode

    def runTest(self):

        # initialize the database
        self.createTable('test_ingest')
        self.execute(self.masterHost(), 'hadoop dfs -rmr /testrf'.split())
        self.execute(self.masterHost(), 'hadoop dfs -rmr /testBulkFail'.split())
        self.execute(self.masterHost(), 'hadoop dfs -mkdir /testBulkFail'.split())

        # insert some data
        self.createRFiles()
        self.bulkLoad(self.masterHost(), '/testrf')
        
        log.info("Verifying Ingestion")
        handles = []
        for i in range(COUNT):
            handles.append(self.verify(self.hosts[i%len(self.hosts)], N, i * N))
        handles.append(self.verify(self.hosts[0], 1, COUNT * N))
        for h in handles:
            out, err = h.communicate()
            self.assert_(h.returncode == 0)

        self.shutdown_accumulo()

def suite():
    result = unittest.TestSuite()
    result.addTest(SimpleBulkTest())
    return result
