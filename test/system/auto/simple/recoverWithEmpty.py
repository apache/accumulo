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

from TestUtils import TestUtilsMixin, ROOT, ROOT_PASSWORD, SITE_PATH

log = logging.getLogger('test.auto')

# XXX As a part of verifying lossy recovery via inserting an empty rfile,
# this test deletes test table tablets within HDFS.
# It will require write access to the backing files of the test Accumulo
# instance in HDFS.
#
# This test should read instance.dfs.dir properly from the test harness, but
# if you want to take a paranoid approach just make sure the test user
# doesn't have write access to the HDFS files of any colocated live
# Accumulo instance.
class RecoverWithEmptyTest(unittest.TestCase, TestUtilsMixin):
    "Ingest some data, verify it was stored properly, replace an underlying rfile with an empty one and verify we can scan."
    order = 95

    def add_options(self, parser):
        if not parser.has_option('-c'):
            parser.add_option('-c', '--rows', dest='rows',
                              default=20000, type=int,
                              help="The number of rows to write "
                              "when testing (%default)")
        if not parser.has_option('-n'):
            parser.add_option('-n', '--size', dest='size',
                              default=50, type=int,
                              help="The size of values to write "
                              "when testing (%default)")
    def setUp(self):
        TestUtilsMixin.setUp(self);
        # initialize the database
        self.createTable('test_ingest')
        # start test ingestion
        log.info("Starting Test Ingester")
        self.ingester = self.ingest(self.masterHost(),
                                    self.options.rows,
                                    size=self.options.size)

    def tearDown(self):
        TestUtilsMixin.tearDown(self)
        self.pkill(self.masterHost(), 'TestIngest')

    def waitTime(self):
        return 1000*120 * self.options.rows / 1e6 + 30

    def runTest(self):
        waitTime = self.waitTime()

        self.waitForStop(self.ingester, waitTime)

        log.info("Verifying Ingestion")
        self.waitForStop(self.verify(self.masterHost(),
                                     self.options.rows,
                                     size=self.options.size),
                         waitTime)
        log.info("Replacing rfile with empty")
        out,err,code = self.shell(self.masterHost(), 'flush -t test_ingest\n')
        self.processResult(out,err,code)
        out,err = self.waitForStop(self.runOn(self.masterHost(), [self.accumulo_sh(), 'shell', '-u', ROOT, '-p', ROOT_PASSWORD, '-e', 'scan -t !METADATA']), waitTime)
        self.failUnless(out.find("%s< file:/default_tablet/F0000000.rf" % self.getTableId('test_ingest')) >= 0,
                                 "Test assumptions about the rfiles backing our test table are wrong. please file a bug.")
        out,err,code = self.shell(self.masterHost(), 'offline -t test_ingest\n')
        self.processResult(out,err,code)
        import config
        rfile = "%s/tables/%s/default_tablet/F0000000.rf" % (config.parse(SITE_PATH)['instance.dfs.dir'], self.getTableId('test_ingest'))
        log.info("Removing rfile '%s'" % rfile)
        self.waitForStop(self.runOn(self.masterHost(), ['hadoop', 'fs', '-rm', rfile]), waitTime)
        self.waitForStop(self.runClassOn(self.masterHost(),
                                         "org.apache.accumulo.core.file.rfile.CreateEmpty",
                                         [rfile]),
                         waitTime)
        log.info("Make sure we can still scan")
        out,err,code = self.shell(self.masterHost(), 'online -t test_ingest\n')
        self.processResult(out,err,code);
        out,err = self.waitForStop(self.runOn(self.masterHost(), [self.accumulo_sh(), 'shell', '-u', ROOT, '-p', ROOT_PASSWORD, '-e', 'scan -t test_ingest']), waitTime)
        self.failUnless(len(out) == 0)
        self.shutdown_accumulo()

def suite():
    result = unittest.TestSuite()
    result.addTest(RecoverWithEmptyTest())
    return result
