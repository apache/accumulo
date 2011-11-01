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

import glob
import logging
import unittest
import sleep
import signal

from TestUtils import ROOT, ROOT_PASSWORD, INSTANCE_NAME, TestUtilsMixin, ACCUMULO_HOME, ACCUMULO_DIR, ZOOKEEPERS, ID
from simple.readwrite import SunnyDayTest

log = logging.getLogger('test.auto')

class GCTest(SunnyDayTest):

    order = SunnyDayTest.order + 1

    settings = SunnyDayTest.settings.copy()
    settings.update({
        'gc.cycle.start': 5,
        'gc.cycle.delay': 15,
        'tserver.memory.maps.max':'5K',
        'tserver.compaction.major.delay': 1,
        })
    tableSettings = SunnyDayTest.tableSettings.copy()
    tableSettings['test_ingest'] = { 
        'table.split.threshold': '5K',
        }

    def fileCount(self):
        handle = self.runOn(self.masterHost(),
                            ['hadoop', 'fs', '-lsr', ACCUMULO_DIR+"/tables"])
        out, err = handle.communicate()
        return len(out.split('\n'))

    def waitForFileCountToStabilize(self):
        count = self.fileCount()
        while True:
            self.sleep(5)
            update = self.fileCount()
            if update == count:
                return count
            count = update

    def runTest(self):
        self.waitForStop(self.ingester, 60)
        self.shell(self.masterHost(), 'flush -t test_ingest')
        self.stop_gc(self.masterHost())

        count = self.waitForFileCountToStabilize()
        gc = self.runOn(self.masterHost(),
                        [self.accumulo_sh(), 'gc'])
        self.sleep(10)
        collected = self.fileCount()
        self.assert_(count > collected)

        handle = self.runOn(self.masterHost(),
                            ['grep', '-q', 'root_tablet'] +
                            glob.glob(os.path.join(ACCUMULO_HOME,'logs',ID,'gc_*')))
        out, err = handle.communicate()
        self.assert_(handle.returncode != 0)
        self.pkill(self.masterHost(), 'java.*Main gc$', signal.SIGHUP)
        self.wait(gc)
        log.info("Verifying Ingestion")
        self.waitForStop(self.verify(self.masterHost(), self.options.rows),
                         10)
        self.shutdown_accumulo()
        
class GCLotsOfCandidatesTest(TestUtilsMixin, unittest.TestCase):

    order = GCTest.order + 1
    settings = SunnyDayTest.settings.copy()
    settings.update({
        'gc.cycle.start': 5,
        'gc.cycle.delay': 15
        })

    def runTest(self):
        self.stop_gc(self.masterHost())
        log.info("Filling !METADATA table with bogus delete flags")
        prep = self.runOn(self.masterHost(),
                        [self.accumulo_sh(), 'org.apache.accumulo.server.test.GCLotsOfCandidatesTest',
                         INSTANCE_NAME,ZOOKEEPERS,ROOT,ROOT_PASSWORD])
        out, err = prep.communicate()
        self.assert_(prep.returncode == 0)

        log.info("Running GC with low memory allotment")
        gc = self.runOn('localhost',
                        ['bash', '-c', 'ACCUMULO_GC_OPTS="-Xmx7m " ' + self.accumulo_sh() + ' gc'])
        self.sleep(20)
        self.pkill('localhost', 'java.*Main gc$', signal.SIGHUP)
        self.wait(gc)

        log.info("Verifying GC ran out of memory and cycled instead of giving up")
        grep = self.runOn('localhost',
                        ['grep', '-q', 'delete candidates has exceeded'] +
                        glob.glob(os.path.join(ACCUMULO_HOME,'logs', ID, 'gc_*')))
        out, err = grep.communicate()
        self.assert_(grep.returncode == 0)

def suite():
    result = unittest.TestSuite()
    result.addTest(GCTest())
    result.addTest(GCLotsOfCandidatesTest())
    return result
