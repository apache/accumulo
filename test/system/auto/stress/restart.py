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

from simple.readwrite import SunnyDayTest

import unittest
import logging
import os
import signal
from TestUtils import TestUtilsMixin, ACCUMULO_HOME
from subprocess import PIPE

log = logging.getLogger('test.auto')

class RestartTest(SunnyDayTest):
    order = 80

class RestartMasterTest(RestartTest):

    def runTest(self):

        self.sleep(3)
        log.info("Stopping master server")
        self.stop_master(self.masterHost())
        self.sleep(1)
        log.info("Starting master server")
        self.start_master(self.masterHost())

        self.waitForStop(self.ingester, 30)
        self.waitForStop(self.verify(self.masterHost(), self.options.rows), 60)


class RestartMasterRecovery(RestartTest):

    settings = RestartTest.settings.copy()
    settings['instance.zookeeper.timeout'] = 5

    def runTest(self):
        self.waitForStop(self.ingester, 30)

        # start a recovery
        self.stop_accumulo()
        self.start_accumulo_procs()

        self.sleep(5)

        self.stop_master(self.masterHost())
        self.start_master(self.masterHost())

        self.waitForStop(self.verify(self.masterHost(), self.options.rows), 100)


class RestartMasterSplitterTest(RestartMasterTest):
    tableSettings = RestartMasterTest.tableSettings.copy()
    tableSettings['test_ingest'] = { 
            'table.split.threshold': '5K',
        }


class KilledTabletServerTest(RestartTest):

    def startRead(self):
        return self.verify(self.masterHost(), self.options.rows)

    def stopRead(self, handle, timeout):
        self.waitForStop(handle, timeout)

    def readRows(self):
        self.stopRead(self.startRead(), 300)

    def runTest(self):

        self.waitForStop(self.ingester, 60)
        log.info("Ingester stopped")
        log.info("starting scan")
        self.readRows()
        for host in self.hosts:
            log.info("Restarting Tablet server on %s", host)
            self.stop_tserver(host, signal.SIGKILL)
            self.start_tserver(host)
            log.info("Tablet server on %s started", host)
            log.info("starting scan")
            self.readRows()

class KilledTabletServerSplitTest(KilledTabletServerTest):
    tableSettings = KilledTabletServerTest.tableSettings.copy()
    tableSettings['test_ingest'] = { 
            'table.split.threshold': '5K',
        }

    settings = TestUtilsMixin.settings.copy()
    settings.update({
        'tserver.memory.maps.max':'5K',
        'tserver.compaction.major.delay': 1,
        'tserver.walog.max.size': '50K',
        })

    def ingest(self, host, count, start=0, timestamp=None, size=50, colf=None, **kwargs):
        return KilledTabletServerTest.ingest(self, host, count*10, start, timestamp, size, colf)

    def verify(self, host, count, start=0, size=50, timestamp=None, colf='colf'):
        return KilledTabletServerTest.verify(self, host, count*10, start, size, timestamp, colf)
            
    def runTest(self):

        for i in range(5):
            self.sleep(20)
            self.stop_tserver(self.masterHost(), signal.SIGKILL)
            self.start_tserver(self.masterHost())

        self.waitForStop(self.ingester, 600)
        log.info("Ingester stopped")
        log.info("starting scan")
        self.readRows()
        for host in self.hosts:
            log.info("Restarting Tablet server on %s", host)
            self.stop_tserver(host, signal.SIGKILL)
            self.start_tserver(host)
            log.info("Tablet server on %s started", host)
            log.info("starting scan")
            self.readRows()


class KilledTabletDuringScan(KilledTabletServerTest):
    "Kill a tablet server while we are scanning a table"

    def runTest(self):

        self.waitForStop(self.ingester, 30)
        log.info("Ingester stopped")
        handle = self.startRead()

        for host in self.hosts:
            log.info("Restarting Tablet server on %s", host)
            self.stop_tserver(host, signal.SIGKILL)
            self.start_tserver(host)
            log.info("Tablet server on %s started", host)
            log.info("starting scan")
            self.stopRead(handle, 400)
            if host != self.hosts[-1]:
                handle = self.startRead()

class KilledTabletDuringShutdown(KilledTabletServerTest):

    def runTest(self):
        self.waitForStop(self.ingester, 30)
        log.info("Ingester stopped")
        self.stop_tserver(self.hosts[0], signal.SIGKILL)
        log.info("This can take a couple minutes")
        self.shutdown_accumulo()


from simple.split import TabletShouldSplit

class ShutdownSplitter(TabletShouldSplit):
    "Shutdown while compacting, splitting, and migrating"

    tableSettings = TabletShouldSplit.tableSettings.copy()
    tableSettings['!METADATA'] = { 
            'table.split.threshold': '10K',
        }
    tableSettings['test_ingest'] = { 
            'table.split.threshold': '5K',
        }

    def runTest(self):
        self.sleep(1)
        self.shutdown_accumulo()

        # look for any exceptions
        self.wait(
            self.runOn(self.masterHost(),
                       ['grep', '-r', '-q', '" at org.apache.accumulo.core"\\\\\\|" at org.apache.accumulo.server"',
                        os.path.join(ACCUMULO_HOME,'logs') ])
            )

class RestartLoggerLate(KilledTabletServerTest):

    def runTest(self):
        self.waitForStop(self.ingester, 30)
        self.stop_tserver(self.hosts[0])
        self.stop_logger(self.hosts[0])
        self.start_tserver(self.hosts[0])
        self.sleep(15)
        self.start_logger(self.hosts[0])
        self.waitForStop(self.verify(self.masterHost(), self.options.rows), 100)
        

def suite():
    result = unittest.TestSuite()
    result.addTest(ShutdownSplitter())
    result.addTest(KilledTabletDuringShutdown())
    result.addTest(RestartMasterRecovery())
    result.addTest(KilledTabletDuringScan())
    result.addTest(RestartMasterTest())
    result.addTest(RestartMasterSplitterTest())
    result.addTest(KilledTabletServerTest())
    result.addTest(RestartLoggerLate())
    result.addTest(KilledTabletServerSplitTest())
    return result
