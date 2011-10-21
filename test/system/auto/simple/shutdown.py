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
import sleep
import signal
from subprocess import PIPE

from TestUtils import TestUtilsMixin, ROOT, ROOT_PASSWORD
from simple.readwrite import SunnyDayTest

log = logging.getLogger('test.auto')

class ShutdownDuringIngest(SunnyDayTest):

    order = SunnyDayTest.order + 1

    def runTest(self):
        self.shutdown_accumulo()
        

class ShutdownDuringQuery(SunnyDayTest):

    order = SunnyDayTest.order + 1

    def runTest(self):
        self.waitForStop(self.ingester, self.waitTime())

        log.info("Verifying Ingestion")
        for i in range(10):
            h = self.verify(self.masterHost(),
                            self.options.rows,
                            size=self.options.size)
        self.shutdown_accumulo()

class ShutdownDuringDelete(SunnyDayTest):
    
    order = SunnyDayTest.order + 1

    def runTest(self):
        self.waitForStop(self.ingester, self.waitTime())
        h = self.runClassOn(self.masterHost(), "org.apache.accumulo.server.test.TestRandomDeletes", [])
        self.shutdown_accumulo()


class ShutdownDuringDeleteTable(TestUtilsMixin, unittest.TestCase):
    
    order = SunnyDayTest.order + 1

    def runTest(self):
        ct = ''
        dt = ''
        for i in range(10):
            ct += 'createtable test%02d\n' % i
            dt += 'deletetable test%02d\n' % i
        out, err, code = self.shell(self.masterHost(), ct)
        handle = self.runOn(self.masterHost(),
                            [self.accumulo_sh(),
                             'shell', '-u', ROOT, '-p', ROOT_PASSWORD],
                            stdin=PIPE)
        handle.stdin.write(dt)
        self.shutdown_accumulo()

class ShutdownDuringStart(TestUtilsMixin, unittest.TestCase):

    order = SunnyDayTest.order + 1
    
    def runTest(self):
        self.hosts = self.options.hosts
        self.clean_accumulo(self.masterHost())
        self.start_accumulo()
        self.shutdown_accumulo()

def suite():
    result = unittest.TestSuite()
    result.addTest(ShutdownDuringIngest())
    result.addTest(ShutdownDuringQuery())
    result.addTest(ShutdownDuringDelete())
    result.addTest(ShutdownDuringDeleteTable())
    result.addTest(ShutdownDuringStart())
    return result
