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
import signal

from readwrite import SunnyDayTest
import TestUtils

class SessionExpired(SunnyDayTest):

    order = 25

    def signal(self, which):
        for host in self.hosts:
            self.pkill(host, ' tserver$', which)

    def runTest(self):
        # stop the tservers from talking to zookeeeper
        self.signal(signal.SIGSTOP)
        
        # timeout the session
        self.sleep(40)
        
        # turn the tservers back on so that they see the expired session
        self.signal(signal.SIGCONT)

        # wait for the tesrvers to stop (master and monitor are first and last
        # handles)
        for h in self.accumuloHandles[1:-1]:
            if 'tserver' in h.cmd:
                self.waitForStop(h, 5)
        self.cleanupAccumuloHandles()

        
def suite():
    result = unittest.TestSuite()
    result.addTest(SessionExpired())
    return result
