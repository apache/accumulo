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

import logging
log = logging.getLogger('test.auto')

import unittest

class MasterFailover(SunnyDayTest):
    "Test master automatic master fail-over"

    order = 85

    def start_master(self, host, safeMode=None):
        goalState = 'NORMAL'
        if safeMode:
           goalState = 'SAFE_MODE'
        self.wait(self.runOn('localhost',
                             [self.accumulo_sh(),
                              'org.apache.accumulo.server.master.state.SetGoalState',
                              goalState]))
        return self.runOn(host, [self.accumulo_sh(), 'master', 'dooomed'])

    def runTest(self):
         waitTime = self.waitTime()
         self.waitForStop(self.ingester, waitTime)
         handle = self.start_master(self.masterHost())
         self.pkill(self.masterHost(), 'doomed')
         self.sleep(2)
         self.shutdown_accumulo()

def suite():
     result = unittest.TestSuite()
     result.addTest(MasterFailover())
     return result
 
