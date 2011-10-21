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

import signal
import os
import unittest
import logging
from simple.readwrite import SunnyDayTest
from TestUtils import WALOG, ACCUMULO_HOME

log = logging.getLogger('test.auto')

class WriteAheadLog(SunnyDayTest):

     order = 25

     settings = SunnyDayTest.settings.copy()
   
     # roll the log at least once
     settings['tserver.walog.max.size'] = '2M'
     settings['gc.cycle.delay'] = 1
     settings['gc.cycle.start'] = 1

     # compact frequently
     settings['tserver.memory.maps.max'] = '200K'
     settings['tserver.compaction.major.delay'] = 1

     # split frequently
     tableSettings = SunnyDayTest.tableSettings.copy()
     tableSettings['test_ingest'] = { 
         'table.split.threshold': '750K',
         }

     def runTest(self):
          self.sleep(3)
          waitTime = self.waitTime()
          self.waitForStop(self.ingester, waitTime)
          log.info("Stopping tablet servers hard")
          self.stop_accumulo(signal.SIGKILL)
          self.sleep(5)
          self.start_accumulo()
          h = self.runOn(self.masterHost(), [self.accumulo_sh(), "gc"])
          self.sleep(3)
          log.info("Verifying Ingestion")
          self.waitForStop(self.verify(self.masterHost(),
                                       self.options.rows,
                                       size=self.options.size),
                           waitTime)
          self.shutdown_accumulo()

class DiskFailure(SunnyDayTest):

     order = 25

     settings = SunnyDayTest.settings.copy()
   
     # compact frequently
     settings['tserver.port.search'] = 'true'
     settings['tserver.memory.maps.max'] = '200K'
     settings['tserver.compaction.major.delay'] = 1
     settings['tserver.logger.timeout'] = '5s'

     def start_accumulo_procs(self, safeMode=None):
          log.info("Starting normal accumulo")
          SunnyDayTest.start_accumulo_procs(self, safeMode)
          log.info("Starting victim logger")
          libpath = '%s/test/system/auto/fake_disk_failure.so' % ACCUMULO_HOME
          os.environ['LD_PRELOAD'] = libpath
          os.environ['DYLD_INSERT_LIBRARIES'] = libpath
          os.environ['DYLD_FORCE_FLAT_NAMESPACE'] = 'true'
          stop = self.start_logger(self.masterHost())
          del os.environ['LD_PRELOAD']
          del os.environ['DYLD_FORCE_FLAT_NAMESPACE']
          del os.environ['DYLD_INSERT_LIBRARIES']
          self.flagFile = os.getenv("HOME") + "/HOLD_IO_%d" % stop.pid
          self.sleep(5)
          
     def runTest(self):
          self.sleep(3)
          waitTime = self.waitTime()
          log.info("Waiting for ingest to stop")
          self.waitForStop(self.ingester, waitTime)

          log.info("Starting fake disk failure for logger")
          fp = open(self.flagFile, "w+")
          fp.close()
          self.ingester = self.ingest(self.masterHost(),
                                      self.options.rows,
                                      self.options.rows,
                                      size=self.options.size)
          self.waitForStop(self.ingester, waitTime)
          
          log.info("Verifying Ingestion")
          self.waitForStop(self.verify(self.masterHost(),
                                       self.options.rows * 2,
                                       size=self.options.size),
                           waitTime)

     def tearDown(self):
          SunnyDayTest.tearDown(self)
          try:
               os.unlink(self.flagFile)
          except:
               pass
          

def suite():
     result = unittest.TestSuite()
     result.addTest(WriteAheadLog())
     result.addTest(DiskFailure())
     return result

