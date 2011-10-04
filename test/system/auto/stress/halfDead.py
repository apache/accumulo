import os
import time
import signal
import unittest

from simple.readwrite import SunnyDayTest
from TestUtils import ACCUMULO_HOME

import logging
log = logging.getLogger('test.auto')

class TabletServerHangs(SunnyDayTest):

     order = 25
   
     # connections should timeout quickly for faster tests
     settings = SunnyDayTest.settings.copy()
     settings['general.rpc.timeout'] = '5s'
     settings['instance.zookeeper.timeout'] = '15s'

     def start_tserver(self, host):
         log.info("Starting tserver we can pause with bad read/writes")
         libpath = '%s/test/system/auto/fake_disk_failure.so' % ACCUMULO_HOME
         os.environ['LD_PRELOAD'] = libpath
         os.environ['DYLD_INSERT_LIBRARIES'] = libpath
         os.environ['DYLD_FORCE_FLAT_NAMESPACE'] = 'true'
         self.stop = self.runOn(self.masterHost(),
                                [self.accumulo_sh(), 'tserver'])
         del os.environ['LD_PRELOAD']
         del os.environ['DYLD_FORCE_FLAT_NAMESPACE']
         del os.environ['DYLD_INSERT_LIBRARIES']
         self.flagFile = os.getenv("HOME") + "/HOLD_IO_%d" % self.stop.pid
         log.debug("flag file is " + self.flagFile)
         return self.stop
          
     def runTest(self):
         waitTime = self.waitTime()
         log.info("Waiting for ingest to stop")
         self.waitForStop(self.ingester, waitTime)
         MANY_ROWS = 500000

         
         self.ingester = self.ingest(self.masterHost(),
                                     MANY_ROWS,
                                     size=self.options.size)
         # wait for the ingester to get going
         self.ingester.stdout.readline()
         self.ingester.stdout.readline()

         log.info("Starting faking disk failure for tserver")
         fp = open(self.flagFile, "w+")
         fp.close()

         self.sleep(10)
         log.info("Ending faking disk failure for tserver")
         os.unlink(self.flagFile)

         # look for the log message that indicates a timeout
         out, err = self.waitForStop(self.ingester, waitTime)
         self.assert_(out.find('requeuing') >= 0)

         log.info("Verifying Ingestion")
         self.waitForStop(self.verify(self.masterHost(),
                                      MANY_ROWS,
                                      size=self.options.size),
                          waitTime)
         os.kill(self.stop.pid, signal.SIGHUP)

         # look for the log message that indicates the tablet server stopped for a while
         out, err = self.stop.communicate()
         self.assert_(err.find('sleeping\nsleeping\nsleeping\n') >= 0)
          

     def tearDown(self):
         SunnyDayTest.tearDown(self)
         try:
              os.unlink(self.flagFile)
         except:
              pass

def suite():
    result = unittest.TestSuite()
    result.addTest(TabletServerHangs())
    return result
