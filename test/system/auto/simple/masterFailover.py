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
 
