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
