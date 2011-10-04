import os
import logging
import unittest
import time
import sys
import signal
import socket

from TestUtils import ACCUMULO_HOME, SITE, FUZZ

from simple.readwrite import SunnyDayTest

log = logging.getLogger('test.auto')

class LateLastContact(SunnyDayTest):
    """Fake the "tablet stops talking but holds its lock" problem we see when hard drives and NFS fail.
    Start a tabletserver, create a fake lock, start the master, kill the tablet server.
    """

    order = 80

    settings = SunnyDayTest.settings.copy()
    
    def runTest(self):
        self.waitForStop(self.ingester, self.waitTime())
        cfg = os.path.join(ACCUMULO_HOME, 'conf', SITE)
        import config
        dir = config.parse(cfg)['instance.dfs.dir']

        handle = self.runOn(self.masterHost(),
                            ["hadoop","dfs","-ls", dir +"/instance_id"])
        out, err = handle.communicate()
        out = out.strip()
        instance_id = out.split("\n")[-1].split("/")[-1]
        zkcli = os.path.join(os.getenv("ZOOKEEPER_HOME"), "bin", "zkCli.sh")
        if not os.path.exists(zkcli):
            zkcli = "/usr/share/zookeeper/bin/zkCli.sh"
        myaddr = socket.getaddrinfo(socket.gethostname(), None)[0][-1][0]
        self.wait(self.runOn(self.masterHost(),
                             [zkcli, "-server", "localhost",
                              "create", "-s",
                              "/accumulo/%s/tservers/%s:%s/zlock-0" %
                              (instance_id, myaddr, 42000),
                              "tserver", "world:anyone:cdrw"]))
        self.stop_tserver(self.masterHost(), signal=signal.SIGKILL)
        log.info("Making sure the tablet server is still considered online")
        handle = self.runClassOn(self.masterHost(), 'org.apache.accumulo.server.test.GetMasterStats', [])
        out, err = handle.communicate()
        tServer = "%s:%s" % (myaddr, 39000 + FUZZ)
        assert out.find(tServer) > 0
        self.sleep(12)
        log.info("Making sure the tablet server is now offline")
        handle = self.runClassOn(self.masterHost(), 'org.apache.accumulo.server.test.GetMasterStats', [])
        out, err = handle.communicate()
        assert (out.find(tServer) < 0) or (out.find(tServer) > out.find('Bad servers'))
        
        
def suite():
    result = unittest.TestSuite()
    result.addTest(LateLastContact())
    return result
