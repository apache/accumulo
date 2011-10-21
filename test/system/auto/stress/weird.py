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
        if not os.getenv("ZOOKEEPER_HOME"):
            self.fail("ZOOKEEPER_HOME environment variable is not set please set the location of ZOOKEEPER home in this environment variable")
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
