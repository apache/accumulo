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


import unittest

from lib import cloudshell
from lib.Benchmark import Benchmark
from lib.slaves import runEach, slaveNames
from lib.path import accumulo, accumuloJar
from lib.util import sleep
from lib.options import log

class IngestBenchmark(Benchmark):
    "TestIngest records on each slave"
    
    rows = 1000000

    def setUp(self):
        code, out, err = cloudshell.run(self.username, self.password, 'table test_ingest\n')
        if out.find('no such table') >= 0:
            log.debug("Deleting table test_ingest")
            code, out, err = cloudshell.run(self.username, self.password, 'deletetable test_ingest\n')
            self.sleep(10)
        code, out, err = cloudshell.run(self.username, self.password, 'createtable test_ingest\n')
        self.assertEqual(code, 0)
        Benchmark.setUp(self)

    def size(self):
        return 50

    def random(self):
        return 56

    def count(self):
        return self.rows

    def runTest(self):
        commands = {}
        for i, s in enumerate(slaveNames()):
            commands[s] = '%s %s -username %s -password %s -size %d -random %d %d %d %d' % (
                accumulo('bin', 'accumulo'),
                'org.apache.accumulo.server.test.TestIngest',
                self.username, self.password,
                self.size(),
                self.random(),
                self.count(),
                i*self.count(),
                1)
        results = runEach(commands)
        codes = {}
        for slave, (code, out, err) in results.items():
            codes.setdefault(code, [])
            codes[code].append(slave)
        for code, slaves in codes.items():
            if code != 0:
                self.assertEqual(code, 0, "Bad exit code (%d) from slaves %r" % (code, slaves))
        command = 'deletetable test_ingest\n'
        log.debug("Running Command %r", command)
        code, out, err = cloudshell.run(self.username, self.password, command)
        # print err

    def score(self):
        if self.finished:
            return self.count() * self.size() / 1e6 / self.runTime()
        return 0.
    
    def shortDescription(self):
        return 'Ingest %d rows of values %d bytes on every slave.  '\
               'Higher is better.' % (self.count(), self.size())

    def setSpeed(self, speed):
        if speed == "fast":
            self.rows = 10000
        elif speed == "medium":
            self.rows = 100000
        elif speed == "slow":
            self.rows = 1000000
        
