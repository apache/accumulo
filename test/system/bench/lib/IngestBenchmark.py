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
from lib.tservers import runEach, tserverNames
from lib.path import accumulo, accumuloJar
from lib.util import sleep
from lib.options import log

class IngestBenchmark(Benchmark):
    "TestIngest records on each tserver"
    
    rows = 1000000

    def setUp(self):
        code, out, err = cloudshell.run(self.username, self.password, 'table test_ingest\n')
        if out.find('does not exist') == -1:
            log.debug("Deleting table test_ingest")
            code, out, err = cloudshell.run(self.username, self.password, 'deletetable test_ingest -f\n')
            self.assertEquals(code, 0, "Could not delete the table 'test_ingest'")
        code, out, err = cloudshell.run(self.username, self.password, 'createtable test_ingest\n')
        self.assertEqual(code, 0, "Could not create the table 'test_ingest'")
        Benchmark.setUp(self)

    def tearDown(self):
        command = 'deletetable test_ingest -f\n'
        log.debug("Running Command %r", command)
        code, out, err = cloudshell.run(self.username, self.password, command)
        self.assertEqual(code, 0, "Could not delete the table 'test_ingest'")
        Benchmark.tearDown(self)

    def size(self):
        return 50

    def random(self):
        return 56

    def count(self):
        return self.rows

    def runTest(self):
        commands = {}
        for i, s in enumerate(tserverNames()):
            commands[s] = '%s %s -u %s -p %s --size %d --random %d --rows %d --start %d --cols %d' % (
                accumulo('bin', 'accumulo'),
                'org.apache.accumulo.test.TestIngest',
                self.username, self.password,
                self.size(),
                self.random(),
                self.count(),
                i*self.count(),
                1)
        results = runEach(commands)
        codes = {}
        for tserver, (code, out, err) in results.items():
            codes.setdefault(code, [])
            codes[code].append(tserver)
        for code, tservers in codes.items():
            if code != 0:
                self.assertEqual(code, 0, "Bad exit code (%d) from tservers %r" % (code, tservers))

    def score(self):
        if self.finished:
            return self.count() * self.size() / 1e6 / self.runTime()
        return 0.
    
    def shortDescription(self):
        return 'Ingest %d rows of values %d bytes on every tserver.  '\
               'Higher is better.' % (self.count(), self.size())

    def setSpeed(self, speed):
        if speed == "fast":
            self.rows = 10000
        elif speed == "medium":
            self.rows = 100000
        elif speed == "slow":
            self.rows = 1000000
        
