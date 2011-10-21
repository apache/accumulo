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

from TestUtils import TestUtilsMixin, FUZZ, ACCUMULO_HOME, SITE

log = logging.getLogger('test.auto')

class SunnyDayTest(unittest.TestCase, TestUtilsMixin):
    "Start a clean accumulo, ingest some data, verify it was stored properly"
    order = 20

    def add_options(self, parser):
        if not parser.has_option('-c'):
            parser.add_option('-c', '--rows', dest='rows',
                              default=20000, type=int,
                              help="The number of rows to write "
                              "when testing (%default)")
        if not parser.has_option('-n'):
            parser.add_option('-n', '--size', dest='size',
                              default=50, type=int,
                              help="The size of values to write "
                              "when testing (%default)")
    def setUp(self):
        TestUtilsMixin.setUp(self);
        
        # initialize the database
        self.createTable('test_ingest')
        
        # start test ingestion
        log.info("Starting Test Ingester")
        self.ingester = self.ingest(self.masterHost(),
                                    self.options.rows,
                                    size=self.options.size)

    def tearDown(self):
        TestUtilsMixin.tearDown(self)
        self.pkill(self.masterHost(), 'TestIngest')

    def waitTime(self):
        return 1000*120 * self.options.rows / 1e6 + 30

    def runTest(self):
        waitTime = self.waitTime()

        self.waitForStop(self.ingester, waitTime)

        log.info("Verifying Ingestion")
        self.waitForStop(self.verify(self.masterHost(),
                                     self.options.rows,
                                     size=self.options.size),
                         waitTime)

        log.info("Hitting the web pages")
        import urllib2
        handle = urllib2.urlopen('http://%s:%d/monitor' % (self.masterHost(), 50099))
        self.assert_(len(handle.read()) > 100)
        
        self.shutdown_accumulo()

class MultiTableTest(SunnyDayTest):

    order = 21
    
    def ingest(self, host, count, *args, **kwargs):
        klass = 'org.apache.accumulo.server.test.TestMultiTableIngest'
        args = '-count %d ' % count
        return self.runClassOn(host, klass, args.split())

    def verify(self, host, count, *args, **kwargs):
        klass = 'org.apache.accumulo.server.test.TestMultiTableIngest'
        args = '-count %d -readonly ' % count
        return self.runClassOn(host, klass, args.split())


class LargeTest(SunnyDayTest):
    
    order = 21

    def ingest(self, host, count, start=0, timestamp=None, size=50, **kwargs):
        if sys.platform != "darwin":
            return SunnyDayTest.ingest(self, host, count / 1000, start, timestamp, size * 100000, **kwargs)
        else:
            return SunnyDayTest.ingest(self, host, count / 1000, start, timestamp, size, **kwargs)
    
    def verify(self, host, count, start=0, size=50, timestamp=None):
        if sys.platform != "darwin":
            return SunnyDayTest.verify(self, host, count / 1000, start, size * 100000, timestamp)
        else:
            return SunnyDayTest.verify(self, host, count / 1000, start, size, timestamp)


class Interleaved(SunnyDayTest):

    order = 21

    def setUp(self):
        TestUtilsMixin.setUp(self);
        
        # initialize the database
        self.createTable('test_ingest')

    def tearDown(self):
        TestUtilsMixin.tearDown(self)
        self.pkill(self.masterHost(), 'TestIngest')


    def runTest(self):
        waitTime = self.waitTime()

        N = self.options.rows
        ingester = self.ingest(self.masterHost(), N, 0)
        for i in range(0, 10*N, N):
            self.waitForStop(ingester, waitTime)
            verifier = self.verify(self.hosts[-1], N, i)
            ingester = self.ingest(self.masterHost(), N, i + N)
            self.waitForStop(verifier, waitTime)
        verifier = self.verify(self.hosts[-1], N, i)
        self.waitForStop(verifier, waitTime)

        self.shutdown_accumulo()


class SunnyLG(SunnyDayTest):
    "Start a clean accumulo, ingest some data, verify it was stored properly"

    order=20

    tableSettings = SunnyDayTest.tableSettings.copy()
    tableSettings['test_ingest'] = {
        'table.group.g1':'colf',
        'table.groups.enabled':'g1',
        }
    def runTest(self):
        SunnyDayTest.runTest(self)
        cfg = os.path.join(ACCUMULO_HOME, 'conf', SITE)
        import config
        dir = config.parse(cfg)['instance.dfs.dir']
        handle = self.runOn(self.masterHost(),
                            [self.accumulo_sh(),
                             'org.apache.accumulo.core.file.rfile.PrintInfo',
                             dir + '/tables/1/default_tablet/F0000000.rf'])
        out, err = handle.communicate()
        self.assert_(handle.returncode == 0)
        self.assert_(out.find('Locality group         : g1') >= 0)
        self.assert_(out.find('families      : [colf]') >= 0)


class LocalityGroupPerf(SunnyDayTest):
    "Start a clean accumulo, ingest some data, verify it was stored properly"

    order=21

    tableSettings = SunnyDayTest.tableSettings.copy()
    tableSettings['test_ingest'] = {
        'table.group.g1':'colf',
        'table.groups.enabled':'g1',
        }
            
    def setUp(self):
        TestUtilsMixin.setUp(self);
        
        # initialize the database
        self.createTable('test_ingest')
        
        # start test ingestion
        log.info("Starting Test Ingester")
        self.ingester = self.ingest(self.masterHost(),
                                    self.options.rows,
                                    size=self.options.size)
    def runTest(self):
        waitTime = self.waitTime()
        self.waitForStop(self.ingester, waitTime)
        self.shell(self.masterHost(),'flush -t test_ingest\n')
        t = time.time()
        out, err, code = self.shell(self.masterHost(),
                                    'table test_ingest\n'
                                    'insert zzzzzzzzzz colf2 cq value\n'
                                    'scan -c colf\n')
        t2 = time.time()
        diff = t2 - t
        out, err, code = self.shell(self.masterHost(),
                                    'table test_ingest\n'
                                    'scan -c colf2\n')
        diff2 = time.time() - t2
        self.assert_(diff2 < diff, "skipping over locality group was slower than a full scan")
        

class LocalityGroupChange(unittest.TestCase, TestUtilsMixin):
    "Start a clean accumulo, ingest some data with different column familys"

    order = 25

    def setUp(self):
        TestUtilsMixin.setUp(self);
        
        # initialize the database
        self.createTable('test_ingest')

    def flush(self):
        out, err, code = self.shell(self.masterHost(), 'flush -t test_ingest\n')
        self.sleep(5)
        
    def runTest(self):
        waitTime = 10

        # start test ingestion
        log.info("Starting Test Ingester")
        N = 10000
        config = [ ({'lg1':('colf',)}, 'lg1'), 
                   ({'lg1':('colf',)}, ''), 
                   ({'lg1':('colf','xyz')}, 'lg1'),
                   ({'lg1':('colf','xyz'), 'lg2':('c1','c2')}, 'lg1,lg2'),]
        i = 0
        for cfg in config:
            cmds = ''
            for group in cfg[0]:
                cmds += 'config -t test_ingest -s table.group.%s=%s\n' % (group, ','.join(cfg[0][group]))
            cmds += 'config -t test_ingest -s table.groups.enabled=%s\n' % cfg[1]
            self.shell(self.masterHost(), cmds)
            self.waitForStop(self.ingest(self.masterHost(), N * (i+1), N*i), waitTime)
            self.flush()
            self.waitForStop(self.verify(self.masterHost(), 0, N * (i+1)), waitTime)
            i += 1

        self.shell(self.masterHost(), 'deletetable test_ingest\ncreatetable test_ingest\n')
        config = [ ({'lg1':('colf',)}, 'lg1'), 
                   ({'lg1':('colf',)}, ''), 
                   ({'lg1':('colf','xyz')}, 'lg1'),
                   ({'lg1':('colf',), 'lg2':('xyz',)}, 'lg1,lg2'),]

        i = 1
        for cfg in config:
            self.waitForStop(self.ingest(self.masterHost(), N*i, 0), waitTime)
            cmds = ''
            for group in cfg[0]:
                cmds += 'config -t test_ingest -s table.group.%s=%s\n' % (group, ','.join(cfg[0][group]))
            cmds += 'config -t test_ingest -s table.groups.enabled=%s\n' % cfg[1]
            self.waitForStop(self.ingest(self.masterHost(), N*i, 0, colf='xyz'), waitTime)
            self.shell(self.masterHost(), cmds)
            self.flush()
            self.waitForStop(self.verify(self.masterHost(), N*i, 0), waitTime)
            self.waitForStop(self.verify(self.masterHost(), N*i, 0, colf='xyz'), waitTime)
            i += 1
            
        self.shutdown_accumulo()


def suite():
    result = unittest.TestSuite()
    result.addTest(SunnyDayTest())
    result.addTest(Interleaved())
    result.addTest(LargeTest())
    result.addTest(MultiTableTest())
    result.addTest(SunnyLG())
    result.addTest(LocalityGroupPerf())
    result.addTest(LocalityGroupChange())
    return result
