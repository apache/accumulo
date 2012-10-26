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
import sys
import shutil

import logging
import unittest
import time

from TestUtils import TestUtilsMixin, ACCUMULO_HOME

log = logging.getLogger('test.auto')

class CombinerTest(TestUtilsMixin, unittest.TestCase):
    "Start a clean accumulo, use a Combiner, verify the data is aggregated"

    order = 25

    def checkSum(self):
        # check the scan
        out, err, code = self.shell(self.masterHost(),"table test\nscan\n")
        self.assert_(code == 0)
        for line in out.split('\n'):
            if line.find('row1') == 0:
                self.assert_(int(line.split()[-1]) == sum(range(10)))
                break
        else:
            self.fail("Unable to find needed output in %r" % out)
        
    def runTest(self):

        # initialize the database
        out, err, code = self.rootShell(self.masterHost(),"createtable test\n"
                     "setiter -t test -scan -p 10 -n mycombiner -class org.apache.accumulo.core.iterators.user.SummingCombiner\n"
                     "\n"
                     "cf\n"
                     "\n"
                     "STRING\n"
                     "deleteiter -t test -n vers -minc -majc -scan\n")
        self.assert_(code == 0)

        # insert some rows
        log.info("Starting Test Ingester")
        cmd = 'table test\n';
        for i in range(10):
            cmd += 'insert row1 cf col1 %d\n' % i
        out, err, code = self.rootShell(self.masterHost(), cmd)
        self.assert_(code == 0)
        self.checkSum()
        out, err, code = self.rootShell(self.masterHost(), "flush -t test -w\n")
        self.assert_(code == 0)
        self.checkSum()
        self.shutdown_accumulo()
        self.start_accumulo()
        self.checkSum()

class ClassLoaderTest(TestUtilsMixin, unittest.TestCase):
    "Start a clean accumulo, ingest one data, read it, set a combiner, read it again, change the combiner jar, read it again" 
    order = 26

    def checkSum(self, val):
        # check the scan
        out, err, code = self.shell(self.masterHost(), "table test\nscan\n")
        self.assert_(code == 0)
        for line in out.split('\n'):
            if line.find('row1') == 0:
                self.assert_(line.split()[-1] == val)
                break
        else:
            self.fail("Unable to find needed output in %r" % out)

    def runTest(self):
        jarPath = ACCUMULO_HOME+"/lib/ext/TestCombiner.jar"
        # make sure the combiner is not there
        if os.path.exists(jarPath):
            os.remove(jarPath)
        # initialize the database
        out, err, code = self.rootShell(self.masterHost(), "createtable test\n")
        self.assert_(code == 0)

        # insert some rows
        log.info("Starting Test Ingester")
        
        out, err, code = self.rootShell(self.masterHost(), "table test\ninsert row1 cf col1 Test\n")
        self.assert_(code == 0)
        self.checkSum("Test")
        
        shutil.copy(sys.path[0]+"/TestCombinerX.jar", jarPath)
	time.sleep(1)
        out, err, code = self.rootShell(self.masterHost(), "setiter -t test -scan -p 10 -n TestCombiner -class org.apache.accumulo.server.test.functional.TestCombiner\n"
                     "\n"
                     "cf\n")
        self.assert_(code == 0)
        self.checkSum("TestX")
        
        shutil.copy(sys.path[0]+"/TestCombinerY.jar", jarPath)
        time.sleep(1)
        self.checkSum("TestY")
        
        os.remove(jarPath)
        

def suite():
    result = unittest.TestSuite()
    result.addTest(CombinerTest())
    result.addTest(ClassLoaderTest())
    return result
