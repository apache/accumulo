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

import subprocess

from lib import cloudshell, runner, path
from lib.Benchmark import Benchmark
from lib.tservers import runEach, tserverNames
from lib.path import accumulo, accumuloJar
from lib.util import sleep
from lib.options import log

class CreateTablesBenchmark(Benchmark):
    "Creating and deleting tables"

    tables = 1000

    def setUp(self): 
        for x in range(1, self.tables):
            currentTable = 'test_ingest%d' % (x)      
            log.debug("Checking for table existence: %s" % currentTable)
            code, out, err = cloudshell.run(self.username, self.password, 'table %s\n' % currentTable)
            if out.find('does not exist') == -1:
                command = 'deletetable -f %s\n' % (currentTable)
                log.debug("Running Command %r", command)
                code, out, err = cloudshell.run(self.username, self.password, command)
                self.assertEqual(code, 0, 'Did not successfully delete table: %s' % currentTable)
        Benchmark.setUp(self)  

    def runTest(self):
        for x in range(1, self.tables):
            currentTable = 'test_ingest%d' % (x)      
            command = 'createtable %s\n' % (currentTable)
            log.debug("Running Command %r", command)
            code, out, err = cloudshell.run(self.username, self.password, command)
            self.assertEqual(code, 0, 'Did not successfully create table: %s' % currentTable)
            # print err
        for x in range(1, self.tables):
            currentTable = 'test_ingest%d' % (x)      
            command = 'deletetable -f %s\n' % (currentTable)
            log.debug("Running Command %r", command)
            code, out, err = cloudshell.run(self.username, self.password, command)
            self.assertEqual(code, 0, 'Did not successfully delete table: %s' % currentTable)
            # print err
        log.debug("Process finished")
        return code, out, err
            
    def numTables(self):
        return self.tables
    
    def shortDescription(self):
        return 'Creates %d tables and then deletes them. '\
               'Lower score is better.' % (self.numTables())
               
    def setSpeed(self, speed):
        if speed == "slow":
            self.tables = 50
        elif speed == "medium":
            self.tables = 10
        elif speed == "fast":
            self.tables = 5
            
    def needsAuthentication(self):
        return 1
