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
import os
import glob
import random
import time

from lib import cloudshell, runner, path
from lib.Benchmark import Benchmark
from lib.tservers import runEach, tserverNames
from lib.path import accumulo, accumuloJar
from lib.util import sleep
from lib.options import log

class TableSplitsBenchmark(Benchmark):
    "Creating a table with predefined splits and then deletes it"

    splitsfile = 'slowsplits'
    tablename = 'test_splits'

    def setUp(self): 
        # Need to generate a splits file for each speed
        code, out, err = cloudshell.run(self.username, self.password, 'table %s\n' % self.tablename)
        if out.find('does not exist') == -1:
            log.debug('Deleting table %s' % self.tablename)
            code, out, err = cloudshell.run(self.username, self.password, 'deletetable %s -f\n' % self.tablename)
            self.assertEqual(code, 0, "Could not delete table")
        Benchmark.setUp(self)

    def runTest(self):             
        command = 'createtable %s -sf %s\n' % (self.tablename, self.splitsfile)
        log.debug("Running Command %r", command)
        code, out, err = cloudshell.run(self.username, self.password, command)
        self.assertEqual(code, 0, 'Could not create table: %s' % out)
        return code, out, err

    def shortDescription(self):
        return 'Creates a table with splits. Lower score is better.'
        
    def tearDown(self):
        command = 'deletetable %s -f\n' % self.tablename
        log.debug("Running Command %r", command)
        code, out, err = cloudshell.run(self.username, self.password, command)
        self.assertEqual(code, 0, "Could not delete table")
        log.debug("Process finished")        
        Benchmark.tearDown(self)

    def setSpeed(self, speed):
        dir = os.path.dirname(os.path.realpath(__file__))
        if speed == "slow":
            splitsfile = 'slowsplits'
        elif speed == "medium":
            splitsfile = 'mediumsplits'
        else: # speed == "fast"
            splitsfile = 'fastsplits'
        self.splitsfile = os.path.join( dir, splitsfile)
        
    def needsAuthentication(self):
        return 1
        
