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

class RowHashBenchmark(Benchmark):
    "RowHashing Benchmark"

    keymin = 10
    keymax = 10
    valmin = 80
    valmax = 80
    rows = 1000000
    maxmaps = 60
    hadoop_version = ''
    input_table = 'RowHashTestInput'
    output_table = 'RowHashTestOutput'

    def setUp(self): 
        dir = os.path.dirname(os.path.realpath(__file__))
        file = os.path.join( dir, 'splits' )  
        code, out, err = cloudshell.run(self.username, self.password, 'table %s\n' % self.input_table) 
        if out.find('does not exist') == -1:
           code, out, err = cloudshell.run(self.username, self.password, 'deletetable -f %s\n' % self.input_table) 
           self.sleep(15)
        code, out, err = cloudshell.run(self.username, self.password, "createtable %s -sf %s\n" % (self.input_table, file))
        code, out, err = cloudshell.run(self.username, self.password, 'table %s\n' % self.output_table) 
        if out.find('does not exist') == -1:
            code, out, err = cloudshell.run(self.username, self.password, 'deletetable -f %s\n' %
                    self.output_table) 
            self.sleep(15)
        code, out, err = cloudshell.run(self.username, self.password, "createtable %s -sf %s\n" % (self.output_table, file))
        command = self.buildcommand('org.apache.accumulo.test.mapreduce.TeraSortIngest',
                                    '--count', self.numrows(),
                                    '-nk', self.keysizemin(),
                                    '-xk', self.keysizemax(),
                                    '-nv', self.minvaluesize(),
                                    '-xv', self.maxvaluesize(),
                                    '--table', self.input_table, 
                                    '-i', self.getInstance(),
                                    '-z', self.getZookeepers(),
                                    '-u', self.getUsername(),
                                    '-p', self.getPassword(),
                                    '--splits', self.maxmaps)
        handle = runner.start(command, stdin=subprocess.PIPE)
        log.debug("Running: %r", command)
        out, err = handle.communicate("")  
        Benchmark.setUp(self)

    def tearDown(self):
        code, out, err = cloudshell.run(self.username, self.password, "deletetable -f %s\n" % self.input_table)
        self.assertEqual(code, 0, 'Could not delete %s, %s' % (self.input_table, out))
        code, out, err = cloudshell.run(self.username, self.password, "deletetable -f %s\n" % self.output_table)
        self.assertEqual(code, 0, 'Could not delete %s, %s' % (self.output_table, out))
        Benchmark.tearDown(self)

    def tearDown(self):
        code, out, err = cloudshell.run(self.username, self.password, "deletetable %s\n" % self.input_table)
        self.assertEqual(code, 0, 'Could not delete %s, %s' % (self.input_table, out))
        code, out, err = cloudshell.run(self.username, self.password, "deletetable %s\n" % self.output_table)
        self.assertEqual(code, 0, 'Could not delete %s, %s' % (self.output_table, out))
        Benchmark.tearDown(self)

    def keysizemin(self):
        return self.keymin

    def keysizemax(self):
        return self.keymax

    def numrows(self):
        return self.rows

    def minvaluesize(self):
        return self.valmin

    def maxvaluesize(self):
        return self.valmax
        
    def runTest(self):   
        command = self.buildcommand('org.apache.accumulo.test.mapreduce.RowHash',
                                    self.getInstance(),
                                    self.getZookeepers(),
                                    self.getUsername(),
                                    self.getPassword(),
                                    self.input_table,
                                    'column:columnqual',
                                    self.output_table,
                                    self.maxmaps)
        handle = runner.start(command, stdin=subprocess.PIPE)        
        log.debug("Running: %r", command)
        out, err = handle.communicate("")
        log.debug("Process finished: %d (%s)", handle.returncode, ' '.join(handle.command))
        return handle.returncode, out, err
    
    def shortDescription(self):
        return 'Hashes %d rows from one table and outputs them into another table. '\
               'Lower score is better.' % (self.numrows())
               
    def setSpeed(self, speed):
        if speed == "slow":
            self.rows = 1000000
            self.maxmaps = 400
        elif speed == "medium":
            self.rows = 100000
            self.maxmaps = 40
        else: # if speed == "fast"
            self.rows = 10000
            self.maxmaps = 4
            
    def needsAuthentication(self):
        return 1
