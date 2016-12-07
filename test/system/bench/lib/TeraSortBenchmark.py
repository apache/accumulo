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
from lib.util import sleep
from lib.options import log

class TeraSortBenchmark(Benchmark):
    "TeraSort in the cloud"

    keymin = 10
    keymax = 10
    valmin = 78
    valmax = 78
    rows = 10000000000
    numsplits = 400
    # Change this number to modify how the jobs are run on hadoop
    rows_per_split = 250000
    hadoop_version = ''
    tablename = 'CloudIngestTest'


    def setUp(self): 
        code, out, err = cloudshell.run(self.username, self.password, 'table %s\n' % self.tablename)
        if out.find('does not exist') == -1:
            log.debug('Deleting table %s' % self.tablename)
            code, out, err = cloudshell.run(self.username, self.password, 'deletetable -f %s\n' % self.tablename)
        Benchmark.setUp(self)

    def tearDown(self):
        code, out, err = cloudshell.run(self.username, self.password, "deletetable -f %s\n" % self.tablename)
        self.assertEqual(code, 0, 'Could not delete %s, %s' % (self.tablename, out))
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
        dir = os.path.dirname(os.path.realpath(__file__))
        file = os.path.join( dir, 'splits' )
        code, out, err = cloudshell.run(self.username, self.password, "createtable %s -sf %s\n" % (self.tablename, file))
        command = self.buildcommand('org.apache.accumulo.test.mapreduce.TeraSortIngest',
                                    '--count', self.numrows(),
                                    '-nk', self.keysizemin(),
                                    '-xk', self.keysizemax(),
                                    '-nv', self.minvaluesize(),
                                    '-xv', self.maxvaluesize(),
                                    '-t', self.tablename,
                                    '-i', self.instance,
                                    '-z', self.zookeepers,
                                    '-u', self.username,
                                    '-p', self.password,
                                    '--splits', self.numsplits)
        handle = runner.start(command, stdin=subprocess.PIPE)
        log.debug("Running: %r", command)
        out, err = handle.communicate("")
        log.debug("Process finished: %d (%s)", handle.returncode, ' '.join(handle.command))
        self.assertEqual(handle.returncode, 0, "Job did not complete successfully")
        return handle.returncode, out, err
        
    def needsAuthentication(self):
        return 1
    
    def shortDescription(self):
        return 'Ingests %d rows (to be sorted). '\
               'Lower score is better.' % (self.numrows())
               
    def setSpeed(self, speed):
        if speed == "slow":
            self.rows = 10000000000            
            self.numsplits = 400
        elif speed == "medium":
            self.rows = 10000000
            self.numsplits = 40
        elif speed == "fast":
            self.rows = 10000 
            self.numsplits = 4
