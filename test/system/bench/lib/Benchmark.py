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

import time

import unittest
import os
import glob
import sys

from options import log

from path import accumulo

class Benchmark(unittest.TestCase):
    
    username = ''
    password = ''
    zookeepers = ''
    instance = ''

    def __init__(self):
        unittest.TestCase.__init__(self)
        self.finished = None

    def name(self):
        return self.__class__.__name__

    def setUp(self):
        # verify accumulo is running
        self.start = time.time()

    def tearDown(self):
        self.stop = time.time()
        log.debug("Runtime: %.2f", self.stop - self.start)
        self.finished = True

    def runTime(self):
        return self.stop - self.start

    def score(self):
        if self.finished:
            return self.runTime()
        return 0.

    # Each class that extends Benchmark should overwrite this
    def setSpeed(self, speed):
        print "Classes that extend Benchmark need to override setSpeed."
        

    def setUsername(self, user):
        self.username = user

    def getUsername(self):
        return self.username
        
    def setPassword(self, password):
        self.password = password

    def getPassword(self):
        return self.password
        
    def setZookeepers(self, zookeepers):
        self.zookeepers = zookeepers

    def getZookeepers(self):
        return self.zookeepers
        
    def setInstance(self, instance):
        self.instance = instance

    def getInstance(self):
        return self.instance
        
    def sleep(self, tts):
        time.sleep(tts)
        
    def needsAuthentication(self):
        return 0
    
    def findjar(self, path):
        globjar = [ j for j in glob.glob(path) if j.find('javadoc') == -1 and j.find('sources') == -1 ]
        return globjar[0]
        
    # Returns the location of the local test jar
    def gettestjar(self):
        return self.findjar(accumulo() + '/lib/accumulo-test.jar')
    
    # Returns a string of core, thrift and zookeeper jars with a specified delim
    def getjars(self, delim=','):
        accumulo_core_jar = self.findjar(accumulo('lib', 'accumulo-core.jar'))
        accumulo_start_jar = self.findjar(accumulo('lib', 'accumulo-start.jar'))
        accumulo_fate_jar = self.findjar(accumulo('lib', 'accumulo-fate.jar'))
        accumulo_trace_jar = self.findjar(accumulo('lib', 'accumulo-trace.jar'))
        accumulo_thrift_jar = self.findjar(accumulo('lib', 'libthrift.jar'))
        accumulo_zookeeper_jar = self.findjar(os.path.join(os.getenv('ZOOKEEPER_HOME'), 'zookeeper*.jar'))
        return delim.join([accumulo_core_jar, accumulo_thrift_jar, accumulo_zookeeper_jar, accumulo_start_jar,
            accumulo_fate_jar, accumulo_trace_jar])
       
    # Builds the running command for the map/reduce class specified sans the arguments
    def buildcommand(self, classname, *args):
        return [accumulo('bin', 'accumulo'), classname, '-libjars', self.getjars()] + list(map(str, args))

