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
import subprocess
import hashlib
import base64
import re
import glob
import TestUtils
import socket
from TestUtils import TestUtilsMixin

log = logging.getLogger('test.auto')

def globbase(root, name):
    return glob.glob(os.path.join(root, name))[0]

def globa(name):
    return globbase(TestUtils.ACCUMULO_HOME, name)

class MapReduceTest(TestUtilsMixin,unittest.TestCase):
    """The test is used to test the functionality of a map reduce job on accumulo
       Here are the steps of this test
       1.Create a file called mapred_ftest_input with x number of lines with 1 value per line
       2.Put file on Hadoop
       3.Run Map Reduce Test that hashes the lines in the input (MD5) and puts each hash on its own row
       4.Generate Hashes on the same input in test
       5.Read table and compare hashes. Fail if they do not match
       6.Delete mapred_ftset_input from hadoop 
    """
    order = 21

    input_tablename = "mapredf"
    output_tablename = "mapredf"
    input_cfcq = "cf-HASHTYPE:cq-NOTHASHED"
    output_cfcq = "cf-HASHTYPE:cq-MD5BASE64"
    example_class_to_run ="org.apache.accumulo.examples.simple.mapreduce.RowHash"
    
    def setUp(self):
        if not os.getenv("ZOOKEEPER_HOME"):
            self.fail("ZOOKEEPER_HOME environment variable is not set please set the location of ZOOKEEPER home in this environment variable")
            return
        TestUtilsMixin.setUp(self)
        
    def tearDown(self):
        TestUtilsMixin.tearDown(self)
    
    def runTest(self):
        #These Environment variables are need to run this test it will fail if they are not in the environment
        thriftjar = globa(os.path.join('lib','libthrift*.jar'))
        examples = globa(os.path.join('lib','examples-simple*[!javadoc|sources].jar'))
        core = globa(os.path.join('lib','accumulo-core*[!javadoc|sources].jar'))
        start = globa(os.path.join('lib','accumulo-start*[!javadoc|sources].jar'))
        trace = globa(os.path.join('lib','cloudtrace*[!javadoc|sources].jar'))
        zkjar = globbase(os.getenv("ZOOKEEPER_HOME"),"zookeeper*[!javadoc|src|bin].jar")
        self.createInputTableInAccumulo();
        #Arguments for the Example Class
        arg_list = [TestUtils.INSTANCE_NAME,
                    TestUtils.ZOOKEEPERS,
                    TestUtils.ROOT,
                    TestUtils.ROOT_PASSWORD,
                    self.input_tablename,
                    self.input_cfcq,
                    self.output_tablename]
        #MapReduce class to run
        mapred_class= [self.accumulo_sh(),self.example_class_to_run]
        #classes needed to run the mapreduce
        libjars = ["-libjars",",".join([zkjar,thriftjar,examples,core,trace])]
        cmd = mapred_class+libjars+arg_list
        if(self.isAccumuloRunning()):
            log.debug("COMMAND:"+str(cmd))
            handle = self.runOn(self.masterHost(), cmd)
            out, err = handle.communicate()

            log.debug(out)
            log.debug(err)
            log.debug("Return code: "+str(handle.returncode))

            log.debug("\n\n!!!FINISHED!!!\n\n")
            if(handle.returncode==0):
                self.checkResults()
            else:
                self.fail("Test did not finish")

    def isAccumuloRunning(self):
        output = subprocess.Popen(["jps","-m"],stderr=subprocess.PIPE, stdout=subprocess.PIPE).communicate()[0]
        if(output.find("tserver")!=-1 and output.find("master")!=-1):
            return True
        return False
    
    def retrieveValues(self,tablename,cfcq):
        input = "table %s\nscan\n" % tablename
        out,err,code = self.rootShell(self.masterHost(),input)
        #print out
        restr1 = "[0-9].*\[\]    (.*)"
        restr2 = "[0-9] %s \[\]    (.*)"%(cfcq)
        val_list = re.findall(restr2,out)
        return val_list
    
    def checkResults(self):
        control_values = [base64.b64encode(hashlib.md5("row%s"%(i)).digest()) for i in range(10)]
        experiment_values = self.retrieveValues(self.output_tablename, self.output_cfcq)
        self.failIf(len(control_values) != len(experiment_values), "List aren't the same length")
        diff=[ev for ev in experiment_values if ev not in control_values]
        self.failIf(len(diff)>0, "Start and MapReduced Values aren't not the same")
    
    def fakeMRResults(self):
        vals = self.retrieveValues(self.input_tablename, self.input_cfcq)
        values = ["insert %s %s %s\n" % (i,self.output_cfcq.replace(":"," "),base64.b64encode(hashlib.md5("row%s" % i).digest())) for i in range(10,20)]
        input = "table %s\n" % (self.input_tablename,)+"".join(values)
        out,err,code = self.rootShell(self.masterHost(),input)
        #print "FAKE",out
    
    def createInputTableInAccumulo(self):
        #my leet python list comprehensions skills in action
        values = ["insert %s %s row%s\n" % (i,self.input_cfcq.replace(":"," "),i) for i in range(10)]
        input = "createtable %s\ntable %s\n" % (self.input_tablename,self.input_tablename) + \
                "".join(values)
        out,err,code = self.rootShell(self.masterHost(),input)
        #print "CREATE",out
def suite():
    result = unittest.TestSuite()
    result.addTest(MapReduceTest())
    return result
