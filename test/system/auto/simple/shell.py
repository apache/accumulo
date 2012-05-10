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
from TestUtils import TestUtilsMixin, ROOT, ROOT_PASSWORD, ACCUMULO_HOME

log = logging.getLogger('test.shell')
      
class ShellTest(TestUtilsMixin,unittest.TestCase):
    """Start a clean accumulo, and test different shell functions.
    Some other shell functions are tests in the systemp and tablep tests"""
    
    command_list = [ "help", "tables", "table", "createtable", "deletetable", 
                    "insert", "scan", "user", "users", "delete",
                    "flush", "config", "setiter", "deleteiter", "whoami", "debug",
                    "tablepermissions", "userpermissions", "authenticate", "createuser",
                    "dropuser", "passwd", "setauths", "getauths", "grant", "revoke" ]
    
    def setUp(self):     
        TestUtilsMixin.setUp(self)
        
    def runTest(self):
        self.setIterTest()
        self.setScanIterTest()
        self.iteratorsTest()
        self.createtableTestSplits()
        self.createtableTestCopyConfig()
        self.classpathTest()
        self.tableTest()
        self.configTest()
        self.helpTest()
        self.tablesTest()
        self.createtableTest()
        self.deletetableTest()
        self.scanTest()
        self.insertTest()
        self.flushTest()
        self.whoamiTest()
        self.getauthsTest()
        
        
    def setIterTest(self):
        input = 'setiter -t setitertest -n mymax -scan -p 10 -class org.apache.accumulo.core.iterators.user.MaxCombiner\n\ncf\n\nSTRING\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.failUnless(out.find("TableNotFoundException") >= 0,
                        "Was able to setiter a table that didn't exist")
        input = 'createtable setitertest\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'table setitertest\nsetiter -n mymax -scan -p 10 -class org.apache.accumulo.core.iterators.user.MaxCombiner\n\ncf1\n\nSTRING\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'setiter -t setitertest -n mymax -scan -p 10 -class org.apache.accumulo.core.iterators.user.MinCombiner\n\ncf2\n\nSTRING\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.failUnless(out.find("IllegalArgumentException") >= 0,
                        "Was able to configure same iter name twice")
        input = 'setiter -t setitertest -n mymin -scan -p 10 -class org.apache.accumulo.core.iterators.user.MinCombiner\n\ncf2\n\nSTRING\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.failUnless(out.find("IllegalArgumentException") >= 0,
                        "Was able to configure same priority twice")
        input = 'setiter -t setitertest -n mymin -scan -p 11 -class org.apache.accumulo.core.iterators.user.MinCombiner\n\ncf2\n\nSTRING\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'table setitertest\ninsert row1 cf1 cq 10\ninsert row1 cf1 cq 30\ninsert row1 cf1 cq 20\ninsert row1 cf2 cq 10\ninsert row1 cf2 cq 30\nscan -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failIf(out.find("row1 cf1:cq []    30") == -1 or out.find("row1 cf2:cq []    10") == -1,
                        "SetIter Failed:  combining failed")
        
    def setScanIterTest(self):
        input = 'createtable setscanitertest\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'table setscanitertest\ninsert row cf cq val1\ninsert row cf cq val2\nscan -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failIf(out.find("row cf:cq []    val1") == 1 or out.find("row cf:cq []    val2") == -1,
                        "SetScanIter Failed:  default versioning failed")
        input = 'setscaniter -t setscanitertest -n vers -p 20 -class org.apache.accumulo.core.iterators.user.VersioningIterator\n2\ntable setscanitertest\nscan -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failIf(out.find("row cf:cq []    val1") == -1 or out.find("row cf:cq []    val2") == -1,
                        "SetScanIter Failed:  versioning override failed")
        input = 'table setscanitertest\nsetscaniter -n vers -p 20 -class org.apache.accumulo.core.iterators.user.VersioningIterator\n2\nscan -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failIf(out.find("row cf:cq []    val1") == -1 or out.find("row cf:cq []    val2") == -1,
                        "SetScanIter Failed:  set on current table failed") 
        input = 'setscaniter -t setscanitertest -n vers -p 20 -class org.apache.accumulo.core.iterators.user.VersioningIterator\n2\ndeletescaniter -t setscanitertest -n vers\ntable setscanitertest\nscan -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failIf(out.find("row cf:cq []    val1") == 1 or out.find("row cf:cq []    val2") == -1,
                        "SetScanIter Failed:  deletescaniter (single) failed")
        input = 'table setscanitertest\nsetscaniter -n vers -p 20 -class org.apache.accumulo.core.iterators.user.VersioningIterator\n2\ndeletescaniter -n vers\ntable setscanitertest\nscan -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failIf(out.find("row cf:cq []    val1") == 1 or out.find("row cf:cq []    val2") == -1,
                        "SetScanIter Failed:  deletescaniter on current table failed")
        input = 'setscaniter -t setscanitertest -n vers -p 20 -class org.apache.accumulo.core.iterators.user.VersioningIterator\n2\ndeletescaniter -t setscanitertest -a\ntable setscanitertest\nscan -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failIf(out.find("row cf:cq []    val1") == 1 or out.find("row cf:cq []    val2") == -1,
                        "SetScanIter Failed:  deletescaniter (all) failed")
        input = 'setscaniter -t setscanitertest -n vers -p 20 -class org.apache.accumulo.core.iterators.user.VersioningIterator\n2\nsetscaniter -t setscanitertest -n vers -p 10 -class org.apache.accumulo.core.iterators.user.VersioningIterator\n2\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failUnless(out.find("IllegalArgumentException") >= 0,
                        "Was able to configure same iter name twice")
        input = 'setscaniter -t setscanitertest -n vers -p 20 -class org.apache.accumulo.core.iterators.user.VersioningIterator\n2\nsetscaniter -t setscanitertest -n vers2 -p 20 -class org.apache.accumulo.core.iterators.user.VersioningIterator\n2\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failUnless(out.find("IllegalArgumentException") >= 0,
                        "Was able to configure same priority twice")
        
    def classpathTest(self):
        input = 'classpath\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        lines = out.split()
        for line in lines:
            self.failUnless(line.startswith("file:") >= 0 or
                            line.startswith("List of classpath items are:") >= 0, 
                            "Classpath command: Command didn't work or classpath items were formatted incorrectly");
        
    def iteratorsTest(self):
        input = 'createtable filtertest\nsetiter -t filtertest -n myfilter -scan -p 10 -class org.apache.accumulo.core.iterators.user.AgeOffFilter\n\n4000\n\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'config -t filtertest -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failIf(out.find("table.iterator.scan.myfilter.opt.ttl") == -1, 
                        "Config Failed:  Iterator doesn't exist in the config")
        input = 'table filtertest\ninsert foo a b c\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'table filtertest\nscan\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failUnless(out.find("foo a:b") >= 0, "Scan Failed:  Entries don't exist")
        # Wait until ageoff happens
        self.sleep(5)
        input = 'table filtertest\nscan\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failUnless(out.find("foo a:b") == -1, "Scan Failed:  Entries didn't ageoff")
        input = 'deleteiter -t filtertest -n myfilter -scan\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'config filtertest -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failUnless(out.find("table.iterator.scan.myfilter.opt.ttl") == -1, 
                        "Config Failed:  Iterator doesn't exist in the config")
        input = 'table filtertest\nsetiter -n myfilter -scan -p 10 -class org.apache.accumulo.core.iterators.user.AgeOffFilter\n\n4000\n\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'config -t filtertest -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failIf(out.find("table.iterator.scan.myfilter.opt.ttl") == -1,
                        "Config Failed:  Iterator doesn't exist in the config")
        input = 'table filtertest\ndeleteiter -n myfilter -scan\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'config filtertest -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failUnless(out.find("table.iterator.scan.myfilter.opt.ttl") == -1,
                        "Config Failed:  Iterator doesn't exist in the config")
        
    def configTest(self):
        cf_option = "table.scan.max.memory"
        cf_value = "9361234"
        input = 'createtable t1\nconfig -t t1 -s %s=%s -np\n' % (cf_option, cf_value)
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'config -t t1 -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        lines = out.split("\n")
        foundConfig = False
        foundOverride = False
        for line in lines:
            if foundConfig:
                self.failUnless(line.startswith("table") and line.find("@override") >= 0 and line.find(cf_value),
                                 "Error setting or retrieving config values")
                foundOverride = True
                break
            if line.find(cf_option) >= 0:
                foundConfig = True
        self.failUnless(foundConfig and foundOverride, "Did not find the configuration that was set")
        input = 'config -t t1 -d %s -np\n' % cf_option
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'config -t t1 -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        lines = out.split("\n")
        for line in lines:
            self.failIf(line.find(cf_value) >= 0, "Could not delete the value")
        
    def helpTest(self):
        commands = self.command_list
        input = "help -np\n"
        startLooking = False
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        lines = out.split("\n")
        for line in lines:
            line = line.rstrip()
            if startLooking:
                command = line.split("-")[0].rstrip()
                if not command.startswith("\t") and command in commands:
                    commands.remove(command)
            else:
                if line[-10:] == "> help -np":
                    startLooking = True
        log.debug("missing commands:" + ", ".join(commands))
        self.failIf(len(commands) > 0, "help command doesn't cover all the commands") 
        
    def tablesTest(self):
        input = "tables\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failUnless(out.find("!METADATA"), 
                        "tables command does not return the correct tables" )
    
    def tableTest(self):
        input = "table !METADATA\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failUnless(out.split("\n")[-1].find("!METADATA >"), 
                        "table command does not switch context to the table")
        input = "table null\n"
        out2, err2, code2 = self.rootShell(self.masterHost(), input)
        self.failUnless(out2.find("TableNotFoundException") >= 0, 
                        "Was able to connect to a table that didn't exist")
        
    
    def createtableTest(self):
        input = "createtable test_table\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        input = "tables\n"
        out2, err2, code2 = self.rootShell(self.masterHost(), input)
        self.processResult(out2, err2, code2)
        self.failUnless(out2.find("test_table"), 
                        "createtable command did not correctly create the table")
        self.failUnless(out.split("\n")[-1].find("test_table >"), 
                        "createtable command did not switch contexts to the new table")
        
    def createtableTestCopyConfig(self):
        input = 'createtable cttest\nsetiter -t cttest -n myfilter -scan -p 10 -class org.apache.accumulo.core.iterators.user.AgeOffFilter\n\n2000\n\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'config -t cttest -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failIf(out.find("table.iterator.scan.myfilter.opt.ttl") == -1, 
                        "CreateTable Failed:  Iterator doesn't exist in the config")
        input = 'createtable cttest2 -cc cttest\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        input = 'config -t cttest2 -np\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        self.failIf(out.find("table.iterator.scan.myfilter.opt.ttl") == -1, 
                        "CreateTable Failed:  Iterator doesn't exist in the config after copying the table config")
        
    def createtableTestSplits(self):
        splits_file = os.path.join(ACCUMULO_HOME, 'test','system','bench','lib','splits')
        input = 'createtable splits_test -sf %s\n' % splits_file
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'table !METADATA\nscan\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        splitTestID = self.getTableId('splits_test')
        splits = []
        for a in out.split("\n"):
            if a.startswith(splitTestID+';'):
                split = a.split()[0].split(";",1)[1]
                splits.append(split)
        self.failUnless(len(splits) == 190*5, 
                        "CreateTable Failed:  Splits were not created correctly")
        input = 'createtable test_splits -cs splits_test\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'table !METADATA\nscan\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        testSplitsID = self.getTableId('test_splits')
        for a in out.split("\n"):
            if a.startswith(testSplitsID+';'):
                split = a.split()[0].split(";",1)[1]
                splits.remove(split)
        self.failUnless(len(splits) == 0, 
                        "CreateTable Failed:  Splits were not copied correctly")
        input = 'createtable test_splits_2\naddsplits one\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'table !METADATA\nscan\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        splitTestID = self.getTableId('test_splits_2')
        splits = []
        for a in out.split("\n"):
            if a.startswith(splitTestID+';'):
                split = a.split()[0].split(";",1)[1]
                splits.append(split)
        log.debug(splits)
        self.failUnless(len(splits) == 1*5 and splits[0] == 'one',
                        "CreateTable Failed:  Splits were not created correctly (add one split)")
        input = 'createtable test_splits_3\naddsplits -sf %s\n' % splits_file 
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        input = 'table !METADATA\nscan\n'
        out,err,code = self.rootShell(self.masterHost(),input)
        self.processResult(out, err, code)
        splitTestID = self.getTableId('test_splits_3')
        splits = []
        for a in out.split("\n"):
            if a.startswith(splitTestID+';'):
                split = a.split()[0].split(";",1)[1]
                splits.append(split)
        self.failUnless(len(splits) == 190*5,
                        "CreateTable Failed:  Splits were not created correctly (addsplits from file)")
    
    def deletetableTest(self):
        create = "createtable test_delete_table\n"
        out, err, code = self.rootShell(self.masterHost(), create)
        self.processResult(out, err, code)
        self.failUnless(out.split("\n")[-1].find("test_table >"), 
                        "createtable command did not switch contexts to the new table")
        delete = "deletetable -t test_delete_table\ny\n"
        out, err, code = self.rootShell(self.masterHost(), delete)
        self.processResult(out, err, code)
        input = "tables\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failIf(out.find("test_delete_table") >= 0, 
                        "deletetable command did not delete the table" )
        delete = "createtable test_delete_table1\ncreatetable test_delete_table2\ndeletetable -p test_delete_table.* -f\n"
        out, err, code = self.rootShell(self.masterHost(), delete)
        self.processResult(out, err, code)
        input = "tables\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failIf(out.find("test_delete_table") >= 0,
                        "deletetable -p command did not delete the tables" )
        delete = "createtable test_delete_table\ndeletetable\ny\n"
        out, err, code = self.rootShell(self.masterHost(), delete)
        self.processResult(out, err, code)
        input = "tables\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failIf(out.find("test_delete_table") >= 0,
                        "deletetable command did not delete the current table" )
        delete = "createtable test_delete_table\ndeletetable test_delete_table -f\n"
        out, err, code = self.rootShell(self.masterHost(), delete)
        self.processResult(out, err, code)
        input = "tables\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failIf(out.find("test_delete_table") >= 0,
                        "deletetable command did not delete the current table" )
        
    def scanTest(self):
        input = "createtable test_scan_table\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        input = "table test_scan_table\ninsert one two three four\nscan\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failUnless(out.find("one") >= 0 and out.find("two") >= 0 and 
                        out.find("three") >= 0 and out.find("four") >= 0 and
                        out.find("one") < out.find("two") < 
                        out.find("three") < out.find("four"), 
                                    "scan command did not return the correct results")
        input = "table test_scan_table\ndelete one two three\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        input = "table test_scan_table\nscan\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failIf((out.find("one") >= 0) or (out.find("two") >= 0) or (out.find("three") >= 0),
                                    "scan command did not return the correct results")
        input = "table test_scan_table\ninsert one two three four -ts 42\nscan\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failUnless(out.find("one") >= 0 and out.find("two") >= 0 and
                        out.find("three") >= 0 and out.find("four") >= 0 and
                        out.find("one") < out.find("two") <
                        out.find("three") < out.find("four"),
                                    "scan command did not return the correct results")
        input = "table test_scan_table\ndelete one two three -ts 42\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        input = "table test_scan_table\nscan\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failIf(out.find("one") >= 0 or out.find("two") >= 0 or out.find("three") >= 0,
                                    "scan command did not return the correct results")
        
    def insertTest(self):
        input = "createtable test_insert_table\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        input = "table test_insert_table\ninsert a b c d\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        
    def flushTest(self):
        input = "flush -t !METADATA -w\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failUnless(out.find("Flush of table !METADATA completed") >= 0, 
                        "flush command did not flush the tables")
        input = "flush !METADATA -w\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failUnless(out.find("Flush of table !METADATA completed") >= 0,
                        "flush command did not flush the tables")
        input = "table !METADATA\nflush -w\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failUnless(out.find("Flush of table !METADATA completed") >= 0,
                        "flush command did not flush the tables")
        
    def whoamiTest(self):
        input = "whoami\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failUnless(out.find("root") >= 0, 
                        "whoami command did not return the correct values")
    def getauthsTest(self):
        passwd = 'secret'
        input = "createuser test_user -s 12,3,4\n%s\n%s\n" % (passwd, passwd)
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        input = "getauths -u test_user\n"
        out, err, code = self.rootShell(self.masterHost(), input)
        self.processResult(out, err, code)
        self.failUnless(out.find("3") >= 0 and out.find("4") >= 0 and out.find("12") >= 0, 
                        "getauths command did not return the correct values")
        
def suite():  
    result = unittest.TestSuite()
    result.addTest(ShellTest())
    return result 
