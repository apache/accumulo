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
import glob

from TestUtils import TestUtilsMixin, ACCUMULO_HOME, SITE, ROOT, ROOT_PASSWORD, INSTANCE_NAME, ZOOKEEPERS

table='testTable'
count=str(10000)
min=str(0)
max=str(99999)
valueSize=str(100)
memory=str(1<<20)
latency=str(1000)
numThreads=str(4)
visibility='A|B'
auths='A,B'

log = logging.getLogger('test.auto')

class Examples(TestUtilsMixin, unittest.TestCase):
    "Start a clean accumulo, run the examples"
    order = 21

    def runExample(self, cmd):
        return self.wait(self.runOn(self.masterHost(), [self.accumulo_sh(),] + cmd))

    def ashell(self, input):
        out, err, code = self.shell(self.masterHost(), input + '\n')
        self.assert_(code == 0)
        return out

    def comment(self, description):
        LINE = '-'*40
        log.info(LINE)
        log.info(description)
        log.info(LINE)

    def execute(self, *cmd):
        self.wait(self.runOn('localhost', cmd))

    def runTest(self):
        examplesJar = glob.glob(ACCUMULO_HOME+'/lib/accumulo-examples-simple*.jar')[0]

	self.comment("Testing MaxMutation constraint")
	self.ashell('createtable test_ingest\n'
                    'constraint -a org.apache.accumulo.examples.simple.constraints.MaxMutationSize\n')
        handle = self.runOn('localhost', [self.accumulo_sh(), 'org.apache.accumulo.test.TestIngest', '-u', ROOT, '--rows', '1', '--start', '0', '--cols', '10000', '-p', ROOT_PASSWORD])
        out, err = handle.communicate()
        self.failIf(handle.returncode==0)
        self.failUnless(err.find("MutationsRejectedException: # constraint violations : 1") >= 0, "Was able to insert a mutation larger than max size")
        
        self.ashell('createtable %s\nsetauths -u %s -s A,B\nquit\n' %(table, ROOT))
        self.comment("Testing dirlist example (a little)")
        self.comment("  ingesting accumulo source")
        self.execute(self.accumulo_sh(), 'org.apache.accumulo.examples.simple.dirlist.Ingest',
                     '-i', INSTANCE_NAME, '-z', ZOOKEEPERS, '-u', ROOT, '-p', ROOT_PASSWORD,
                     '--dirTable', 'dirTable',
                     '--indexTable', 'indexTable',
                     '--dataTable', 'dataTable',
                     '--vis', visibility,
                     '--chunkSize', 100000,
                     ACCUMULO_HOME+"/fate")
        self.comment("  searching for a file")
        handle = self.runOn('localhost', [self.accumulo_sh(), 'org.apache.accumulo.examples.simple.dirlist.QueryUtil',
                                          '-i', INSTANCE_NAME, '-z', ZOOKEEPERS, '-u', ROOT, '-p', ROOT_PASSWORD,
                                          '-t', 'indexTable', '--auths', auths, '--search', '--path', 'Fate.java'])
        out, err = handle.communicate()
        self.assert_(handle.returncode == 0)
        self.assert_(out.find('accumulo/fate/Fate.java') >= 0)
        self.comment("  found file at " + out)

    
        self.comment("Testing ageoff filtering")
        out = self.ashell("createtable filtertest\n"
                     "setiter -t filtertest -scan -p 10 -n myfilter -ageoff\n"
                     "\n"
                     "5000\n"
                     "\n"
                     "insert foo a b c\n"
                     "scan\n"
                     "sleep 5\n"
                     "scan\n")
        self.assert_(2 == len([line for line in out.split('\n') if line.find('foo') >= 0]))

        self.comment("Testing bloom filters are fast for missing data")
        self.ashell('createtable bloom_test\nconfig -t bloom_test -s table.bloom.enabled=true\n')
        self.execute(self.accumulo_sh(), 'org.apache.accumulo.examples.simple.client.RandomBatchWriter', '--seed', '7',
                     '-i', INSTANCE_NAME, '-z', ZOOKEEPERS, '-u', ROOT, '-p', ROOT_PASSWORD, '-t', 'bloom_test',
                     '--num', '1000000', '--min', '0', '--max', '1000000000', '--size', '50', '--batchMemory', '2M', '--batchLatency', '60s', 
                     '--batchThreads', '3')
        self.ashell('flush -t bloom_test -w\n')
        now = time.time()
        self.execute(self.accumulo_sh(), 'org.apache.accumulo.examples.simple.client.RandomBatchScanner', '--seed', '7',
                     '-i', INSTANCE_NAME, '-z', ZOOKEEPERS, '-u', ROOT, '-p', ROOT_PASSWORD, '-t', 'bloom_test',
                     '--num', '500', '--min', '0', '--max', '1000000000', '--size', '50', '--scanThreads', 4)
        diff = time.time() - now
        now = time.time()
        self.execute(self.accumulo_sh(), 'org.apache.accumulo.examples.simple.client.RandomBatchScanner', '--seed', '8',
                     '-i', INSTANCE_NAME, '-z', ZOOKEEPERS, '-u', ROOT, '-p', ROOT_PASSWORD, '-t', 'bloom_test',
                     '--num', '500', '--min', '0', '--max', '1000000000', '--size', '50', '--scanThreads', 4)
        diff2 = time.time() - now
        self.assert_(diff2 < diff)

        self.comment("Creating a sharded index of the accumulo java files")
        self.ashell('createtable shard\ncreatetable doc2term\nquit\n')
        self.execute('/bin/sh', '-c',
                     'find src -name "*.java" | xargs ./bin/accumulo org.apache.accumulo.simple.examples.shard.Index -i %s -z %s -t shard -u %s -p %s --partitions 30' %
                     (INSTANCE_NAME, ZOOKEEPERS, ROOT, ROOT_PASSWORD))
        self.execute(self.accumulo_sh(), 'org.apache.accumulo.simple.examples.shard.Query',
                     '-i', INSTANCE_NAME, '-z', ZOOKEEPERS, '-t', 'shard', '-u', ROOT, '-p', ROOT_PASSWORD,
                     'foo', 'bar')
        self.comment("Creating a word index of the sharded files")
        self.execute(self.accumulo_sh(), 'org.apache.accumulo.simple.examples.shard.Reverse',
                     '-i', INSTANCE_NAME, '-z', ZOOKEEPERS, '-t', 'shard', '--doc2Term', 'doc2term', '-u', ROOT, '-p', ROOT_PASSWORD)
        self.comment("Making 1000 conjunctive queries of 5 random words")
        self.execute(self.accumulo_sh(), 'org.apache.accumulo.simple.examples.shard.ContinuousQuery',
                     '-i', INSTANCE_NAME, '-z', ZOOKEEPERS, '-t', 'shard', '--doc2Term', 'doc2term', '-u', ROOT, '-p', ROOT_PASSWORD, '--term', 5, '--count', 1000)

        self.execute('hadoop', 'fs', '-rmr', "/tmp/input", "/tmp/files", "/tmp/splits.txt", "/tmp/failures")
        self.execute('hadoop', 'fs', '-mkdir', "/tmp/input")
        self.comment("Starting bulk ingest example")
        self.comment("   Creating some test data")
        self.execute(self.accumulo_sh(), 'org.apache.accumulo.simple.examples.mapreduce.bulk.GenerateTestData', 0, 1000000, '/tmp/input/data')
        self.execute(self.accumulo_sh(), 'org.apache.accumulo.simple.examples.mapreduce.bulk.SetupTable',
                 INSTANCE_NAME, ZOOKEEPERS, ROOT, ROOT_PASSWORD, 'bulkTable')
        self.execute(ACCUMULO_HOME+'/bin/tool.sh', examplesJar, 'org.apache.accumulo.simple.examples.mapreduce.bulk.BulkIngestExample',
                 INSTANCE_NAME, ZOOKEEPERS, ROOT, ROOT_PASSWORD, 'bulkTable', '/tmp/input', '/tmp')
        self.execute(ACCUMULO_HOME+'/bin/tool.sh', examplesJar, 'org.apache.accumulo.simple.examples.mapreduce.bulk.VerifyIngest',
                 INSTANCE_NAME, ZOOKEEPERS, ROOT, ROOT_PASSWORD, 'bulkTable', 0, 1000000)
        self.wait(self.runOn(self.masterHost(), [
            'hadoop', 'fs', '-rmr', "/tmp/tableFile", "/tmp/nines"
            ]))
        self.comment("Running TeraSortIngest for a million rows")
        # 10,000 times smaller than the real terasort
        ROWS = 1000*1000
        self.wait(self.runOn(self.masterHost(), [
            ACCUMULO_HOME+'/bin/tool.sh',
            examplesJar,
            'org.apache.accumulo.simple.examples.mapreduce.TeraSortIngest',
            '--count', ROWS,  
            '-nk', 10, '-xk', 10,
            '-nk', 78, '-xk', 78,
            '-t', 'sorted',
            '-i', INSTANCE_NAME,
            '-z', ZOOKEEPERS,
            '-u', ROOT,
            '-p', ROOT_PASSWORD,
            '--splits', 4]))
        self.comment("Looking for '999' in all rows")
        self.wait(self.runOn(self.masterHost(), [
            ACCUMULO_HOME+'/bin/tool.sh',
            examplesJar,
            'org.apache.accumulo.simple.examples.mapreduce.RegexExample',
            '-i', INSTANCE_NAME,
            '-z', ZOOKEEPERS,
            '-u', ROOT,
            '-p', ROOT_PASSWORD,
            '-t', 'sorted',
            '--rowRegex', '.*999.*',
            '/tmp/nines']))
        self.comment("Generating hashes of each row into a new table")
        self.wait(self.runOn(self.masterHost(), [
            ACCUMULO_HOME+'/bin/tool.sh',
            examplesJar,
            'org.apache.accumulo.simple.examples.mapreduce.RowHash',
            '-i', INSTANCE_NAME,
            '-z', ZOOKEEPERS,
            '-u', ROOT,
            '-p', ROOT_PASSWORD,
            '-t', 'sorted',
            '--column', ':',
            'sortedHashed',
            ]))
        self.comment("Exporting the table to HDFS")
        self.wait(self.runOn(self.masterHost(), [
            ACCUMULO_HOME+'/bin/tool.sh',
            examplesJar,
            'org.apache.accumulo.simple.examples.mapreduce.TableToFile',
            '-i', INSTANCE_NAME,
            '-z', ZOOKEEPERS,
            '-u', ROOT,
            '-p', ROOT_PASSWORD,
            '-t', 'sorted',
            '--output', '/tmp/tableFile'
            ]))
        self.comment("Running WordCount using Accumulo aggregators")
        self.wait(self.runOn(self.masterHost(), [
            'hadoop', 'fs', '-rmr', "/tmp/wc"
            ]))
        self.wait(self.runOn(self.masterHost(), [
            'hadoop', 'fs', '-mkdir', "/tmp/wc"
            ]))
        self.wait(self.runOn(self.masterHost(), [
            'hadoop', 'fs', '-copyFromLocal', ACCUMULO_HOME + "/README", "/tmp/wc/Accumulo.README"
            ]))
        self.ashell('createtable wordCount\nsetiter -scan -majc -minc -p 10 -n sum -class org.apache.accumulo.core.iterators.user.SummingCombiner\n\ncount\n\nSTRING\nquit\n')
        self.wait(self.runOn(self.masterHost(), [
            ACCUMULO_HOME+'/bin/tool.sh',
            examplesJar,
            'org.apache.accumulo.simple.examples.mapreduce.WordCount',
            '-i', INSTANCE_NAME,
            '-z', ZOOKEEPERS,
            '--input', '/tmp/wc',
            '-t', 'wctable'
            ]))
        self.comment("Inserting data with a batch writer")
        self.runExample(['org.apache.accumulo.simple.examples.helloworld.InsertWithBatchWriter',
                        '-i', INSTANCE_NAME,
                        '-z', ZOOKEEPERS,
                        '-t', 'helloBatch',
                        '-u', ROOT,
                        '-p', ROOT_PASSWORD])
        self.comment("Reading data")
        self.runExample(['org.apache.accumulo.simple.examples.helloworld.ReadData',
                        '-i', INSTANCE_NAME,
                        '-z', ZOOKEEPERS,
                        '-t', 'helloBatch',
                        '-u', ROOT,
                        '-p', ROOT_PASSWORD])
        self.comment("Running isolated scans")
        self.runExample(['org.apache.accumulo.simple.examples.isolation.InterferenceTest',
                        '-i', INSTANCE_NAME,
                        '-z', ZOOKEEPERS,
                        '-u', ROOT,
                        '-p', ROOT_PASSWORD,
                         '-t', 'itest1',
                         '--iterations', 100000,
                         '--isolated'])
        self.comment("Running scans without isolation")
        self.runExample(['org.apache.accumulo.simple.examples.isolation.InterferenceTest',
                        '-i', INSTANCE_NAME,
                        '-z', ZOOKEEPERS,
                        '-u', ROOT,
                        '-p', ROOT_PASSWORD,
                         '-t', 'itest2',
                         '--iterations', 100000])
        self.comment("Using some example constraints")
        self.ashell('\n'.join([
            'createtable testConstraints',
            'constraint -t testConstraints -a org.apache.accumulo.examples.simple.constraints.NumericValueConstraint',
            'constraint -t testConstraints -a org.apache.accumulo.examples.simple.constraints.AlphaNumKeyConstraint',
            'insert r1 cf1 cq1 1111',
            'insert r1 cf1 cq1 ABC',
            'scan',
            'quit'
            ]))
        self.comment("Performing some row operations")
        self.runExample(['org.apache.accumulo.simple.examples.client.RowOperations', 
                        '-i', INSTANCE_NAME,
                        '-z', ZOOKEEPERS,
                        '-u', ROOT,
                        '-p', ROOT_PASSWORD ])
        self.comment("Using the batch writer")
        self.runExample(['org.apache.accumulo.simple.examples.client.SequentialBatchWriter',
                        '-i', INSTANCE_NAME,
                        '-z', ZOOKEEPERS,
                        '-u', ROOT,
                        '-p', ROOT_PASSWORD,
                         '-t', table,
                         '--start', min,
                         '--num', count,
                         '--size', valueSize,
                         '--batchMemory', memory,
                         '--batchLatency', latency,
                         '--batchThreads', numThreads,
                         '--vis', visibility])
        self.comment("Reading and writing some data")
        self.runExample(['org.apache.accumulo.simple.examples.client.ReadWriteExample',
                           '-i', INSTANCE_NAME, 
                           '-z', ZOOKEEPERS, 
                           '-u', ROOT, 
                           '-p', ROOT_PASSWORD, 
                           '--auths', auths,
                           '-t', table,
                           '--createtable', 
                           '-c', 
                           '--debug'])
        self.comment("Deleting some data")
        self.runExample(['org.apache.accumulo.simple.examples.client.ReadWriteExample',
                           '-i', INSTANCE_NAME, 
                           '-z', ZOOKEEPERS, 
                           '-u', ROOT, 
                           '-p', ROOT_PASSWORD, 
                           '-s', auths,
                           '-t', table,
                           '-d', 
                           '--debug'])
        self.comment("Writing some random data with the batch writer")
        self.runExample(['org.apache.accumulo.simple.examples.client.RandomBatchWriter',
                           '-i', INSTANCE_NAME, 
                           '-z', ZOOKEEPERS, 
                           '-u', ROOT, 
                           '-p', ROOT_PASSWORD, 
                           '-t', table,
                           '--num', count, 
                           '--min', min, 
                           '--max', max, 
                           '--size', valueSize, 
                           '--batchMemory', memory, 
                           '--batchLatency', latency, 
                           '--batchThreads', numThreads, 
                           '--vis', visibility])
        self.comment("Writing some random data with the batch writer")
        self.runExample(['org.apache.accumulo.simple.examples.client.RandomBatchScanner',
                           '-i', INSTANCE_NAME, 
                           '-z', ZOOKEEPERS, 
                           '-u', ROOT, 
                           '-p', ROOT_PASSWORD, 
                           '-t', table,
                           '--num', count, 
                           '--min', min, 
                           '--max', max, 
                           '--size', valueSize, 
                           '--scanThreads', numThreads, 
                           '--auths', auths]);
        self.comment("Running an example table operation (Flush)")
        self.runExample(['org.apache.accumulo.simple.examples.client.Flush',
                           '-i', INSTANCE_NAME, 
                           '-z', ZOOKEEPERS, 
                           '-u', ROOT, 
                           '-p', ROOT_PASSWORD, 
                           '-t', table])
        self.shutdown_accumulo();


def suite():
    result = unittest.TestSuite()
    result.addTest(Examples())
    return result
