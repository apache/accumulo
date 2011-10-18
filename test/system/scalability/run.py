#!/usr/bin/python

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


import logging
import time
import os
import sys
from ConfigParser import ConfigParser
from subprocess import Popen, PIPE

class JavaConfig:
    '''Enable access to properities in java siteConfig file'''
    def __init__(self, fname):
        self.prop_d = {}
        for line in open(fname):
            line = line.strip();
            if line.startswith('#') or len(line) == 0:
                continue
            pair = line.split('=')
            if len(pair) != 2:
                log.error("Invalid property (%s)" % line)
                continue
            self.prop_d[pair[0].strip()] = pair[1].strip()

    def get(self, prop):
        return self.prop_d[prop]

def file_len(fname):
    i=0
    for line in open(fname):
        i += 1
    return i

def runTest(testName, siteConfig, testDir, numNodes, fdata):
   
    log('Stopping accumulo')
    syscall('$ACCUMULO_HOME/bin/stop-all.sh')
 
    log('Creating slaves file for this test')
    slavesPath = siteConfig.get('SLAVES')
    nodesPath = testDir+'/nodes/%d' % numNodes
    syscall('head -n %d %s > %s' % (numNodes,slavesPath,nodesPath))

    log('Copying slaves file to accumulo config')
    syscall('cp '+nodesPath+' $ACCUMULO_HOME/conf/slaves');

    log('Removing /accumulo directory in HDFS')
    syscall("hadoop fs -rmr /accumulo")
    
    log('Initializing new Accumulo instance')
    instance = siteConfig.get('INSTANCE_NAME')
    passwd = siteConfig.get('PASSWORD')
    syscall('printf "%s\nY\n%s\n%s\n" | $ACCUMULO_HOME/bin/accumulo init' % (instance, passwd, passwd))

    log('Starting new Accumulo instance')
    syscall('$ACCUMULO_HOME/bin/start-all.sh')

    sleepTime = 30
    if numNodes > 120:
        sleepTime = int(numNodes / 4)
    log('Sleeping for %d seconds' % sleepTime)
    time.sleep(sleepTime)

    log('Setting up %s test' % testName)
    syscall('$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.scalability.Run %s setup %s' % (testName, numNodes))

    log('Sleeping for 5 seconds')
    time.sleep(5)

    log('Starting %s clients' % testName)
    numThreads = numNodes
    if int(numNodes) > 128:
        numThreads='128'
    syscall('pssh -P -h %s -p %s "$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.scalability.Run %s client %s >/tmp/scale.out 2>/tmp/scale.err &" < /dev/null' % (nodesPath, numThreads, testName, numNodes))
   
    log('Sleeping for 30 sec before checking how many clients started...')
    time.sleep(30)
    output = Popen(["hadoop fs -ls /accumulo-scale/clients"], stdout=PIPE, shell=True).communicate()[0]
    num_clients = int(output.split()[1])
    log('%s clients started!' % num_clients)

    log('Waiting until %d clients finish.' % num_clients)
    last = 0
    done = 0
    while done < num_clients:
        time.sleep(5)
        output = Popen(["hadoop fs -ls /accumulo-scale/results"], stdout=PIPE, shell=True).communicate()[0]
        if not output:
            sys.stdout.write('.')
            sys.stdout.flush()
            continue
        done = int(output.split()[1])
        if done != last:
            sys.stdout.write('.%s' % done)
        else:
            sys.stdout.write('.')
        sys.stdout.flush()
        last = done
        sys.stdout.flush()
    log('\nAll clients are finished!')

    log('Copying results from HDFS')
    resultsDir = "%s/results/%s" % (testDir, numNodes)
    syscall('hadoop fs -copyToLocal /accumulo-scale/results %s' % resultsDir)

    log('Calculating results from clients')
    times = []
    totalMs = 0L
    totalEntries = 0L
    totalBytes = 0L
    for fn in os.listdir(resultsDir):
        for line in open('%s/%s' % (resultsDir,fn)):
            words = line.split()
            if words[0] == 'ELAPSEDMS':
                ms = long(words[1].strip())
                totalMs += ms
                times.append(ms)
                totalEntries += long(words[2].strip())
                totalBytes += long(words[3].strip())
    times.sort()

    print times
    numClients = len(times)
    min = times[0] / 1000
    avg = (float(totalMs) / numClients) / 1000
    median = times[int(numClients/2)] / 1000
    max = times[numClients-1] / 1000

    log('Tservs\tClients\tMin\tAvg\tMed\tMax\tEntries\tMB')
    log('%d\t%d\t%d\t%d\t%d\t%d\t%dM\t%d' % (numNodes, numClients, min, avg, median, max, totalEntries / 1000000, totalBytes / 1000000))
    fdata.write('%d\t%d\t%d\t%d\t%d\t%d\t%dM\t%d\n' % (numNodes, numClients, min, avg, median, max, totalEntries / 1000000, totalBytes / 1000000))
    fdata.flush()

    time.sleep(5)

    log('Tearing down %s test' % testName)
    syscall('$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.scalability.Run %s teardown %s' % (testName, numNodes))

    time.sleep(10)
   
def syscall(cmd):
    log('> %s' % cmd)
    os.system(cmd)

def run(cmd, **kwargs):
    log.debug("Running %s", ' '.join(cmd))
    handle = Popen(cmd, stdout=PIPE, **kwargs)
    out, err = handle.communicate()
    log.debug("Result %d (%r, %r)", handle.returncode, out, err)
    return handle.returncode

def log(msg):
    print msg
    sys.stdout.flush()  
 
def main():

    if not os.getenv('ACCUMULO_HOME'):
        raise 'ACCUMULO_HOME needs to be set!'

    if not os.getenv('HADOOP_HOME'):
		raise 'HADOOP_HOME needs to be set!'

    if len(sys.argv) != 2:
        log('Usage: run.py <testName>')
        sys.exit()
    testName = sys.argv[1]

    logging.basicConfig(level=logging.DEBUG)

    log('Creating test directory structure')
    testDir = 'test-%d' % time.time()
    nodesDir = testDir+'/nodes'
    syscall('mkdir %s' % testDir)
    syscall('mkdir %s' % nodesDir)

    log('Removing current /accumulo-scale directory')
    syscall('hadoop fs -rmr /accumulo-scale')

    log('Creating new /accumulo-scale directory structure')
    syscall('hadoop fs -mkdir /accumulo-scale')
    syscall('hadoop fs -mkdir /accumulo-scale/clients')
    syscall('hadoop fs -mkdir /accumulo-scale/results')
    syscall('hadoop fs -chmod -R 777 /accumulo-scale')

    log('Copying config to HDFS')
    syscall('hadoop fs -copyFromLocal ./conf /accumulo-scale/conf')

    siteConfig = JavaConfig('conf/site.conf');
    slavesPath = siteConfig.get('SLAVES')
    maxNodes = file_len(slavesPath)

    fdata = open('%s/scale.dat' % testDir, 'w')
    fdata.write('Tservs\tClients\tMin\tAvg\tMed\tMax\tEntries\tMB\n')

    for numNodes in siteConfig.get('TEST_CASES').split(','):
        log('Running %s test with %s nodes' % (testName, numNodes))
        if int(numNodes) > maxNodes:
            logging.error('Skipping %r test case as slaves file %r contains only %r nodes', numNodes, slavesPath, maxNodes)
            continue
        runTest(testName, siteConfig, testDir, int(numNodes), fdata)
        sys.stdout.flush()

if __name__ == '__main__':
    main()
