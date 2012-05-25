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
import glob

from aggregation import AggregationTest
from TestUtils import ACCUMULO_HOME

log = logging.getLogger('test.auto')

class DynamicClassloader(AggregationTest):
    "Start a clean accumulo, use an newly created aggregator, verify the data is aggregated"

    order = 25
    def runWait(self, cmd):
        handle = self.runOn(self.masterHost(), ['bash', '-c', cmd]);
        self.wait(handle)
    def runTest(self):

        import string, random
        rand = list(string.hexdigits)
        random.shuffle(rand)
        rand = ''.join(rand[0:4])
        #Make sure paths exists for test
        if not os.path.exists(os.path.join(ACCUMULO_HOME, 'target','dynamictest%s' % rand, 'accumulo','test')):
          os.makedirs(os.path.join(ACCUMULO_HOME, 'target', 'dynamictest%s' % rand, 'accumulo', 'test'))
        fp = open(os.path.join(ACCUMULO_HOME, 'target', 'dynamictest%s' % rand, 'accumulo', 'test', 'StringSummation%s.java' % rand), 'wb')
        fp.write('''
package accumulo.test;

import org.apache.accumulo.core.data.Value;

public class StringSummation%s implements org.apache.accumulo.core.iterators.aggregation.Aggregator {

	long sum = 0;
	
	public Value aggregate() {
		return new Value(Long.toString(sum).getBytes());
	}

	public void collect(Value value) {
		sum += Long.parseLong(new String(value.get()));
	}

	public void reset() {
		sum = 0;
		
	}
}
''' % rand)
        fp.close()

        handle = self.runOn(self.masterHost(), [self.accumulo_sh(), 'classpath'])
        out, err = handle.communicate()
        path = ':'.join(out.split('\n')[1:])

        self.runWait("javac -cp %s:%s %s" % (
            path,
            os.path.join(ACCUMULO_HOME,'src','core','target','classes'),
            os.path.join(ACCUMULO_HOME,'target','dynamictest%s' % rand,'accumulo','test','StringSummation%s.java' % rand)
            ))
        self.runWait("jar -cf %s -C %s accumulo/" % (
            os.path.join(ACCUMULO_HOME,'lib','ext','Aggregator%s.jar' % rand),
            os.path.join(ACCUMULO_HOME,'target','dynamictest%s' % rand)
            ))

        self.sleep(1)

        # initialize the database
        aggregator = 'accumulo.test.StringSummation%s' % rand
        cmd = 'createtable test\nsetiter -agg -minc -majc -scan -p 10 -t test\ncf ' + aggregator + '\n\n'
        out, err, code = self.rootShell(self.masterHost(),"%s\n" % cmd)
        self.assert_(code == 0)

        # insert some rows
        log.info("Starting Test Ingester")
        cmd = ''
        for i in range(10):
            cmd += 'table test\ninsert row1 cf col1 %d\n' % i
        out, err, code = self.rootShell(self.masterHost(), cmd)
        self.assert_(code == 0)
        self.checkSum()
        self.shutdown_accumulo()
        self.start_accumulo()
        self.checkSum()
        os.remove(os.path.join(ACCUMULO_HOME, 'lib','ext','Aggregator%s.jar' % rand))
def suite():
    result = unittest.TestSuite()
    result.addTest(DynamicClassloader())
    return result
