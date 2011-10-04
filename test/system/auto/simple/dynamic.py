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

        self.runWait("javac -cp %s:%s %s" % (
            os.path.join(ACCUMULO_HOME,'src','core','target','classes'),
            glob.glob(os.path.join(ACCUMULO_HOME,'lib','accumulo-core*.jar'))[0],
            os.path.join(ACCUMULO_HOME,'target','dynamictest%s' % rand,'accumulo','test','StringSummation%s.java' % rand)
            ))
        self.runWait("jar -cf %s -C %s accumulo/" % (
            os.path.join(ACCUMULO_HOME,'lib','ext','Aggregator%s.jar' % rand),
            os.path.join(ACCUMULO_HOME,'target','dynamictest%s' % rand)
            ))

        self.sleep(1)

        # initialize the database
        aggregator = 'accumulo.test.StringSummation%s' % rand
        cmd = 'createtable test -a cf=' + aggregator
        out, err, code = self.rootShell(self.masterHost(),"%s\n" % cmd)

        # insert some rows
        log.info("Starting Test Ingester")
        cmd = ''
        for i in range(10):
            cmd += 'table test\ninsert row1 cf col1 %d\n' % i
        out, err, code = self.rootShell(self.masterHost(), cmd)
        self.checkSum()
        self.shutdown_accumulo()
        self.start_accumulo()
        self.checkSum()
        os.remove(os.path.join(ACCUMULO_HOME, 'lib','ext','Aggregator%s.jar' % rand))
def suite():
    result = unittest.TestSuite()
    result.addTest(DynamicClassloader())
    return result
