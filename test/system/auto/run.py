#! /usr/bin/env python

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
import time
import logging
import unittest
import glob
import re
import shutil
import sys
import socket
from subprocess import Popen, PIPE

from TestUtils import ACCUMULO_HOME, ACCUMULO_DIR, COBERTURA_HOME, findCoberturaJar
import sleep

log = logging.getLogger('test.auto')

def getTests():
    allTests = []
    base = os.path.dirname(os.path.realpath(__file__))
    sys.path.insert(0, base)
    for path in glob.glob(os.path.join(base,'*','*.py')):
        path = path[len(base):]
        if path.find('__init__') >= 0: continue
        moduleName = path.replace(os.path.sep, '.')
        moduleName = moduleName.lstrip('.')[:-3]
        module = __import__(moduleName, globals(), locals(), [moduleName])
        allTests.extend(list(module.suite()))
    return allTests

def parseArguments(parser, allTests):
    for test in allTests:
        if hasattr(test, 'add_options'):
            test.add_options(parser)
    options, hosts = parser.parse_args()
    options.hosts = hosts or [socket.getfqdn()]
    return options

def testName(test):
    klass = test.__class__
    return '%s.%s' % (klass.__module__, klass.__name__)

def filterTests(allTests, patterns):
    if not patterns:
        return allTests
    filtered = []
    for test in allTests:
        name = testName(test)
        for pattern in patterns:
            if re.search(pattern, name, re.IGNORECASE):
                filtered.append(test)
                break
        else:
            log.debug("Test %s filtered out", name)
    return filtered

def sortTests(tests):
    def compare(t1, t2):
        result = cmp(getattr(t1, 'order', 50), getattr(t2, 'order', 50))
        if result == 0:
            return cmp(testName(t1), testName(t2))
        return result
    copy = tests[:]
    copy.sort(compare)
    return copy

def assignOptions(tests, options):
    for test in tests:
        test.options = options

def run(cmd, **kwargs):
    log.debug("Running %s", ' '.join(cmd))
    handle = Popen(cmd, stdout=PIPE, **kwargs)
    out, err = handle.communicate()
    log.debug("Result %d (%r, %r)", handle.returncode, out, err)
    return handle.returncode

def fixCoberturaShellScripts():
    "unDOS-ify the scripts"
    shellScripts = glob.glob(os.path.join(COBERTURA_HOME,'*.sh'))
    run(['sed', '-i', r's/\r//'] + shellScripts)
    run(['chmod', '+x'] + shellScripts)

def removeCoverageFromPreviousRun():
    """If the class files change between runs, we get confusing results.
    We might be able to remove the files only if they are older than the
    jar file"""
    for f in (os.path.join(os.environ['HOME'], 'cobertura.ser'),
              'cobertura.ser'):
        try:
            os.unlink(f)
        except OSError:
            pass

def classpath(dir):
    return ':'.join([f for f in os.listdir(dir) if f.endswith('.jar')])

def instrumentAccumuloJar(jar):
    basedir = os.path.join(ACCUMULO_HOME, 'lib')
    instpath = os.path.join(ACCUMULO_HOME, 'instrumented')
    if not os.access(instpath, os.F_OK):
        os.mkdir(instpath)
    auxcp = classpath(basedir)
    jarname = os.path.basename(jar)
    destjar = os.path.join(instpath, jarname)
    if os.access(destjar, os.F_OK):
        print "%s already instrumented" % jarname
        return destjar
    cmd = os.path.join(COBERTURA_HOME, "cobertura-instrument.sh")
    print '- Instrumenting %s' % jarname
    run(['sh', '-c', '%s --basedir %s --destination %s --auxClasspath %s %s' % (cmd, basedir, instpath, auxcp, jarname)])
    assert os.path.exists('cobertura.ser')
    return destjar

def removeInstrumentedAccumuloJars():
    instpath = os.path.join(ACCUMULO_HOME, 'instrumented')
    shutil.rmtree(instpath, ignore_errors=True)

def mergeCoverage():
    "Most of the coverage ends up in $HOME due to ssh'ing around"
    fname = 'cobertura.ser'
    if os.access(os.path.join(os.environ['HOME'], fname), os.F_OK):
        run(['sh', '-c', ' '.join([
            os.path.join(COBERTURA_HOME, "cobertura-merge.sh"),
            os.path.join(os.environ['HOME'], fname),
            fname])])


def produceCoverageReport(sourceDirectories):
    reporter = os.path.join(COBERTURA_HOME, 'cobertura-report.sh')
    run(['sh', '-c', ' '.join([reporter,
                              '--destination', os.path.join(ACCUMULO_HOME,'test','reports','cobertura-xml'),
                              '--format', 'xml',
                              '--datafile', 'cobertura.ser'] +
                              sourceDirectories)])
    run(['sh', '-c', ' '.join([reporter,
                              '--destination', os.path.join(ACCUMULO_HOME,'test','reports','cobertura-html'),
                              '--format', 'html',
                              '--datafile', 'cobertura.ser'] +
                              sourceDirectories)])

class _TextTestResult(unittest.TestResult):
    """A test result class that can print formatted text results to a stream.

    Used by TextTestRunner.
    """
    separator1 = '=' * 70
    separator2 = '-' * 70

    def __init__(self, stream, descriptions):
        unittest.TestResult.__init__(self)
        self.stream = stream
        self.descriptions = descriptions
        self.successes = []

    def getDescription(self, test):
        if self.descriptions:
            return test.shortDescription() or str(test)
        else:
            return str(test)

    def startTest(self, test):
        unittest.TestResult.startTest(self, test)
        d = self.getDescription(test)
        self.stream.write(time.strftime('%T ', time.localtime()))
        self.stream.write(d)
        self.stream.write(" .%s. " % ('.' * (65 - len(d))) )

    def addSuccess(self, test):
        unittest.TestResult.addSuccess(self, test)
        self.successes.append(test)
        self.stream.writeln("ok")

    def addError(self, test, err):
        unittest.TestResult.addError(self, test, err)
        self.stream.writeln("ERROR")
        self.printErrorList('ERROR', self.errors[-1:])

    def addFailure(self, test, err):
        unittest.TestResult.addFailure(self, test, err)
        self.stream.writeln("FAIL")
        self.printErrorList('FAIL', self.failures[-1:])

    def printErrors(self):
        self.stream.writeln()
        self.printErrorList('ERROR', self.errors)
        self.printErrorList('FAIL', self.failures)

    def printErrorList(self, flavour, errors):
        for test, err in errors:
            self.stream.writeln(self.separator1)
            self.stream.writeln("%s: %s" % (flavour,self.getDescription(test)))
            self.stream.writeln(self.separator2)
            self.stream.writeln("%s" % err)

class TestRunner(unittest.TextTestRunner):
    def _makeResult(self):
        return _TextTestResult(self.stream, self.descriptions)


def makeDiskFailureLibrary():
    def dir(n):
        return os.path.join(ACCUMULO_HOME, "test/system/auto", n)
    def compile():
        fake_disk_failure = dir('fake_disk_failure')
        if sys.platform != 'darwin':
            cmd = 'gcc -D_GNU_SOURCE -Wall -fPIC %s.c -shared -o %s.so -ldl' % (fake_disk_failure, fake_disk_failure)
        else:
            cmd = 'gcc -arch x86_64 -arch i386 -dynamiclib -O3 -fPIC %s.c -o %s.so' % (fake_disk_failure, fake_disk_failure)
        log.debug(cmd)
        os.system(cmd)
    try:
        if os.stat(fake_disk_failure + '.c').st_mtime > os.stat(fake_disk_failure + '.so'):
            compile()
    except:
        compile()
    
def emitMapReduceResult(code, test):
    print '::::: %s %s' % (code, str(test))

def main():
    makeDiskFailureLibrary()
    
    from optparse import OptionParser
    usage = "usage: %prog [options] [host1 [host2 [hostn...]]]"
    parser = OptionParser(usage)
    parser.add_option('-l', '--list', dest='list', action='store_true',
                      default=False)
    parser.add_option('-v', '--level', dest='logLevel',
                      default=logging.WARN, type=int,
                      help="The logging level (%default)")
    parser.add_option('-t', '--test', dest='tests',
                      default=[], action='append',
                      help="A regular expression for the test to run.")
    parser.add_option('-C', '--coverage', dest='coverage',
                      default=False, action='store_true',
                      help="Produce a coverage report")
    parser.add_option('-r', '--repeat', dest='repeat',
                      default=1, type=int,
                      help='Number of times to repeat the tests')
    parser.add_option('-d', '--dirty', dest='clean',
                      default=True, action='store_false',
                      help='Do not clean up at the end of the test.')
    parser.add_option('-s', '--start', dest='start', default=None, 
                      help='Start the test list at the given test name')
    parser.add_option('-x', '--xml', dest='xmlreport', default=False, action='store_true',
                      help='Output test results to xml (jenkins compatible)')
    parser.add_option('-m', '--mapreduce', dest='mapreduce', default=False, action='store_true',
                      help='Output test results suitable for mapreduce')
    parser.add_option('-f', '--timeout-factor', dest='timeout_factor',
                      default=1, type=int,
                      help="Multiplier for some timeouts (use on slower hardware) (%default)")
    
    allTests = getTests()
    options = parseArguments(parser, allTests)
    
    logging.basicConfig(level=options.logLevel)
    filtered = filterTests(allTests, options.tests)
    filtered = sortTests(filtered)

    if options.start:
        while filtered:
            if re.search(options.start, testName(filtered[0]), re.IGNORECASE):
                break
            filtered = filtered[1:]

    if options.list:
        for test in filtered:
            print testName(test)
        sys.exit(0)    

    os.system("hadoop dfs -rmr %s >/dev/null 2>&1 < /dev/null" % ACCUMULO_DIR)

    assignOptions(filtered, options)

    if not os.environ.get('ZOOKEEPER_HOME', None):
       print "ZOOKEEPER_HOME needs to be set"
       sys.exit(1)

    if options.xmlreport:
        import xmlrunner
        runner = xmlrunner.XMLTestRunner(output='test-reports')
    else:    
        runner = TestRunner()

    
    suite = unittest.TestSuite()
    map(suite.addTest, filtered)

    if options.coverage:
        cobertura_jar = findCoberturaJar()
        if cobertura_jar:
            cobertura_jar = os.path.join(COBERTURA_HOME, cobertura_jar)
        if not cobertura_jar or not os.access(cobertura_jar, os.F_OK):
            print "Install Cobertura under %s" % COBERTURA_HOME
            sys.exit(1)
        fixCoberturaShellScripts()
        removeCoverageFromPreviousRun()
        os.environ['HADOOP_CLASSPATH'] = cobertura_jar
        sleep.scale = 2.0
        libpath = os.path.join(ACCUMULO_HOME,'lib')
        libs = os.listdir(libpath)
        for l in libs:
          if re.search(r'^accumulo.*\.jar$', l):
            instrumentAccumuloJar(os.path.join(libpath, l))
    else:
        removeInstrumentedAccumuloJars()

    results = []
    for i in range(options.repeat):
        results.append(runner.run(suite))

    if options.coverage:
        mergeCoverage()
        produceCoverageReport(
            [os.path.join(ACCUMULO_HOME,'src','core','src','main','java'),
             os.path.join(ACCUMULO_HOME,'src','server','src','main','java')]
            )

    numFailures = 0
    doEmitMR = options.mapreduce and not options.xmlreport
    for result in results:
        if doEmitMR:
            for test in result.successes:
                emitMapReduceResult('S', test)
            if hasattr(result, 'expectedFailures'):
                for test, err in result.expectedFailures:
                    emitMapReduceResult('G', test)
            for test, err in result.failures:
                emitMapReduceResult('F', test)
            for test, err in result.errors:
                emitMapReduceResult('E', test)
        numFailures += len(result.failures)
        numFailures += len(result.errors)
        if hasattr(result, 'unexpectedSuccesses'):
            if doEmitMR:
                for test in result.unexpectedSuccesses:
                    emitMapReduceResult('T', test)
            numFailures += len(result.unexpectedSuccesses)
    if numFailures > 0:
        sys.exit(1)

if __name__ == '__main__':
    main()
