#! /usr/bin/env python

import getopt
import os
import sys
import logging
import unittest

from lib.options import options, args, log
from lib.Benchmark import Benchmark

def getBenchmarks():
    import glob
    result = []
    here = os.path.dirname(__file__)
    sys.path.insert(0, here)
    for path in glob.glob('%s/*/*.py' % here):
        path = path[len(here):]
        if path.find('__init__') >= 0: continue
        if path.find('/lib/') >= 0: continue
        moduleName = path.replace(os.path.sep, '.')
        moduleName = moduleName.lstrip('.')[:-3]
        module = __import__(moduleName, globals(), locals(), [moduleName])
        result.extend(list(module.suite()))
    return result
    
def benchComparator(first, second):
    if (first.name() < second.name()):
        return -1
    elif (second.name() < first.name()):
        return 1
    else:  
        return 0

def main():
    if not os.getenv('HADOOP_HOME'):
        print 'Please set the environment variable \'HADOOP_HOME\' before running the benchmarks'
        sys.exit(0)
    if not os.getenv('ZOOKEEPER_HOME'):
        print 'Please set the environment variable \'ZOOKEEPER_HOME\' before running the benchmarks'
        sys.exit(0)
    import textwrap
    benchmarks = getBenchmarks()
    benchmarks.sort(benchComparator)
    auth = 0
    for b in benchmarks:
        b.setSpeed(options.runSpeed)
        if auth == 0 and b.needsAuthentication > 0:
            auth = 1 
    if options.list:
        indent = len(benchmarks[0].name())
        wrap = 78 - indent
        prefix = ' ' * indent + '  '
        for b in benchmarks:
            desc = b.shortDescription() or "No description"
            desc = textwrap.wrap(desc, wrap)
            desc = '\n'.join([(prefix + line) for line in desc])
            print '%*s: %s' % (indent, b.name(), desc.lstrip())
        sys.exit(0)                      
    logging.basicConfig(level=options.logLevel)
    if auth == 1:
        if options.user == '':
            print 'User: ',
            user = sys.stdin.readline().strip()
        else:
            user = options.user
        if options.password == '':
            import getpass
            password = getpass.getpass('Password: ')
        else:
            password = options.password
        if options.zookeepers == '':
            print 'Zookeepers: ',
            zookeepers = sys.stdin.readline().strip()    
        else:
            zookeepers = options.zookeepers
        if options.instance == '':
            print 'Instance: ',
            instance = sys.stdin.readline().strip()    
        else:
            instance = options.instance
        Benchmark.instance = instance
        Benchmark.zookeepers = zookeepers
        Benchmark.instance = instance
        Benchmark.password = password
        Benchmark.username = user   
    if args:
        benchmarks = [
            b for b in benchmarks if b.name() in args
            ]
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(unittest.TestSuite(benchmarks))
    for b in benchmarks:
        log.info("%30s: %5.2f", b.name(), b.score())

if __name__ == '__main__':
    main()
