import unittest
import subprocess

from lib import cloudshell, runner, path
from lib.Benchmark import Benchmark
from lib.slaves import runEach, slaveNames
from lib.path import accumulo, accumuloJar
from lib.util import sleep
from lib.options import log

class CreateTablesBenchmark(Benchmark):
    "Creating and deleting tables"

    tables = 1000

    def setUp(self): 
        Benchmark.setUp(self)  
        
    def runTest(self):
        for x in range(1, self.tables):
            currentTable = 'test_ingest%d' % (x)      
            command = 'createtable %s\n' % (currentTable)
            log.debug("Running Command %r", command)
            code, out, err = cloudshell.run(self.username, self.password, command)
            # print err
        for x in range(1, self.tables):
            currentTable = 'test_ingest%d' % (x)      
            command = 'deletetable %s\n' % (currentTable)
            log.debug("Running Command %r", command)
            code, out, err = cloudshell.run(self.username, self.password, command)
            # print err
        log.debug("Process finished")
        return code, out, err
            
    def numTables(self):
        return self.tables
    
    def shortDescription(self):
        return 'Creates %d tables and then deletes them. '\
               'Lower score is better.' % (self.numTables())
               
    def setSpeed(self, speed):
        if speed == "slow":
            self.tables = 50
        elif speed == "medium":
            self.tables = 10
        elif speed == "fast":
            self.tables = 5
            
    def needsAuthentication(self):
        return 1
