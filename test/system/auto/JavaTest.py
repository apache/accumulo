import os
import unittest
import time
import logging

from subprocess import PIPE
from TestUtils import TestUtilsMixin, ROOT, ROOT_PASSWORD, INSTANCE_NAME

log = logging.getLogger('test.auto')

class JavaTest(TestUtilsMixin, unittest.TestCase):
    "Base class for Java Functional Test"

    order = 21
    testClass=""

    maxRuntime = 120

    def setUp(self):
        handle = self.runJTest('localhost','getConfig')
        out,err = handle.communicate()
        log.debug(out)
        log.debug(err)
        assert handle.returncode==0

        self.settings = TestUtilsMixin.settings.copy()
        self.settings.update(eval(out))
        TestUtilsMixin.setUp(self);

        handle = self.runJTest(self.masterHost(),'setup')
        out,err = handle.communicate()
        log.debug(out)
        log.debug(err)
        assert handle.returncode==0

    def runJTest(self,host, cmd):
        return self.runClassOn(host, 'org.apache.accumulo.server.test.functional.FunctionalTest', ['-m',host,'-u',ROOT,'-p',ROOT_PASSWORD,'-i',INSTANCE_NAME,self.testClass,cmd])
        
    def runTest(self):
        handle = self.runJTest(self.masterHost(),'run')
        self.waitForStop(handle, self.maxRuntime)

        handle = self.runJTest(self.masterHost(),'cleanup')
        out,err = handle.communicate()
        log.debug(out)
        log.debug(err)
        assert handle.returncode==0

        self.shutdown_accumulo()


