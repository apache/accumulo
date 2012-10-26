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

from subprocess import PIPE

from TestUtils import TestUtilsMixin, ROOT, ROOT_PASSWORD

import logging
import unittest
import select

log = logging.getLogger('test.auto')

class NoLoggersNoUpdate(unittest.TestCase, TestUtilsMixin):
    order = 80

    def setUp(self):
        TestUtilsMixin.setUp(self);
        
    def runTest(self):
        # initialize the database
        self.createTable('test_ingest')
        for host in self.hosts:
            self.stop_logger(host)

        log.debug("Running shell")
        handle = self.runOn(self.masterHost(), [self.accumulo_sh(), 'shell', '-u', ROOT, '-p', ROOT_PASSWORD], stdin=PIPE)
        handle.stdin.write('table test_ingest\ninsert a b c d\nscan\n')
        out = ''
        rd, wr, ex = select.select([handle.stdout], [], [], 10)
        while rd:
            out += handle.stdout.read(1)
            rd, wr, ex = select.select([handle.stdout], [], [], 10)
        self.assert_(out.find('b:c') < 0)

    def tearDown(self):
        TestUtilsMixin.tearDown(self)

def suite():
    result = unittest.TestSuite()
    result.addTest(NoLoggersNoUpdate())
    return result

