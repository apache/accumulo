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

from TestUtils import TestUtilsMixin

log = logging.getLogger('test.auto')

class BigRootTablet(TestUtilsMixin, unittest.TestCase):
    "ACCUMULO-542: A large root tablet will fail to load if it does't fit in the tserver scan buffers"

    order = 80

    settings = TestUtilsMixin.settings.copy()
    settings['table.scan.max.memory'] = '1024'
    settings['tserver.compaction.major.delay'] = '60m'

    def setUp(self):
        TestUtilsMixin.setUp(self);
    
    def tearDown(self):
        TestUtilsMixin.tearDown(self);
    
    def runTest(self):
	cmd = 'table !METADATA\naddsplits 0 1 2 3 4 5 6 7 8 9 a\n'
        for i in range(10):
	    cmd += 'createtable %s\nflush -t !METADATA\n' % i
        self.shell(self.masterHost(), cmd)
	self.stop_accumulo()
	self.start_accumulo()

def suite():
    result = unittest.TestSuite()
    result.addTest(BigRootTablet())
    return result
