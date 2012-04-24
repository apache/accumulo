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

class MergeMeta(unittest.TestCase, TestUtilsMixin):
    """Split and merge the !METADATA table"""

    order = 30

    settings = TestUtilsMixin.settings.copy()

    def setUp(self):
        TestUtilsMixin.setUp(self);
    
    def runTest(self):
        out, err, code = self.shell(self.masterHost(), '''
addsplits -t !METADATA 1 2 3 4 5
createtable a1
createtable a2
createtable a3
createtable a4
createtable a5
merge -t !METADATA
sleep 2
scan -np -t !METADATA
''')
        assert code == 0
        # look for delete entries for the abandoned directories
        assert out.find('~del') >= 0

class MergeMetaFail(unittest.TestCase, TestUtilsMixin):
    """test a failed merge of the !METADATA table"""

    order = 30

    settings = TestUtilsMixin.settings.copy()

    def setUp(self):
        TestUtilsMixin.setUp(self);

    def runTest(self):
        out, err, code = self.shell(self.masterHost(), '''
merge -t !METADATA -b ! -e !!
''')
        assert code != 0


def suite():
    result = unittest.TestSuite()
    result.addTest(MergeMeta())
    result.addTest(MergeMetaFail())
    return result
