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

import unittest

from TestUtils import TestUtilsMixin
from JavaTest import JavaTest

log = logging.getLogger('test.auto')

class Merge(unittest.TestCase, TestUtilsMixin):
    "Start a clean accumulo, split a table and merge part of it"
    order = 80

    def setUp(self):
        TestUtilsMixin.setUp(self);

    def runTest(self):
        out, err, code = self.shell(self.masterHost(), '''
createtable test
addsplits a b c d e f g h i j k
insert a cf cq value
insert b cf cq value
insert c cf cq value
insert d cf cq value
insert e cf cq value
insert f cf cq value
insert g cf cq value
insert h cf cq value
insert i cf cq value
insert j cf cq value
insert k cf cq value
flush -w
merge -b c1 -e f1
getsplits
quit
''')
        self.assert_(code == 0)
        out = out[out.find('getsplits'):out.find('quit')]
        self.assert_(len(out.split('\n')) == 10)
        out, err, code = self.shell(self.masterHost(), '''
scan -t test
quit
''')
        out = out[out.find('test\n'):out.find('quit')]
        self.assert_(len(out.split('\n')) == 13)


class MergeSize(unittest.TestCase, TestUtilsMixin):
    "Start a clean accumulo, split a table and merge based on size"
    order = 80

    def setUp(self):
        TestUtilsMixin.setUp(self);

    def runTest(self):
        out, err, code = self.shell(self.masterHost(), '''
createtable merge
addsplits a b c d e f g h i j k l m n o p q r s t u v w x y z
insert c cf cq mersydotesanddozeydotesanlittolamsiedives
insert e cf cq mersydotesanddozeydotesanlittolamsiedives
insert f cf cq mersydotesanddozeydotesanlittolamsiedives
insert y cf cq mersydotesanddozeydotesanlittolamsiedives
flush -w
merge -s 100 -v
getsplits
merge -s 100 -f -v
getsplits
quit
''')
        self.assert_(code == 0)
        out = out.split("getsplits")
        firstMerge = out[-2]
        firstMerge = firstMerge.strip().split('\n')[:-5]
        self.assert_(firstMerge == ['b','c','d','e','f','x','y'])
        secondMerge = out[-1]
        secondMerge = secondMerge.strip().split('\n')[:-1]
        self.assert_(secondMerge == ['c','e','f','y'])

class MergeTest(JavaTest):
    "Test Merge"

    order = 92
    testClass="org.apache.accumulo.server.test.functional.MergeTest"


def suite():
    result = unittest.TestSuite()
    result.addTest(Merge())
    result.addTest(MergeSize())
    result.addTest(MergeTest())
    return result
