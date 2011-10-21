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

import unittest

import os
from TestUtils import TestUtilsMixin, ROOT, ROOT_PASSWORD, ACCUMULO_DIR
from subprocess import PIPE

class Start(TestUtilsMixin, unittest.TestCase):

    order = 21

    def start(self, *args):
        handle = self.runOn(self.masterHost(),
                            [self.accumulo_sh(), 'org.apache.accumulo.start.TestMain'] + list(args), stdin=PIPE)
        out, err = handle.communicate('')
        return handle.returncode

    def runTest(self):
        assert self.start() != 0
        assert self.start('success') == 0
        assert self.start('exception') != 0
        
def suite():
    result = unittest.TestSuite()
    result.addTest(Start())
    return result
