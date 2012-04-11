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

class LateLastContact(unittest.TestCase, TestUtilsMixin):
    """Fake the "tablet stops talking but holds its lock" problem we see when hard drives and NFS fail.
       Start a ZombieTServer, and see that master stops it.
    """

    order = 80

    settings = TestUtilsMixin.settings.copy()
    settings['general.rpc.timeout'] = '2s'

    def setUp(self):
        TestUtilsMixin.setUp(self);
    
    def runTest(self):
        handle = self.runClassOn(self.masterHost(), 'org.apache.accumulo.server.test.functional.ZombieTServer', [])
        out, err = handle.communicate()
        assert handle.returncode == 0

def suite():
    result = unittest.TestSuite()
    result.addTest(LateLastContact())
    return result
