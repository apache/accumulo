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
import logging

from TestUtils import ACCUMULO_DIR
from simple.binary import BinaryTest

log = logging.getLogger('test.auto')

class BinaryStressTest(BinaryTest) :
    order = 80

    tableSettings = BinaryTest.tableSettings.copy()
    tableSettings['bt'] = { 
    	'table.split.threshold': '10K',
        }
    settings = BinaryTest.settings.copy()
    settings.update({
        'tserver.memory.maps.max':'50K',
        'tserver.compaction.major.delay': 0,
        })

    def runTest(self):
        BinaryTest.runTest(self)
        handle = self.runOn(self.masterHost(), [
            'hadoop', 'fs', '-ls', os.path.join(ACCUMULO_DIR,'tables',self.getTableId('bt'))
            ])
        out, err = handle.communicate()
        if len(out.split('\n')) < 8:
            log.debug(out)
        self.assert_(len(out.split('\n')) > 7)

def suite():
    result = unittest.TestSuite()
    result.addTest(BinaryStressTest())
    return result
