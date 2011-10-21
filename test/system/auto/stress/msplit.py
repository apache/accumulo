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
import time

from TestUtils import ACCUMULO_DIR

from simple.split import TabletShouldSplit

log = logging.getLogger('test.auto')

class MetaSplitTest(TabletShouldSplit):

    order = TabletShouldSplit.order + 1
    
    tableSettings = TabletShouldSplit.tableSettings.copy()
    tableSettings['!METADATA'] = { 
    	'table.split.threshold': 500,
        }
    tableSettings['test_ingest'] = { 
    	'table.split.threshold': '10K',
        }

    def runTest(self):
        TabletShouldSplit.runTest(self)
        handle = self.runOn(self.masterHost(), [
            'hadoop', 'fs', '-ls', os.path.join(ACCUMULO_DIR,'tables',self.getTableId('!METADATA'))
            ])
        out, err = handle.communicate()
        lst = [line for line in out.split('\n') if line.find('tables') >= 0]
        self.assert_(len(lst) > 3)

def suite():
    result = unittest.TestSuite()
    result.addTest(MetaSplitTest())
    return result
