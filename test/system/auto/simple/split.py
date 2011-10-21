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
import sleep

from TestUtils import TestUtilsMixin, ACCUMULO_DIR

log = logging.getLogger('test.auto')

from readwrite import SunnyDayTest, Interleaved
from delete import DeleteTest

class TabletShouldSplit(SunnyDayTest):

    order = 80

    settings = TestUtilsMixin.settings.copy()
    settings.update({
        'tserver.memory.maps.max':'5K',
        'tserver.compaction.major.delay': 1,
        })
    tableSettings = SunnyDayTest.tableSettings.copy()
    tableSettings['test_ingest'] = { 
    	'table.split.threshold': '5K',
        }
    def runTest(self):

        self.waitForStop(self.ingester, 60)
        self.waitForStop(self.verify(self.masterHost(), self.options.rows), 60)

        # let the server split tablets and move them around
        self.sleep(30)
        
        # verify that we can read all the data: give it a minute to load
        # tablets
        self.waitForStop(self.verify(self.masterHost(), self.options.rows),
                         120)

        # get the metadata
        out, err, code = self.shell(self.masterHost(), 'table !METADATA\nscan\n')
        self.assert_(code == 0)
        lines = []
        tableID = self.getTableId('test_ingest')
        for line in out.split('\n'):
            if line.find(tableID+';') >= 0:
                line = line[line.find(';') + 1 : line.find(' ')].strip()
                if line:
                    lines.append(line)
        # check that the row values aren't always whole rows, but something shorter
        for line in lines:
            if len(line) != len(lines[0]):
                break
        else:
            self.fail("The split points are not being shortened")

        self.assert_(len(lines) > 10)

        h = self.runOn(self.masterHost(), [self.accumulo_sh(),
                                           'org.apache.accumulo.server.util.CheckForMetadataProblems',
                                           'root',
                                           'secret'])
        out, err = h.communicate()
        self.assert_(h.returncode == 0)
        


class InterleaveSplit(Interleaved):
    order = 80

    settings = TestUtilsMixin.settings.copy()
    settings.update({
        'tserver.memory.maps.max':'5K',
        'tserver.compaction.major.delay': 1,
        })
    tableSettings = SunnyDayTest.tableSettings.copy()
    tableSettings['test_ingest'] = { 
    	'table.split.threshold': '10K',
        }

    def waitTime(self):
        return Interleaved.waitTime(self) * 10

    def runTest(self):
        Interleaved.runTest(self)
        handle = self.runOn(self.masterHost(), [
            'hadoop', 'fs', '-ls', '%s/tables/%s' % (ACCUMULO_DIR,self.getTableId('test_ingest'))
            ])
        out, err = handle.communicate()
        self.assert_(len(out.split('\n')) > 30)

class DeleteSplit(DeleteTest):
    order = 80
        
    settings = TestUtilsMixin.settings.copy()
    settings.update({
        'tserver.memory.maps.max': '50K',
        'tserver.compaction.major.delay': 1,
        })
    tableSettings = SunnyDayTest.tableSettings.copy()
    tableSettings['test_ingest'] = { 
    	'table.split.threshold': '80K',
        }

    def runTest(self):
        DeleteTest.runTest(self)
        handle = self.runOn(self.masterHost(), [
            'hadoop', 'fs', '-ls', '%s/tables/%s' % (ACCUMULO_DIR,self.getTableId('test_ingest'))
            ])
        out, err = handle.communicate()
        self.assert_(len(out.split('\n')) > 20)

def suite():
    result = unittest.TestSuite()
    result.addTest(DeleteSplit())
    result.addTest(TabletShouldSplit())
    result.addTest(InterleaveSplit())
    return result
