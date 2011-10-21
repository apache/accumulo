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

from TestUtils import TestUtilsMixin
import os

import logging
log = logging.getLogger('test.auto')

class MetadataMaxFiles(TestUtilsMixin, unittest.TestCase):
    "open a large !METADATA with too few files"

    order=75

    settings = TestUtilsMixin.settings.copy()
    settings['tserver.compaction.major.delay'] = 1

    def runTest(self):
        # Create a bunch of tables with splits to split the !METADATA table
        self.splitfile = 'splits'
        fp = open(self.splitfile, 'w')
        for i in range(1000):
            fp.write('%03d\n' % i)
        fp.close()
        self.splitfile = os.path.realpath(self.splitfile)
        self.shell(self.masterHost(),
                   'config -t !METADATA -s table.split.threshold=10000\n' + 
                   ''.join(['createtable test%d -sf %s\nflush -t !METADATA\n' % (i, self.splitfile) for i in range(5)]))
        self.shutdown_accumulo(150)
        
        # reconfigure accumulo to use a very small number of files
        self.stop_accumulo()
        self.settings['tserver.scan.files.open.max'] = 10
        self.create_config_file(self.settings)

        # make sure the master knows about all the tables we created
        self.sleep(5)
        self.start_accumulo()
        self.sleep(60)
        h = self.runOn(self.masterHost(),
                       [self.accumulo_sh(), 'org.apache.accumulo.server.test.GetMasterStats'])
        out, err = h.communicate()
        self.assert_(len([x for x in out.split('\n') if x.find('  Tablets 1001') == 0]) == 5)

    def tearDown(self):
        TestUtilsMixin.tearDown(self)
        os.unlink(self.splitfile)

def suite():
    result = unittest.TestSuite()
    result.addTest(MetadataMaxFiles())
    return result
