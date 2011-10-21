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

from optparse import OptionParser
import logging

log = logging.getLogger("test.bench")

usage = "usage: %prog [options] [benchmark]"
parser = OptionParser(usage)
parser.add_option('-l', '--list', dest='list', action='store_true',
                  default=False)
parser.add_option('-v', '--level', dest='logLevel',
                  default=logging.INFO, type=int,
                  help="The logging level (%default)")
parser.add_option('-s', '--speed', dest='runSpeed', action='store', default='slow')
parser.add_option('-u', '--user', dest='user', action='store', default='')
parser.add_option('-p', '--password', dest='password', action='store', default='')
parser.add_option('-z', '--zookeepers', dest='zookeepers', action='store', default='')
parser.add_option('-i', '--instance', dest='instance', action='store', default='')



options, args = parser.parse_args()

__all__ = ['options', 'args']

