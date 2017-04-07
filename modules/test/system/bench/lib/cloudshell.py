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


import subprocess

from lib import path
from lib import runner
from lib.options import log

    
def run(username, password, input):
    "Run a command in accumulo"
    handle = runner.start([path.accumulo('bin', 'accumulo'), 'shell', '-u', username, '-p', password],
                          stdin=subprocess.PIPE)
    log.debug("Running: %r", input)
    out, err = handle.communicate(input)
    log.debug("Process finished: %d (%s)",
              handle.returncode,
              ' '.join(handle.command))
    return handle.returncode, out, err
