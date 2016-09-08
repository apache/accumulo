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

import fcntl

import os
import time
import select
import subprocess

from lib.path import accumuloConf
from lib.options import log

def tserverNames():
    return [s.strip() for s in open(accumuloConf('tservers'))]

def runEach(commandMap):
    result = {}
    handles = []
    for tserver, command in commandMap.items():
        log.debug("ssh: %s: %s", tserver, command)
        handle = subprocess.Popen(['ssh',tserver] + [command],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        for h in handle.stdout, handle.stderr:
            fcntl.fcntl(h, fcntl.F_SETFL, os.O_NDELAY)
        handle.tserver = tserver
        handle.command = command
        handle.start = time.time()
        handles.append(handle)
    handlesLeft = set(handles[:])
    while handlesLeft:
        fds = {}
        doomed = set()
        for h in handlesLeft:
            more = []
            if h.stdout != None:
                more.append(h.stdout)
            if h.stderr != None:
                more.append(h.stderr)
            for m in more:
                fds[m] = h
            if not more:
                doomed.add(h)
        handlesLeft -= doomed
        if not handlesLeft: break
        rd, wr, ex = select.select(fds.keys(), [], [], 10)
        for r in rd:
            handle = fds[r]
            data = r.read(1024)
            result.setdefault(handle, ['', ''])
            if not data:
                if r == handle.stdout:
                    handle.stdout = None
                else:
                    handle.stderr = None
            if r == handle.stdout:
                result[handle][0] += data
            else:
                result[handle][1] += data
            if handle.stdout == None and handle.stderr == None:
                log.debug("Tserver %s finished in %.2f",
                          handle.tserver,
                          time.time() - handle.start)
                handle.wait()
        if not rd:
            log.debug("Waiting on %d tservers (%s...)",
                      len(handlesLeft),
                      ', '.join([h.tserver for h in handlesLeft])[:50])
    return dict([(h.tserver, (h.returncode, out, err))
                 for h, (out, err) in result.items()])

def runAll(command):
    tservers = tserverNames()
    log.debug("Running %s on %s..", command, ', '.join(tservers)[:50])
    return runEach(dict([(s, command) for s in tservers]))

