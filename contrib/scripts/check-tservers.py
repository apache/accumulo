#! /usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# This script will check the configuration and uniformity of all the nodes in a cluster.
# Checks
#   each node is reachable via ssh
#   login identity is the same
#   the physical memory is the same
#   the mounts are the same on each machine
#   a set of writable locations (typically different disks) are in fact writable
# 
# In order to check for writable partitions, you must configure the WRITABLE variable below.
#

import subprocess
import time
import select
import os
import sys
import fcntl
import signal
if not sys.platform.startswith('linux'):
   sys.stderr.write('This script only works on linux, sorry.\n')
   sys.exit(1)

TIMEOUT = 5
WRITABLE = []
#WRITABLE = ['/srv/hdfs1', '/srv/hdfs2', '/srv/hdfs3']

def ssh(tserver, *args):
    'execute a command on a remote tserver and return the Popen handle'
    handle = subprocess.Popen( ('ssh', '-o', 'StrictHostKeyChecking=no', '-q', '-A', '-n', tserver) + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    handle.tserver = tserver
    handle.finished = False
    handle.out = ''
    return handle

def wait(handles, seconds):
    'wait for lots of handles simultaneously, and kill anything that doesn\'t return in seconds time\n'
    'Note that stdout will be stored on the handle as the "out" field and "finished" will be set to True'
    handles = handles[:]
    stop = time.time() + seconds
    for h in handles:
       fcntl.fcntl(h.stdout, fcntl.F_SETFL, os.O_NONBLOCK)
    while handles and time.time() < stop:
       wait = min(0, stop - time.time())
       handleMap = dict( [(h.stdout, h) for h in handles] )
       rd, wr, err = select.select(handleMap.keys(), [], [], wait)
       for r in rd:
           handle = handleMap[r]
           while 1:
               more = handle.stdout.read(1024)
               if more == '':
                   handles.remove(handle)
                   handle.poll()
                   handle.wait()
                   handle.finished = True
               handle.out += more
               if len(more) < 1024:
                   break
    for handle in handles:
       os.kill(handle.pid, signal.SIGKILL)
       handle.poll()

def runAll(tservers, *cmd):
    'Run the given command on all the tservers, returns Popen handles'
    handles = []
    for tserver in tservers:
        handles.append(ssh(tserver, *cmd))
    wait(handles, TIMEOUT)
    return handles

def checkIdentity(tservers):
    'Ensure the login identity is consistent across the tservers'
    handles = runAll(tservers, 'id', '-u', '-n')
    bad = set()
    myIdentity = os.popen('id -u -n').read().strip()
    for h in handles:
        if not h.finished or h.returncode != 0:
            print '#', 'cannot look at identity on', h.tserver
            bad.add(h.tserver)
        else:
            identity = h.out.strip()
            if identity != myIdentity:
                print '#', h.tserver, 'inconsistent identity', identity
                bad.add(h.tserver)
    return bad

def checkMemory(tservers):
    'Run free on all tservers and look for weird results'
    handles = runAll(tservers, 'free')
    bad = set()
    mem = {}
    swap = {}
    for h in handles:
        if not h.finished or h.returncode != 0:
            print '#', 'cannot look at memory on', h.tserver
            bad.add(h.tserver)
        else:
            if h.out.find('Swap:') < 0:
               print '#',h.tserver,'has no swap'
               bad.add(h.tserver)
               continue
            lines = h.out.split('\n')
            for line in lines:
               if line.startswith('Mem:'):
                  mem.setdefault(line.split()[1],set()).add(h.tserver)
               if line.startswith('Swap:'):
                  swap.setdefault(line.split()[1],set()).add(h.tserver)
    # order memory sizes by most common
    mems = sorted([(len(v), k, v) for k, v in mem.items()], reverse=True)
    mostCommon = float(mems[0][1])
    for _, size, tservers in mems[1:]:
        fract = abs(mostCommon - float(size)) / mostCommon
        if fract > 0.05:
            print '#',', '.join(tservers), ': unusual memory size', size
            bad.update(tservers)
    swaps = sorted([(len(v), k, v) for k, v in swap.items()], reverse=True)
    mostCommon = float(mems[0][1])
    for _, size, tservers in swaps[1:]:
        fract = abs(mostCommon - float(size) / mostCommon)
        if fract > 0.05:
            print '#',', '.join(tservers), ': unusual swap size', size
            bad.update(tservers)
    return bad

def checkWritable(tservers):
    'Touch all the directories that should be writable by this user return any nodes that fail'
    if not WRITABLE:
       print '# WRITABLE value not configured, not checking partitions'
       return []
    handles = runAll(tservers, 'touch', *WRITABLE)
    bad = set()
    for h in handles:
        if not h.finished or h.returncode != 0:
           bad.add(h.tserver)
           print '#', h.tserver, 'some drives are not writable'
    return bad

def checkMounts(tservers):
    'Check the file systems that are mounted and report any that are unusual'
    handles = runAll(tservers, 'mount')
    mounts = {}
    finished = set()
    bad = set()
    for handle in handles:
        if handle.finished and handle.returncode == 0:
            for line in handle.out.split('\n'):
                words = line.split()
                if len(words) < 5: continue
                if words[4] == 'nfs': continue
                if words[0].find(':/') >= 0: continue
                mount = words[2]
                mounts.setdefault(mount, set()).add(handle.tserver)
            finished.add(handle.tserver)
        else:
            bad.add(handle.tserver)
            print '#', handle.tserver, 'did not finish'
    for m in sorted(mounts.keys()):
        diff = finished - mounts[m]
        if diff:
            bad.update(diff)
            print '#', m, 'not mounted on', ', '.join(diff)
    return bad

def main(argv):
    if len(argv) < 1:
        sys.stderr.write('Usage: check_tservers tservers\n')
        sys.exit(1)
    sys.stdin.close()
    tservers = set()
    for tserver in open(argv[0]):
        hashPos = tserver.find('#')
        if hashPos >= 0:
           tserver = tserver[:hashPos]
        tserver = tserver.strip()
        if not tserver: continue
        tservers.add(tserver)
    bad = set()
    for test in checkIdentity, checkMemory, checkMounts, checkWritable:
        bad.update(test(tservers - bad))
    for tserver in sorted(tservers - bad):
        print tserver

main(sys.argv[1:])
