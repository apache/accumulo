#! /usr/bin/python

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

import random
import logging
import ConfigParser

# add the environment variables as default settings
import os
defaults=dict([('env.' + k, v) for k, v in os.environ.iteritems()])
config = ConfigParser.ConfigParser(defaults)

# things you can do to a particular kind of process
class Proc:
   program = 'Unknown'
   _frequencyToKill = 1.0

   def start(self, host):
       pass

   def find(self, host):
       pass

   def numberToKill(self):
       return (1, 1)

   def frequencyToKill(self):
       return self._frequencyToKill

   def user(self):
       return config.get(self.program, 'user')

   def kill(self, host, pid):
      kill = config.get('agitator', 'kill').split()
      code, stdout, stderr = self.runOn(host, kill + [pid])
      if code != 0:
         raise logging.warn("Unable to kill %d on %s (%s)", pid, host, stderr)

   def runOn(self, host, cmd):
      ssh = config.get('agitator', 'ssh').split()
      return self.run(ssh + ["%s@%s" % (self.user(), host)] + cmd)

   def run(self, cmd):
      import subprocess
      cmd = map(str, cmd)
      logging.debug('Running %s', cmd)
      p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      stdout, stderr = p.communicate()
      if stdout.strip():
         logging.debug("%s", stdout.strip())
      if stderr.strip():
         logging.error("%s", stderr.strip())
      if p.returncode != 0:
         logging.error("Problem running %s", ' '.join(cmd))
      return p.returncode, stdout, stderr

   def __repr__(self):
      return self.program

class Zookeeper(Proc):
   program = 'zookeeper'
   def __init__(self):
      self._frequencyToKill = config.getfloat(self.program, 'frequency')

   def start(self, host):
      self.runOn(host, [config.get(self.program, 'home') + '/bin/zkServer.sh start'])

   def find(self, host):
     code, stdout, stderr = self.runOn(host, ['pgrep -f [Q]uorumPeerMain || true'])
     return map(int, [line for line in stdout.split("\n") if line])

class Hadoop(Proc):
   section = 'hadoop'
   def __init__(self, program):
      self.program = program
      self._frequencyToKill = config.getfloat(self.section, program + '.frequency')
      self.minimumToKill = config.getint(self.section, program + '.kill.min')
      self.maximumToKill = config.getint(self.section, program + '.kill.max')

   def start(self, host):
      binDir = config.get(self.section, 'bin')
      self.runOn(host, ['nohup %s/hdfs %s < /dev/null >/dev/null 2>&1 &' %(binDir, self.program)])
     
   def find(self, host):
      code, stdout, stderr = self.runOn(host, ["pgrep -f 'proc[_]%s' || true" % (self.program,)])
      return map(int, [line for line in stdout.split("\n") if line])

   def numberToKill(self):
      return (self.minimumToKill, self.maximumToKill)

   def user(self):
      return config.get(self.section, 'user')

class Accumulo(Hadoop):
   section = 'accumulo'
   def start(self, host):
      home = config.get(self.section, 'home')
      self.runOn(host, ['nohup %s/bin/accumulo %s </dev/null >/dev/null 2>&1 & ' %(home, self.program)])

   def find(self, host):
      code, stdout, stderr = self.runOn(host, ["pgrep -f 'app[=]%s' || true" % self.program])
      return map(int, [line for line in stdout.split("\n") if line])

def fail(msg):
   import sys
   logging.critical(msg)
   sys.exit(1)

def jitter(n):
   return random.random() * n - n / 2

def sleep(n):
   if n > 0:
       logging.info("Sleeping %.2f", n)
       import time
       time.sleep(n)

def agitate(hosts, procs):
   starters = []

   logging.info("Agitating %s on %d hosts" % (procs, len(hosts)))

   section = 'agitator'

   # repeatedly...
   while True:
      if starters:
         # start up services that were previously killed
         t = max(0, config.getfloat(section, 'sleep.restart') + jitter(config.getfloat(section, 'sleep.jitter')))
         sleep(t)
         for host, proc in starters:
            logging.info('Starting %s on %s', proc, host)
            proc.start(host)
         starters = []

      # wait some time
      t = max(0, config.getfloat(section, 'sleep') + jitter(config.getfloat(section, 'sleep.jitter')))
      sleep(t)

      # for some processes
      for p in procs:

         # roll dice: should it be killed?
         if random.random() < p.frequencyToKill():

            # find them
            from multiprocessing import Pool
            def finder(host):
               return host, p.find(host)
            with Pool(5) as pool:
               result = pool.map(finder, hosts)
            candidates = {}
            for host, pids in result:
               if pids:
                  candidates[host] = pids

            # how many?
            minKill, maxKill = p.numberToKill()
            count = min(random.randrange(minKill, maxKill + 1), len(candidates))

            # pick the victims
            doomedHosts = random.sample(candidates.keys(), count)

            # kill them
            logging.info("Killing %s on %s", p, doomedHosts)
            for doomedHost in doomedHosts:
               pids = candidates[doomedHost]
               if not pids:
                  logging.error("Unable to kill any %s on %s: no processes of that type are running", p, doomedHost)
               else:
                  pid = random.choice(pids)
                  logging.debug("Killing %s (%d) on %s", p, pid, doomedHost)
                  p.kill(doomedHost, pid)
                  # remember to restart them later
                  starters.append((doomedHost, p))

def main():
   import argparse
   parser = argparse.ArgumentParser(description='Kill random processes')
   parser.add_argument('--log', help='set the log level', default='INFO')
   parser.add_argument('--namenodes', help='randomly kill namenodes', action="store_true")
   parser.add_argument('--secondary', help='randomly kill secondary namenode', action="store_true")
   parser.add_argument('--datanodes', help='randomly kill datanodes', action="store_true")
   parser.add_argument('--tservers', help='randomly kill tservers', action="store_true")
   parser.add_argument('--masters', help='randomly kill masters', action="store_true")
   parser.add_argument('--zookeepers', help='randomly kill zookeepers', action="store_true")
   parser.add_argument('--gc', help='randomly kill the file garbage collector', action="store_true")
   parser.add_argument('--all', 
                       help='kill any of the tservers, masters, datanodes, namenodes or zookeepers', 
                       action='store_true')
   parser.add_argument('--hosts', type=argparse.FileType('r'), required=True)
   parser.add_argument('--config', type=argparse.FileType('r'), required=True)
   args = parser.parse_args()

   config.readfp(args.config)

   level = getattr(logging, args.log.upper(), None)
   if isinstance(level, int):
      logging.basicConfig(level=level)

   procs = []
   def addIf(flag, proc):
       if flag or args.all:
          procs.append(proc)

   addIf(args.namenodes,  Hadoop('namenode'))
   addIf(args.datanodes,  Hadoop('datanode'))
   addIf(args.secondary,  Hadoop('secondarynamenode'))
   addIf(args.tservers,   Accumulo('tserver'))
   addIf(args.masters,    Accumulo('master'))
   addIf(args.gc,         Accumulo('gc'))
   addIf(args.zookeepers, Zookeeper())
   if len(procs) == 0:
       fail("No processes to agitate!\n")

   hosts = []
   for line in args.hosts.readlines():
       line = line.strip()
       if line and line[0] != '#':
           hosts.append(line)
   if not hosts:
       fail('No hosts to agitate!\n')

   agitate(hosts, procs)

if __name__ == '__main__':
   main()
