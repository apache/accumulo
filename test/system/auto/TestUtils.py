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

from subprocess import Popen as BasePopen, PIPE

import os
import time
import logging
import unittest
import sys
import socket
import signal
import select
import random
import shutil
import sleep

# mapreduce sets SIGHUP to ignore, which we use to stop child processes
# so set it back to the default
signal.signal(signal.SIGHUP, signal.SIG_DFL)

# determine unique identity to use for this test run
ID=socket.getfqdn().split('.')[0] + '-' + str(os.getpid())

# offset the port numbers a little to allow simultaneous test execution
FUZZ=os.getpid() % 997

# figure out where we are
ACCUMULO_HOME = os.path.dirname(__file__)
ACCUMULO_HOME = os.path.join(ACCUMULO_HOME, *(os.path.pardir,)*3)
ACCUMULO_HOME = os.path.realpath(ACCUMULO_HOME)
ACCUMULO_DIR = "/user/" + os.getlogin() + "/accumulo-" + ID
SITE = "test-" + ID

WALOG = os.path.join(ACCUMULO_HOME, 'walogs', ID)
LOG_PROPERTIES= os.path.join(ACCUMULO_HOME, 'conf', 'log4j.properties')
LOG_GENERIC = os.path.join(ACCUMULO_HOME, 'conf', 'generic_logger.xml')
General_CLASSPATH = ("$ACCUMULO_HOME/lib/[^.].$ACCUMULO_VERSION.jar, $ACCUMULO_HOME/lib/[^.].*.jar, $ZOOKEEPER_HOME/zookeeper[^.].*.jar,"
"$HADOOP_HOME/conf,$HADOOP_HOME/[^.].*.jar, $HADOOP_HOME/lib/[^.].*.jar") 

log = logging.getLogger('test.auto')

ROOT = 'root'
ROOT_PASSWORD = 'secret'
INSTANCE_NAME=ID
ZOOKEEPERS = socket.getfqdn()

accumulo_site = os.path.join(ACCUMULO_HOME, 'conf', 'accumulo-site.xml')
if os.path.exists(accumulo_site):
   import config
   ZOOKEEPERS = config.parse(accumulo_site).get('instance.zookeeper.host', ZOOKEEPERS)

class Popen(BasePopen):
   def __init__(self, cmd, **args):
      self.cmd = cmd
      BasePopen.__init__(self, cmd, **args)

def quote(cmd):
   result = []
   for part in cmd:
      if '"' in part:
         result.append("'%s'" % part)
      else:
         result.append('"%s"' % part)
   return result

class TestUtilsMixin:
    "Define lots of utilities to run accumulo utilities"
    hosts = ()                          # machines to run accumulo

    settings = {'tserver.port.search': 'true',
                'tserver.memory.maps.max':'100M',
                'tserver.cache.data.size':'10M',
                'tserver.cache.index.size':'20M',
                'instance.zookeeper.timeout': '10s',
                'gc.cycle.delay': '1s',
                'gc.cycle.start': '1s',
                }
    tableSettings = {}

    def masterHost(self):
        return self.hosts[0]

    def runOn(self, host, cmd, **opts):
        cmd = map(str, cmd)
        log.debug('%s: %s', host, ' '.join(cmd))
        if host == 'localhost':
            os.environ['ACCUMULO_TSERVER_OPTS']='-Xmx700m -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 '
            os.environ['ACCUMULO_GENERAL_OPTS']=('-Dorg.apache.accumulo.config.file=%s' % (SITE))
            os.environ['ACCUMULO_LOG_DIR']= ACCUMULO_HOME + '/logs/' + ID
            return Popen(cmd, stdout=PIPE, stderr=PIPE, **opts)
        else:
            cp = 'HADOOP_CLASSPATH=%s' % os.environ.get('HADOOP_CLASSPATH','')
            jo = "ACCUMULO_TSERVER_OPTS='-Xmx700m -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 '"
            go = ("ACCUMULO_GENERAL_OPTS='-Dorg.apache.accumulo.config.file=%s'" % (SITE))
            ld = 'ACCUMULO_LOG_DIR=%s/logs/%s' % (ACCUMULO_HOME, ID)
            execcmd = ['ssh', '-q', host, cp, jo, go, ld] + quote(cmd)
            log.debug(repr(execcmd))
            return Popen(execcmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, **opts)
            
    def shell(self, host, input, **opts):
        """Run accumulo shell with the given input,
        return the exit code, stdout and stderr"""
        log.debug("Running shell with %r", input)
        handle = self.runOn(host, [self.accumulo_sh(), 'shell', '-u', ROOT, '-p', ROOT_PASSWORD], stdin=PIPE, **opts)
        out, err = handle.communicate(input)
        return out, err, handle.returncode

    def accumulo_sh(self):
        "Determine the location of accumulo"
        result = os.path.join(ACCUMULO_HOME, 'scripts', 'accumulo')
        if not os.path.exists(result):
            result = os.path.join(ACCUMULO_HOME, 'bin', 'accumulo')
        return result

    def start_master(self, host, safeMode=None):
        goalState = 'NORMAL'
        if safeMode:
           goalState = 'SAFE_MODE'
        self.wait(self.runOn('localhost', 
                             [self.accumulo_sh(), 
                              'org.apache.accumulo.server.master.state.SetGoalState', 
                              goalState]))
        return self.runOn(host, [self.accumulo_sh(), 'master'])

    def processResult(self, out, err, code):
        if out:
            log.debug("Output from command: %s", str(out).rstrip())
        if err:
            if err.find('at org.apache.accumulo.core') > 0 or err.find('at org.apache.accumulo.server') > 0 :
                log.error("This looks like a stack trace: %s", err)
                return False
            else:
                log.info("Error output from command: %s", err.rstrip())
        log.debug("Exit code: %s", code)
        return code == 0
        

    def wait(self, handle):
        out, err = handle.communicate()
        return self.processResult(out, err, handle.returncode)


    def pkill(self, host, pattern, signal=signal.SIGKILL):
        cmd = [os.path.join(ACCUMULO_HOME, 'test', 'system', 'auto', 'pkill.sh'), str(signal), str(os.getuid()), ID + '.*' + pattern]
        handle = self.runOn(host, cmd)
        handle.communicate()

    def cleanupAccumuloHandles(self, secs=2):
        handles = []
        for h in self.accumuloHandles:
            if not self.isStopped(h, secs):
                handles.append(h)
        self.accumuloHandles = handles

    def stop_master(self, host):
        self.pkill(host, 'Main master$', signal=signal.SIGHUP)
        self.cleanupAccumuloHandles()

    def stop_logger(self, host):
        self.pkill(host, 'Main logger$', signal=signal.SIGHUP)
        # wait for it to stop
        self.sleep(1)
        self.cleanupAccumuloHandles(0.5)

    def start_tserver(self, host):
        return self.runOn(host,
                          [self.accumulo_sh(), 'tserver'])

    def start_logger(self, host):
        return self.runOn(host,
                          [self.accumulo_sh(), 'logger'])

    def start_monitor(self, host):
        return self.runOn(host, [self.accumulo_sh(), 'monitor'])

    def start_gc(self, host):
        return self.runOn(host, [self.accumulo_sh(), 'gc'])

    def stop_gc(self, host):
        self.pkill(host, 'Main gc$', signal=signal.SIGHUP)
        # wait for it to stop
        self.sleep(0.5)
        self.cleanupAccumuloHandles(0.5)

    def stop_monitor(self, host):
        self.pkill(host, 'Main monitor$', signal=signal.SIGHUP)
        # wait for it to stop
        self.sleep(0.5)
        self.cleanupAccumuloHandles(0.5)

    def stop_tserver(self, host, signal=signal.SIGHUP):
        self.pkill(host, 'Main tserver$', signal)
        # wait for it to stop
        self.sleep(0.5)
        self.cleanupAccumuloHandles(0.5)

    def runClassOn(self, host, klass, args, **kwargs):
        "Invoke a the given class in the accumulo classpath"
        return self.runOn(host,
                          [self.accumulo_sh(), klass] + args,
                          **kwargs)

    def ingest(self, host, count, start=0, timestamp=None, size=50, colf=None, **kwargs):
        klass = 'org.apache.accumulo.server.test.TestIngest'
        args = ''
        if timestamp:
            args += "-timestamp %ld " % int(timestamp)
        args += '-tsbw -size %d -random 56 %d %d 1 ' % (size, count, start)
        if colf:
           args = '-colf %s ' % colf + args
        return self.runClassOn(host, klass, args.split(), **kwargs)

    def verify(self, host, count, start=0, size=50, timestamp=None, colf='colf'):
        klass = 'org.apache.accumulo.server.test.VerifyIngest'
        args = ''
        if timestamp:
            args += "-timestamp %ld " % int(timestamp)
        args += '-size %d -random 56 -colf %s %d %d 1 ' % (size, colf, count, start)
        return self.runClassOn(host, klass, args.split())

    def stop_accumulo(self, signal=signal.SIGHUP):
        log.info('killing accumulo processes everywhere')
        for host in self.hosts:
            self.pkill(host, 'org.apache.accumulo.start', signal)

    def create_config_file(self, settings):
        fp = open(os.path.join(ACCUMULO_HOME, 'conf', SITE),
                  'w')
        fp.write('<configuration>\n')
        settings = self.settings.copy()
        settings.update({ 'instance.zookeeper.host': ZOOKEEPERS,
                          'instance.dfs.dir': ACCUMULO_DIR,
                          'tserver.port.client': 39000 + FUZZ,
                          'master.port.client':  41000 + FUZZ,
                          'monitor.port.client': 50099,
                          'logger.port.client':  44000 + FUZZ,
                          'gc.port.client':      45000 + FUZZ,
                          'logger.dir.walog': WALOG,
                          'general.classpaths' :General_CLASSPATH,
                          'instance.secret': 'secret',
                         })
        for a, v in settings.items():
            fp.write('  <property>\n')
            fp.write('    <name>%s</name>\n' % a)
            fp.write('    <value>%s</value>\n' % v)
            fp.write('  </property>\n')
        fp.write('</configuration>\n')
        fp.close()

    def clean_accumulo(self, host):
        self.stop_accumulo(signal.SIGKILL)
        self.create_config_file(self.settings.copy())

        os.system('rm -rf %s/logs/%s/*.log' % (ACCUMULO_HOME, ID))
        if not os.path.exists(WALOG):
           os.mkdir(WALOG)
        else:
           os.system("rm -f '%s'/*" % WALOG)

        self.wait(self.runOn(host,
                             ['hadoop', 'fs', '-rmr', ACCUMULO_DIR]))
        handle = self.runOn(host, [self.accumulo_sh(), 'init','--clear-instance-name'], stdin=PIPE)
        out, err = handle.communicate(INSTANCE_NAME+"\n"+ROOT_PASSWORD + "\n" + ROOT_PASSWORD+"\n")
        self.processResult(out, err, handle.returncode)

    def setup_logging(self):
      if os.path.exists(LOG_PROPERTIES):
         os.rename(LOG_PROPERTIES, '%s.bkp' % LOG_PROPERTIES)
      if os.path.exists(LOG_GENERIC):
         os.rename(LOG_GENERIC, '%s.bkp' % LOG_GENERIC)
      
      shutil.copyfile('%s/conf/examples/512MB/standalone/log4j.properties' % ACCUMULO_HOME, LOG_PROPERTIES)
      shutil.copyfile('%s/conf/examples/512MB/standalone/generic_logger.xml' % ACCUMULO_HOME, LOG_GENERIC)
      

    def start_accumulo_procs(self, safeMode=None):
        self.accumuloHandles = [
           self.start_logger(host) for host in self.hosts 
           ] + [
           self.start_tserver(host) for host in self.hosts
           ] + [
           self.start_monitor(self.masterHost())
           ]
        self.accumuloHandles.insert(0, self.start_master(self.masterHost(), safeMode))

    def setPerTableSettings(self, table):
        settings = []
        values = self.tableSettings.get(table,{})
        if values :
           for k, v in values.items():
               settings.append('config -t %s -s %s=%s\n' % (table, k, v))
           self.processResult(*self.shell(self.masterHost(), ''.join(settings)))

    def start_accumulo(self, safeMode=None):
        self.start_accumulo_procs(safeMode)
        self.setPerTableSettings('!METADATA')

    def rootShell(self, host, cmd, **opts):
        return self.shell(host, cmd, **opts)

    def flush(self, tablename):
        out, err, code = self.rootShell(self.masterHost(),
                                        "flush -t %s\n" % tablename)
        assert code == 0

    def isStopped(self, handle, secs):
        stop = time.time() + secs * sleep.scale
        
        while time.time() < stop:
            time.sleep(0.1)
            try:
                code = handle.poll()
                if code is not None:
                    out, err = '', ''
                    try:
                        out, err = handle.communicate()
                    except Exception:
                        pass
                    return True
            except OSError, ex:
                if ex.args[0] != errno.ECHILD:
                    raise
        return False

    def waitForStop(self, handle, secs):
        log.debug('Waiting for %s to stop in %s secs',
                  ' '.join(handle.cmd),
                  secs)
        stop = time.time() + secs * sleep.scale
        out = ''
        err = ''
        
        handles = []
        if handle.stdout != None:
           handles.append(handle.stdout)
        if handle.stderr != None:
           handles.append(handle.stderr)
        if handles:
            for fd in handles[:]:
               try:
                  import fcntl
                  fcntl.fcntl(fd, fcntl.F_SETFL, os.O_NDELAY)
               except:
                  handles.remove(fd)
            while time.time() < stop:
                rd, wr, ex = select.select(handles[:], [], [], max(0, stop - time.time()))
                if handle.stdout in rd:
                   more = handle.stdout.read(1024)
                   if more:
                      log.debug("out: " + more.rstrip())
                      out += more
                   else:
                      handles.remove(handle.stdout)
                if handle.stderr in rd:
                   more = handle.stderr.read(1024)
                   if more:
                      log.debug("err: " + more.rstrip())
                      err += more
                   else:
                      handles.remove(handle.stderr)
                if not handles:
                   break
        if not handles:
           if handle.returncode is None:
              handle.communicate()
           self.assert_(self.processResult(out, err, handle.returncode))
           return out, err
        self.fail("Process failed to finish in %s seconds" % secs)

    def shutdown_accumulo(self, seconds=100):
        handle = self.runOn(self.masterHost(),
                 [self.accumulo_sh(), 'admin', '-u', ROOT,
                 '-p', ROOT_PASSWORD, 'stopAll'])
        self.waitForStop(handle, seconds)
        for host in self.hosts:
            self.stop_logger(host)
        self.stop_monitor(self.masterHost())
        self.cleanupAccumuloHandles()
        # give everyone a couple seconds to completely stop
        for h in self.accumuloHandles:
            self.waitForStop(h, 60)

    def clean_logging(self):
      LOG_PROPERTIES_BACKUP='%s.bkp' % LOG_PROPERTIES 
      LOG_GENERIC_BACKUP='%s.bkp' % LOG_GENERIC
      os.remove(LOG_PROPERTIES)
      os.remove(LOG_GENERIC)
      if os.path.exists(LOG_PROPERTIES_BACKUP):
        os.rename(LOG_PROPERTIES_BACKUP, LOG_PROPERTIES)
      if os.path.exists(LOG_GENERIC_BACKUP):
         os.rename(LOG_GENERIC_BACKUP, LOG_GENERIC)

    def sleep(self, secs):
        log.debug("Sleeping %f seconds" % secs)
        sleep.sleep(secs)

    def setUp(self):
        self.hosts = self.options.hosts
        self.clean_accumulo(self.masterHost())
        self.setup_logging()
        self.start_accumulo()

    def tearDown(self):
        if self.options.clean:
          self.stop_accumulo()
          self.wait(self.runOn(self.masterHost(),
                               ['hadoop', 'fs', '-rmr', ACCUMULO_DIR]))
          self.wait(self.runClassOn(self.masterHost(),
                                    'org.apache.accumulo.server.util.DeleteZooInstance',
                                    [INSTANCE_NAME]))
          self.wait(self.runOn(self.masterHost(), ['rm', '-rf', WALOG]))
          self.wait(self.runOn(self.masterHost(), ['rm', '-rf', ACCUMULO_HOME + '/logs/' + ID]))
          self.clean_logging() 
          os.unlink(os.path.join(ACCUMULO_HOME, 'conf', SITE))

    def createTable(self, table, splitFile=None):
        if splitFile :
            out, err, code = self.rootShell(self.masterHost(),
                                        "createtable %s -sf %s\n" % (table, splitFile))
        else :
            out, err, code = self.rootShell(self.masterHost(),
                                        "createtable %s\n" % table)
        self.processResult(out, err, code)
        self.setPerTableSettings(table)

    def getTableId(self, table):
        if table == '!METADATA' :
            return '!0'
        handle = self.runClassOn(self.masterHost(), 'org.apache.accumulo.server.test.ListTables',[]);
        out,err = handle.communicate()
        self.assert_(handle.returncode==0)
        for line in out.split('\n') :
            left, right = line.split("=>")
            left = left.strip()
            right = right.strip()
            if left == table :
               return right
        else :
           self.fail('Did not find table id for '+table)
