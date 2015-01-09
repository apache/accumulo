/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional;

import static com.google.common.base.Charsets.UTF_8;

import java.util.HashMap;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Iface;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Processor;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.util.TServerUtils;
import org.apache.accumulo.server.util.TServerUtils.ServerAddress;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.net.HostAndPort;

/**
 * Tablet server that creates a lock in zookeeper, responds to one status request, and then hangs on subsequent requests. Exits with code zero if halted.
 */
public class ZombieTServer {

  public static class ThriftClientHandler extends org.apache.accumulo.test.performance.thrift.NullTserver.ThriftClientHandler {

    int statusCount = 0;

    boolean halted = false;

    ThriftClientHandler(Instance instance, TransactionWatcher watcher) {
      super(instance, watcher);
    }

    @Override
    synchronized public void fastHalt(TInfo tinfo, TCredentials credentials, String lock) {
      halted = true;
      notifyAll();
    }

    @Override
    public TabletServerStatus getTabletServerStatus(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException, TException {
      synchronized (this) {
        if (statusCount++ < 1) {
          TabletServerStatus result = new TabletServerStatus();
          result.tableMap = new HashMap<String,TableInfo>();
          return result;
        }
      }
      UtilWaitThread.sleep(Integer.MAX_VALUE);
      return null;
    }

    @Override
    synchronized public void halt(TInfo tinfo, TCredentials credentials, String lock) throws ThriftSecurityException, TException {
      halted = true;
      notifyAll();
    }

  }

  public static void main(String[] args) throws Exception {
    final Logger log = Logger.getLogger(ZombieTServer.class);
    Random random = new Random(System.currentTimeMillis() % 1000);
    int port = random.nextInt(30000) + 2000;
    Instance instance = HdfsZooInstance.getInstance();

    TransactionWatcher watcher = new TransactionWatcher();
    final ThriftClientHandler tch = new ThriftClientHandler(instance, watcher);
    Processor<Iface> processor = new Processor<Iface>(tch);
    ServerAddress serverPort = TServerUtils.startTServer(HostAndPort.fromParts("0.0.0.0", port), processor, "ZombieTServer", "walking dead", 2, 1000,
        10 * 1024 * 1024, null, -1);

    String addressString = serverPort.address.toString();
    String zPath = ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/" + addressString;
    ZooReaderWriter zoo = ZooReaderWriter.getInstance();
    zoo.putPersistentData(zPath, new byte[] {}, NodeExistsPolicy.SKIP);

    ZooLock zlock = new ZooLock(zPath);

    LockWatcher lw = new LockWatcher() {
      @Override
      public void lostLock(final LockLossReason reason) {
        try {
          tch.halt(Tracer.traceInfo(), null, null);
        } catch (Exception ex) {
          log.error(ex, ex);
          System.exit(1);
        }
      }

      @Override
      public void unableToMonitorLockNode(Throwable e) {
        try {
          tch.halt(Tracer.traceInfo(), null, null);
        } catch (Exception ex) {
          log.error(ex, ex);
          System.exit(1);
        }
      }
    };

    byte[] lockContent = new ServerServices(addressString, Service.TSERV_CLIENT).toString().getBytes(UTF_8);
    if (zlock.tryLock(lw, lockContent)) {
      log.debug("Obtained tablet server lock " + zlock.getLockPath());
    }
    // modify metadata
    synchronized (tch) {
      while (!tch.halted) {
        tch.wait();
      }
    }
    System.exit(0);
  }
}
