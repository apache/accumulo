/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Iface;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Processor;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Tablet server that creates a lock in zookeeper, responds to one status request, and then hangs on
 * subsequent requests. Exits with code zero if halted.
 */
public class ZombieTServer {

  public static class ThriftClientHandler
      extends org.apache.accumulo.test.performance.NullTserver.ThriftClientHandler {

    int statusCount = 0;

    boolean halted = false;

    ThriftClientHandler(ServerContext context, TransactionWatcher watcher) {
      super(context, watcher);
    }

    @Override
    public synchronized void fastHalt(TInfo tinfo, TCredentials credentials, String lock) {
      halted = true;
      notifyAll();
    }

    @Override
    public TabletServerStatus getTabletServerStatus(TInfo tinfo, TCredentials credentials) {
      synchronized (this) {
        if (statusCount++ < 1) {
          TabletServerStatus result = new TabletServerStatus();
          result.tableMap = new HashMap<>();
          return result;
        }
      }
      sleepUninterruptibly(Integer.MAX_VALUE, TimeUnit.DAYS);
      return null;
    }

    @Override
    public synchronized void halt(TInfo tinfo, TCredentials credentials, String lock) {
      halted = true;
      notifyAll();
    }

  }

  private static final Logger log = LoggerFactory.getLogger(ZombieTServer.class);

  public static void main(String[] args) throws Exception {
    Random random = new SecureRandom();
    random.setSeed(System.currentTimeMillis() % 1000);
    int port = random.nextInt(30000) + 2000;
    var context = new ServerContext(SiteConfiguration.auto());
    TransactionWatcher watcher = new TransactionWatcher(context);
    final ThriftClientHandler tch = new ThriftClientHandler(context, watcher);
    Processor<Iface> processor = new Processor<>(tch);
    ServerAddress serverPort = TServerUtils.startTServer(
        Metrics.initSystem(ZombieTServer.class.getSimpleName()), context.getConfiguration(),
        ThriftServerType.CUSTOM_HS_HA, processor, "ZombieTServer", "walking dead", 2, 1, 1000,
        10 * 1024 * 1024, null, null, -1, HostAndPort.fromParts("0.0.0.0", port));

    String addressString = serverPort.address.toString();
    String zPath = context.getZooKeeperRoot() + Constants.ZTSERVERS + "/" + addressString;
    ZooReaderWriter zoo = context.getZooReaderWriter();
    zoo.putPersistentData(zPath, new byte[] {}, NodeExistsPolicy.SKIP);

    ZooLock zlock = new ZooLock(zoo, zPath);

    LockWatcher lw = new LockWatcher() {

      @SuppressFBWarnings(value = "DM_EXIT",
          justification = "System.exit() is a bad idea here, but okay for now, since it's a test")
      @Override
      public void lostLock(final LockLossReason reason) {
        try {
          tch.halt(TraceUtil.traceInfo(), null, null);
        } catch (Exception ex) {
          log.error("Exception", ex);
          System.exit(1);
        }
      }

      @SuppressFBWarnings(value = "DM_EXIT",
          justification = "System.exit() is a bad idea here, but okay for now, since it's a test")
      @Override
      public void unableToMonitorLockNode(Throwable e) {
        try {
          tch.halt(TraceUtil.traceInfo(), null, null);
        } catch (Exception ex) {
          log.error("Exception", ex);
          System.exit(1);
        }
      }
    };

    byte[] lockContent =
        new ServerServices(addressString, Service.TSERV_CLIENT).toString().getBytes(UTF_8);
    if (zlock.tryLock(lw, lockContent)) {
      log.debug("Obtained tablet server lock {}", zlock.getLockPath());
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
