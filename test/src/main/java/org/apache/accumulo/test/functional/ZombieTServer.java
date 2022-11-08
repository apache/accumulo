/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.apache.accumulo.harness.AccumuloITBase.random;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.LockLossReason;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.LockWatcher;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletScanClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.thrift.TMultiplexedProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Tablet server that creates a lock in zookeeper, responds to one status request, and then hangs on
 * subsequent requests. Exits with code zero if halted.
 */
public class ZombieTServer {

  public static class ZombieTServerThriftClientHandler
      extends org.apache.accumulo.test.performance.NullTserver.NullTServerTabletClientHandler
      implements TabletClientService.Iface, TabletScanClientService.Iface {

    int statusCount = 0;

    boolean halted = false;

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
    int port = random.nextInt(30000) + 2000;
    var context = new ServerContext(SiteConfiguration.auto());
    final ClientServiceHandler csh =
        new ClientServiceHandler(context, new TransactionWatcher(context));
    final ZombieTServerThriftClientHandler tch = new ZombieTServerThriftClientHandler();

    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(ThriftClientTypes.CLIENT.getServiceName(),
        ThriftProcessorTypes.CLIENT.getTProcessor(ClientService.Processor.class,
            ClientService.Iface.class, csh, context));
    muxProcessor.registerProcessor(ThriftClientTypes.TABLET_SERVER.getServiceName(),
        ThriftProcessorTypes.TABLET_SERVER.getTProcessor(TabletClientService.Processor.class,
            TabletClientService.Iface.class, tch, context));
    muxProcessor.registerProcessor(ThriftProcessorTypes.TABLET_SERVER_SCAN.getServiceName(),
        ThriftProcessorTypes.TABLET_SERVER_SCAN.getTProcessor(
            TabletScanClientService.Processor.class, TabletScanClientService.Iface.class, tch,
            context));

    ServerAddress serverPort =
        TServerUtils.startTServer(context.getConfiguration(), ThriftServerType.CUSTOM_HS_HA,
            muxProcessor, "ZombieTServer", "walking dead", 2, ThreadPools.DEFAULT_TIMEOUT_MILLISECS,
            1000, 10 * 1024 * 1024, null, null, -1, HostAndPort.fromParts("0.0.0.0", port));

    String addressString = serverPort.address.toString();
    var zLockPath =
        ServiceLock.path(context.getZooKeeperRoot() + Constants.ZTSERVERS + "/" + addressString);
    ZooReaderWriter zoo = context.getZooReaderWriter();
    zoo.putPersistentData(zLockPath.toString(), new byte[] {}, NodeExistsPolicy.SKIP);

    ServiceLock zlock = new ServiceLock(zoo.getZooKeeper(), zLockPath, UUID.randomUUID());

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
      public void unableToMonitorLockNode(Exception e) {
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
