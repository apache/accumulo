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
package org.apache.accumulo.server.manager;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy.SKIP;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TUnloadTabletGoal;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiveTServerSet implements Watcher {

  public interface Listener {
    void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added);
  }

  private static final Logger log = LoggerFactory.getLogger(LiveTServerSet.class);

  private final Listener cback;
  private final ServerContext context;
  private ZooCache zooCache;

  public class TServerConnection {
    private final HostAndPort address;

    public TServerConnection(HostAndPort addr) {
      address = addr;
    }

    public HostAndPort getAddress() {
      return address;
    }

    private String lockString(ServiceLock mlock) {
      return mlock.getLockID().serialize(context.getZooKeeperRoot() + Constants.ZMANAGER_LOCK);
    }

    private void loadTablet(TabletClientService.Client client, ServiceLock lock, KeyExtent extent)
        throws TException {
      client.loadTablet(TraceUtil.traceInfo(), context.rpcCreds(), lockString(lock),
          extent.toThrift());
    }

    public void assignTablet(ServiceLock lock, KeyExtent extent) throws TException {
      if (extent.isMeta()) {
        // see ACCUMULO-3597
        try (TTransport transport = ThriftUtil.createTransport(address, context)) {
          TabletClientService.Client client =
              ThriftUtil.createClient(ThriftClientTypes.TABLET_SERVER, transport);
          loadTablet(client, lock, extent);
        }
      } else {
        TabletClientService.Client client =
            ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, address, context);
        try {
          loadTablet(client, lock, extent);
        } finally {
          ThriftUtil.returnClient(client, context);
        }
      }
    }

    public void unloadTablet(ServiceLock lock, KeyExtent extent, TUnloadTabletGoal goal,
        long requestTime) throws TException {
      TabletClientService.Client client =
          ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, address, context);
      try {
        client.unloadTablet(TraceUtil.traceInfo(), context.rpcCreds(), lockString(lock),
            extent.toThrift(), goal, requestTime);
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    }

    public TabletServerStatus getTableMap(boolean usePooledConnection)
        throws TException, ThriftSecurityException {

      if (usePooledConnection) {
        throw new UnsupportedOperationException();
      }

      long start = System.currentTimeMillis();

      try (TTransport transport = ThriftUtil.createTransport(address, context)) {
        TabletClientService.Client client =
            ThriftUtil.createClient(ThriftClientTypes.TABLET_SERVER, transport);
        TabletServerStatus status =
            client.getTabletServerStatus(TraceUtil.traceInfo(), context.rpcCreds());
        if (status != null) {
          status.setResponseTime(System.currentTimeMillis() - start);
        }
        return status;
      }
    }

    public void halt(ServiceLock lock) throws TException, ThriftSecurityException {
      TabletClientService.Client client =
          ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, address, context);
      try {
        client.halt(TraceUtil.traceInfo(), context.rpcCreds(), lockString(lock));
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    }

    public void fastHalt(ServiceLock lock) throws TException {
      TabletClientService.Client client =
          ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, address, context);
      try {
        client.fastHalt(TraceUtil.traceInfo(), context.rpcCreds(), lockString(lock));
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    }

    public void flush(ServiceLock lock, TableId tableId, byte[] startRow, byte[] endRow)
        throws TException {
      TabletClientService.Client client =
          ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, address, context);
      try {
        client.flush(TraceUtil.traceInfo(), context.rpcCreds(), lockString(lock),
            tableId.canonical(), startRow == null ? null : ByteBuffer.wrap(startRow),
            endRow == null ? null : ByteBuffer.wrap(endRow));
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    }

    public void chop(ServiceLock lock, KeyExtent extent) throws TException {
      TabletClientService.Client client =
          ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, address, context);
      try {
        client.chop(TraceUtil.traceInfo(), context.rpcCreds(), lockString(lock), extent.toThrift());
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    }

    public void splitTablet(KeyExtent extent, Text splitPoint)
        throws TException, ThriftSecurityException, NotServingTabletException {
      TabletClientService.Client client =
          ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, address, context);
      try {
        client.splitTablet(TraceUtil.traceInfo(), context.rpcCreds(), extent.toThrift(),
            ByteBuffer.wrap(splitPoint.getBytes(), 0, splitPoint.getLength()));
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    }

    public void compact(ServiceLock lock, String tableId, byte[] startRow, byte[] endRow)
        throws TException {
      TabletClientService.Client client =
          ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, address, context);
      try {
        client.compact(TraceUtil.traceInfo(), context.rpcCreds(), lockString(lock), tableId,
            startRow == null ? null : ByteBuffer.wrap(startRow),
            endRow == null ? null : ByteBuffer.wrap(endRow));
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    }

    public boolean isActive(long tid) throws TException {
      ClientService.Client client =
          ThriftUtil.getClient(ThriftClientTypes.CLIENT, address, context);
      try {
        return client.isActive(TraceUtil.traceInfo(), tid);
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    }

  }

  static class TServerInfo {
    TServerConnection connection;
    TServerInstance instance;

    TServerInfo(TServerInstance instance, TServerConnection connection) {
      this.connection = connection;
      this.instance = instance;
    }
  }

  // The set of active tservers with locks, indexed by their name in zookeeper
  private Map<String,TServerInfo> current = new HashMap<>();
  // as above, indexed by TServerInstance
  private Map<TServerInstance,TServerInfo> currentInstances = new HashMap<>();

  // The set of entries in zookeeper without locks, and the first time each was noticed
  private Map<String,Long> locklessServers = new HashMap<>();

  public LiveTServerSet(ServerContext context, Listener cback) {
    this.cback = cback;
    this.context = context;
  }

  public synchronized ZooCache getZooCache() {
    if (zooCache == null) {
      zooCache = new ZooCache(context.getZooReader(), this);
    }
    return zooCache;
  }

  public synchronized void startListeningForTabletServerChanges() {
    scanServers();

    ThreadPools.watchCriticalScheduledTask(this.context.getScheduledExecutor()
        .scheduleWithFixedDelay(this::scanServers, 0, 5000, TimeUnit.MILLISECONDS));
  }

  public synchronized void scanServers() {
    try {
      final Set<TServerInstance> updates = new HashSet<>();
      final Set<TServerInstance> doomed = new HashSet<>();

      final String path = context.getZooKeeperRoot() + Constants.ZTSERVERS;

      HashSet<String> all = new HashSet<>(current.keySet());
      all.addAll(getZooCache().getChildren(path));

      locklessServers.keySet().retainAll(all);

      for (String zPath : all) {
        checkServer(updates, doomed, path, zPath);
      }

      // log.debug("Current: " + current.keySet());
      this.cback.update(this, doomed, updates);
    } catch (Exception ex) {
      log.error("{}", ex.getMessage(), ex);
    }
  }

  private void deleteServerNode(String serverNode) throws InterruptedException, KeeperException {
    try {
      context.getZooReaderWriter().delete(serverNode);
    } catch (NotEmptyException ex) {
      // acceptable race condition:
      // tserver created the lock under this server's node after our last check
      // we'll see it at the next check
    }
  }

  private synchronized void checkServer(final Set<TServerInstance> updates,
      final Set<TServerInstance> doomed, final String path, final String zPath)
      throws InterruptedException, KeeperException {

    TServerInfo info = current.get(zPath);

    final var zLockPath = ServiceLock.path(path + "/" + zPath);
    ZcStat stat = new ZcStat();
    byte[] lockData = ServiceLock.getLockData(getZooCache(), zLockPath, stat);

    if (lockData == null) {
      if (info != null) {
        doomed.add(info.instance);
        current.remove(zPath);
        currentInstances.remove(info.instance);
      }

      Long firstSeen = locklessServers.get(zPath);
      if (firstSeen == null) {
        locklessServers.put(zPath, System.currentTimeMillis());
      } else if (System.currentTimeMillis() - firstSeen > MINUTES.toMillis(10)) {
        deleteServerNode(path + "/" + zPath);
        locklessServers.remove(zPath);
      }
    } else {
      locklessServers.remove(zPath);
      ServerServices services = new ServerServices(new String(lockData, UTF_8));
      HostAndPort client = services.getAddress(ServerServices.Service.TSERV_CLIENT);
      TServerInstance instance = new TServerInstance(client, stat.getEphemeralOwner());

      if (info == null) {
        updates.add(instance);
        TServerInfo tServerInfo = new TServerInfo(instance, new TServerConnection(client));
        current.put(zPath, tServerInfo);
        currentInstances.put(instance, tServerInfo);
      } else if (!info.instance.equals(instance)) {
        doomed.add(info.instance);
        updates.add(instance);
        TServerInfo tServerInfo = new TServerInfo(instance, new TServerConnection(client));
        current.put(zPath, tServerInfo);
        currentInstances.remove(info.instance);
        currentInstances.put(instance, tServerInfo);
      }
    }
  }

  @Override
  public void process(WatchedEvent event) {

    // its important that these event are propagated by ZooCache, because this ensures when reading
    // zoocache that is has already processed the event and cleared
    // relevant nodes before code below reads from zoocache

    if (event.getPath() != null) {
      if (event.getPath().endsWith(Constants.ZTSERVERS)) {
        scanServers();
      } else if (event.getPath().contains(Constants.ZTSERVERS)) {
        int pos = event.getPath().lastIndexOf('/');

        // do only if ZTSERVER is parent
        if (pos >= 0 && event.getPath().substring(0, pos).endsWith(Constants.ZTSERVERS)) {

          String server = event.getPath().substring(pos + 1);

          final Set<TServerInstance> updates = new HashSet<>();
          final Set<TServerInstance> doomed = new HashSet<>();

          final String path = context.getZooKeeperRoot() + Constants.ZTSERVERS;

          try {
            checkServer(updates, doomed, path, server);
            this.cback.update(this, doomed, updates);
          } catch (Exception ex) {
            log.error("Exception", ex);
          }
        }
      }
    }
  }

  public synchronized TServerConnection getConnection(TServerInstance server) {
    if (server == null) {
      return null;
    }
    TServerInfo tServerInfo = currentInstances.get(server);
    if (tServerInfo == null) {
      return null;
    }
    return tServerInfo.connection;
  }

  public synchronized Set<TServerInstance> getCurrentServers() {
    return new HashSet<>(currentInstances.keySet());
  }

  public synchronized int size() {
    return current.size();
  }

  public synchronized TServerInstance find(String tabletServer) {
    return find(current, tabletServer);
  }

  TServerInstance find(Map<String,TServerInfo> servers, String tabletServer) {
    HostAndPort addr;
    String sessionId = null;
    if (tabletServer.charAt(tabletServer.length() - 1) == ']') {
      int index = tabletServer.indexOf('[');
      if (index == -1) {
        throw new IllegalArgumentException("Could not parse tabletserver '" + tabletServer + "'");
      }
      addr = AddressUtil.parseAddress(tabletServer.substring(0, index), false);
      // Strip off the last bracket
      sessionId = tabletServer.substring(index + 1, tabletServer.length() - 1);
    } else {
      addr = AddressUtil.parseAddress(tabletServer, false);
    }
    for (Entry<String,TServerInfo> entry : servers.entrySet()) {
      if (entry.getValue().instance.getHostAndPort().equals(addr)) {
        // Return the instance if we have no desired session ID, or we match the desired session ID
        if (sessionId == null || sessionId.equals(entry.getValue().instance.getSession())) {
          return entry.getValue().instance;
        }
      }
    }
    return null;
  }

  public synchronized void remove(TServerInstance server) {
    String zPath = null;
    for (Entry<String,TServerInfo> entry : current.entrySet()) {
      if (entry.getValue().instance.equals(server)) {
        zPath = entry.getKey();
        break;
      }
    }
    if (zPath == null) {
      return;
    }
    current.remove(zPath);
    currentInstances.remove(server);

    log.info("Removing zookeeper lock for {}", server);
    String fullpath = context.getZooKeeperRoot() + Constants.ZTSERVERS + "/" + zPath;
    try {
      context.getZooReaderWriter().recursiveDelete(fullpath, SKIP);
    } catch (Exception e) {
      String msg = "error removing tablet server lock";
      // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
      log.error("FATAL: {}", msg, e);
      Halt.halt(msg, -1);
    }
    getZooCache().clear(fullpath);
  }
}
