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
package org.apache.accumulo.server.master;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy.SKIP;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TUnloadTabletGoal;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiveTServerSet implements Watcher {

  public interface Listener {
    void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added);
  }

  private static final Logger log = LoggerFactory.getLogger(LiveTServerSet.class);

  private final Listener cback;
  private final ClientContext context;
  private ZooCache zooCache;

  public class TServerConnection {
    private final HostAndPort address;

    public TServerConnection(HostAndPort addr) throws TException {
      address = addr;
    }

    private String lockString(ZooLock mlock) {
      return mlock.getLockID().serialize(ZooUtil.getRoot(context.getInstance()) + Constants.ZMASTER_LOCK);
    }

    private void loadTablet(TabletClientService.Client client, ZooLock lock, KeyExtent extent) throws TException {
      client.loadTablet(Tracer.traceInfo(), context.rpcCreds(), lockString(lock), extent.toThrift());
    }

    public void assignTablet(ZooLock lock, KeyExtent extent) throws TException {
      if (extent.isMeta()) {
        // see ACCUMULO-3597
        TTransport transport = ThriftUtil.createTransport(address, context);
        try {
          TabletClientService.Client client = ThriftUtil.createClient(new TabletClientService.Client.Factory(), transport);
          loadTablet(client, lock, extent);
        } finally {
          transport.close();
        }
      } else {
        TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, context);
        try {
          loadTablet(client, lock, extent);
        } finally {
          ThriftUtil.returnClient(client);
        }
      }
    }

    public void unloadTablet(ZooLock lock, KeyExtent extent, TUnloadTabletGoal goal, long requestTime) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, context);
      try {
        client.unloadTablet(Tracer.traceInfo(), context.rpcCreds(), lockString(lock), extent.toThrift(), goal, requestTime);
      } finally {
        ThriftUtil.returnClient(client);
      }
    }

    public TabletServerStatus getTableMap(boolean usePooledConnection) throws TException, ThriftSecurityException {

      if (usePooledConnection == true)
        throw new UnsupportedOperationException();

      long start = System.currentTimeMillis();

      TTransport transport = ThriftUtil.createTransport(address, context);

      try {
        TabletClientService.Client client = ThriftUtil.createClient(new TabletClientService.Client.Factory(), transport);
        TabletServerStatus status = client.getTabletServerStatus(Tracer.traceInfo(), context.rpcCreds());
        if (status != null) {
          status.setResponseTime(System.currentTimeMillis() - start);
        }
        return status;
      } finally {
        if (transport != null)
          transport.close();
      }
    }

    public void halt(ZooLock lock) throws TException, ThriftSecurityException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, context);
      try {
        client.halt(Tracer.traceInfo(), context.rpcCreds(), lockString(lock));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }

    public void fastHalt(ZooLock lock) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, context);
      try {
        client.fastHalt(Tracer.traceInfo(), context.rpcCreds(), lockString(lock));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }

    public void flush(ZooLock lock, Table.ID tableId, byte[] startRow, byte[] endRow) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, context);
      try {
        client.flush(Tracer.traceInfo(), context.rpcCreds(), lockString(lock), tableId.canonicalID(), startRow == null ? null : ByteBuffer.wrap(startRow),
            endRow == null ? null : ByteBuffer.wrap(endRow));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }

    public void chop(ZooLock lock, KeyExtent extent) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, context);
      try {
        client.chop(Tracer.traceInfo(), context.rpcCreds(), lockString(lock), extent.toThrift());
      } finally {
        ThriftUtil.returnClient(client);
      }
    }

    public void splitTablet(ZooLock lock, KeyExtent extent, Text splitPoint) throws TException, ThriftSecurityException, NotServingTabletException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, context);
      try {
        client.splitTablet(Tracer.traceInfo(), context.rpcCreds(), extent.toThrift(), ByteBuffer.wrap(splitPoint.getBytes(), 0, splitPoint.getLength()));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }

    public void compact(ZooLock lock, String tableId, byte[] startRow, byte[] endRow) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, context);
      try {
        client.compact(Tracer.traceInfo(), context.rpcCreds(), lockString(lock), tableId, startRow == null ? null : ByteBuffer.wrap(startRow),
            endRow == null ? null : ByteBuffer.wrap(endRow));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }

    public boolean isActive(long tid) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, context);
      try {
        return client.isActive(Tracer.traceInfo(), tid);
      } finally {
        ThriftUtil.returnClient(client);
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

  public LiveTServerSet(ClientContext context, Listener cback) {
    this.cback = cback;
    this.context = context;
  }

  public synchronized ZooCache getZooCache() {
    if (zooCache == null)
      zooCache = new ZooCache(this);
    return zooCache;
  }

  public synchronized void startListeningForTabletServerChanges() {
    scanServers();
    SimpleTimer.getInstance(context.getConfiguration()).schedule(new Runnable() {
      @Override
      public void run() {
        scanServers();
      }
    }, 0, 5000);
  }

  public synchronized void scanServers() {
    try {
      final Set<TServerInstance> updates = new HashSet<>();
      final Set<TServerInstance> doomed = new HashSet<>();

      final String path = ZooUtil.getRoot(context.getInstance()) + Constants.ZTSERVERS;

      HashSet<String> all = new HashSet<>(current.keySet());
      all.addAll(getZooCache().getChildren(path));

      locklessServers.keySet().retainAll(all);

      for (String zPath : all) {
        checkServer(updates, doomed, path, zPath);
      }

      // log.debug("Current: " + current.keySet());
      if (!doomed.isEmpty() || !updates.isEmpty())
        this.cback.update(this, doomed, updates);
    } catch (Exception ex) {
      log.error("{}", ex.getMessage(), ex);
    }
  }

  private void deleteServerNode(String serverNode) throws InterruptedException, KeeperException {
    try {
      ZooReaderWriter.getInstance().delete(serverNode, -1);
    } catch (NotEmptyException ex) {
      // race condition: tserver created the lock after our last check; we'll see it at the next check
    } catch (NoNodeException nne) {
      // someone else deleted it
    }
  }

  private synchronized void checkServer(final Set<TServerInstance> updates, final Set<TServerInstance> doomed, final String path, final String zPath)
      throws TException, InterruptedException, KeeperException {

    TServerInfo info = current.get(zPath);

    final String lockPath = path + "/" + zPath;
    Stat stat = new Stat();
    byte[] lockData = ZooLock.getLockData(getZooCache(), lockPath, stat);

    if (lockData == null) {
      if (info != null) {
        doomed.add(info.instance);
        current.remove(zPath);
        currentInstances.remove(info.instance);
      }

      Long firstSeen = locklessServers.get(zPath);
      if (firstSeen == null) {
        locklessServers.put(zPath, System.currentTimeMillis());
      } else if (System.currentTimeMillis() - firstSeen > 10 * 60 * 1000) {
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

    // its important that these event are propagated by ZooCache, because this ensures when reading zoocache that is has already processed the event and cleared
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

          final String path = ZooUtil.getRoot(context.getInstance()) + Constants.ZTSERVERS;

          try {
            checkServer(updates, doomed, path, server);
            if (!doomed.isEmpty() || !updates.isEmpty())
              this.cback.update(this, doomed, updates);
          } catch (Exception ex) {
            log.error("Exception", ex);
          }
        }
      }
    }
  }

  public synchronized TServerConnection getConnection(TServerInstance server) {
    if (server == null)
      return null;
    TServerInfo tServerInfo = currentInstances.get(server);
    if (tServerInfo == null)
      return null;
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
    if (']' == tabletServer.charAt(tabletServer.length() - 1)) {
      int index = tabletServer.indexOf('[');
      if (-1 == index) {
        throw new IllegalArgumentException("Could not parse tabletserver '" + tabletServer + "'");
      }
      addr = AddressUtil.parseAddress(tabletServer.substring(0, index), false);
      // Strip off the last bracket
      sessionId = tabletServer.substring(index + 1, tabletServer.length() - 1);
    } else {
      addr = AddressUtil.parseAddress(tabletServer, false);
    }
    for (Entry<String,TServerInfo> entry : servers.entrySet()) {
      if (entry.getValue().instance.getLocation().equals(addr)) {
        // Return the instance if we have no desired session ID, or we match the desired session ID
        if (null == sessionId || sessionId.equals(entry.getValue().instance.getSession()))
          return entry.getValue().instance;
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
    if (zPath == null)
      return;
    current.remove(zPath);
    currentInstances.remove(server);

    log.info("Removing zookeeper lock for {}", server);
    String fullpath = ZooUtil.getRoot(context.getInstance()) + Constants.ZTSERVERS + "/" + zPath;
    try {
      ZooReaderWriter.getInstance().recursiveDelete(fullpath, SKIP);
    } catch (Exception e) {
      String msg = "error removing tablet server lock";
      // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
      log.error("FATAL: {}", msg, e);
      Halt.halt(msg, -1);
    }
    getZooCache().clear(fullpath);
  }
}
