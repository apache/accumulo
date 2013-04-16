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

import static org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy.SKIP;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.util.AddressUtil;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public class LiveTServerSet implements Watcher {
  
  public interface Listener {
    void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added);
  }
  
  private static final Logger log = Logger.getLogger(LiveTServerSet.class);
  
  private final Listener cback;
  private final Instance instance;
  private ZooCache zooCache;
  
  public class TServerConnection {
    private final InetSocketAddress address;
    
    public TServerConnection(InetSocketAddress addr) throws TException {
      address = addr;
    }
    
    private String lockString(ZooLock mlock) {
      return mlock.getLockID().serialize(ZooUtil.getRoot(instance) + Constants.ZMASTER_LOCK);
    }
    
    public void assignTablet(ZooLock lock, KeyExtent extent) throws TException {
      TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
      try {
        client.loadTablet(null, SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift());
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void unloadTablet(ZooLock lock, KeyExtent extent, boolean save) throws TException {
      TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
      try {
        client.unloadTablet(null, SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift(), save);
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public TabletServerStatus getTableMap() throws TException, ThriftSecurityException {
      TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
      try {
        return client.getTabletServerStatus(null, SecurityConstants.getSystemCredentials());
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void halt(ZooLock lock) throws TException, ThriftSecurityException {
      TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
      try {
        client.halt(null, SecurityConstants.getSystemCredentials(), lockString(lock));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void fastHalt(ZooLock lock) throws TException {
      TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
      try {
        client.fastHalt(null, SecurityConstants.getSystemCredentials(), lockString(lock));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void flush(ZooLock lock, String tableId, byte[] startRow, byte[] endRow) throws TException {
      TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
      try {
        client.flush(null, SecurityConstants.getSystemCredentials(), lockString(lock), tableId, startRow == null ? null : ByteBuffer.wrap(startRow),
            endRow == null ? null : ByteBuffer.wrap(endRow));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void useLoggers(Set<String> loggers) throws TException {
      TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
      try {
        client.useLoggers(null, SecurityConstants.getSystemCredentials(), loggers);
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void chop(ZooLock lock, KeyExtent extent) throws TException {
      TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
      try {
        client.chop(null, SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift());
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void splitTablet(ZooLock lock, KeyExtent extent, Text splitPoint) throws TException, ThriftSecurityException, NotServingTabletException {
      TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
      try {
        client
            .splitTablet(null, SecurityConstants.getSystemCredentials(), extent.toThrift(), ByteBuffer.wrap(splitPoint.getBytes(), 0, splitPoint.getLength()));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void flushTablet(ZooLock lock, KeyExtent extent) throws TException {
      TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
      try {
        client.flushTablet(null, SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift());
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void compact(ZooLock lock, String tableId, byte[] startRow, byte[] endRow) throws TException {
      TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
      try {
        client.compact(null, SecurityConstants.getSystemCredentials(), lockString(lock), tableId, startRow == null ? null : ByteBuffer.wrap(startRow),
            endRow == null ? null : ByteBuffer.wrap(endRow));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public boolean isActive(long tid) throws TException {
      TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
      try {
        return client.isActive(null, tid);
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
  };
  
  // Map from tserver master service to server information
  private Map<String,TServerInfo> current = new HashMap<String,TServerInfo>();
  private Map<String,Long> locklessServers = new HashMap<String,Long>();
  
  public LiveTServerSet(Instance instance, Listener cback) {
    this.cback = cback;
    this.instance = instance;
    
  }
  
  public synchronized ZooCache getZooCache() {
    if (zooCache == null)
      zooCache = new ZooCache(this);
    return zooCache;
  }
  
  public synchronized void startListeningForTabletServerChanges() {
    scanServers();
    SimpleTimer.getInstance().schedule(new TimerTask() {
      @Override
      public void run() {
        scanServers();
      }
    }, 0, 5000);
  }
  
  public synchronized void scanServers() {
    try {
      final Set<TServerInstance> updates = new HashSet<TServerInstance>();
      final Set<TServerInstance> doomed = new HashSet<TServerInstance>();
      
      final String path = ZooUtil.getRoot(instance) + Constants.ZTSERVERS;
      
      HashSet<String> all = new HashSet<String>(current.keySet());
      all.addAll(getZooCache().getChildren(path));
      
      locklessServers.keySet().retainAll(all);

      for (String server : all) {
        checkServer(updates, doomed, path, server);
      }

      // log.debug("Current: " + current.keySet());
      if (!doomed.isEmpty() || !updates.isEmpty())
        this.cback.update(this, doomed, updates);
    } catch (Exception ex) {
      log.error(ex, ex);
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
  
  private synchronized void checkServer(final Set<TServerInstance> updates, final Set<TServerInstance> doomed, final String path, final String server)
      throws TException, InterruptedException, KeeperException {

    TServerInfo info = current.get(server);
    
    final String lockPath = path + "/" + server;
    Stat stat = new Stat();
    byte[] lockData = ZooLock.getLockData(getZooCache(), lockPath, stat);

    if (lockData == null) {
      if (info != null) {
        doomed.add(info.instance);
        current.remove(server);
      }
      
      Long firstSeen = locklessServers.get(server);
      if (firstSeen == null) {
        locklessServers.put(server, System.currentTimeMillis());
      } else if (System.currentTimeMillis() - firstSeen > 600000) {
        deleteServerNode(path + "/" + server);
        locklessServers.remove(server);
      }
    } else {
      locklessServers.remove(server);
      ServerServices services = new ServerServices(new String(lockData));
      InetSocketAddress client = services.getAddress(ServerServices.Service.TSERV_CLIENT);
      InetSocketAddress addr = AddressUtil.parseAddress(server, Property.TSERV_CLIENTPORT);
      TServerInstance instance = new TServerInstance(client, stat.getEphemeralOwner());
      
      if (info == null) {
        updates.add(instance);
        current.put(server, new TServerInfo(instance, new TServerConnection(addr)));
      } else if (!info.instance.equals(instance)) {
        doomed.add(info.instance);
        updates.add(instance);
        current.put(server, new TServerInfo(instance, new TServerConnection(addr)));
      }
    }
  }

  @Override
  public void process(WatchedEvent event) {

    // its imporant that these event are propogated by ZooCache, because this ensures when reading zoocache that is has already processed the event and cleared
    // relevant nodes before code below reads from zoocache

    if (event.getPath() != null) {
      if (event.getPath().endsWith(Constants.ZTSERVERS)) {
        scanServers();
      } else if (event.getPath().contains(Constants.ZTSERVERS)) {
        int pos = event.getPath().lastIndexOf('/');

        // do only if ZTSERVER is parent
        if (pos >= 0 && event.getPath().substring(0, pos).endsWith(Constants.ZTSERVERS)) {
          
          String server = event.getPath().substring(pos + 1);
          
          final Set<TServerInstance> updates = new HashSet<TServerInstance>();
          final Set<TServerInstance> doomed = new HashSet<TServerInstance>();
          
          final String path = ZooUtil.getRoot(instance) + Constants.ZTSERVERS;

          try {
            checkServer(updates, doomed, path, server);
            if (!doomed.isEmpty() || !updates.isEmpty())
              this.cback.update(this, doomed, updates);
          } catch (Exception ex) {
            log.error(ex, ex);
          }
        }
      }
    }
  }
  
  public synchronized TServerConnection getConnection(TServerInstance server) throws TException {
    TServerConnection result;
    synchronized (this) {
      if (server == null)
        return null;
      TServerInfo serverInfo = current.get(server.hostPort());
      // lock was lost?
      if (serverInfo == null)
        return null;
      // instance changed?
      if (!serverInfo.instance.equals(server))
        return null;
      result = serverInfo.connection;
    }
    return result;
  }
  
  public synchronized Set<TServerInstance> getCurrentServers() {
    HashSet<TServerInstance> result = new HashSet<TServerInstance>();
    for (TServerInfo c : current.values()) {
      result.add(c.instance);
    }
    return result;
  }
  
  public synchronized int size() {
    return current.size();
  }
  
  public synchronized TServerInstance find(String serverName) {
    TServerInfo serverInfo = current.get(serverName);
    if (serverInfo != null) {
      return serverInfo.instance;
    }
    return null;
  }
  
  public synchronized boolean isOnline(String serverName) {
    return current.containsKey(serverName);
  }
  
  public synchronized void remove(TServerInstance server) {
    current.remove(server.hostPort());

    log.info("Removing zookeeper lock for " + server);
    String zpath = ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/" + server.hostPort();
    try {
      ZooReaderWriter.getRetryingInstance().recursiveDelete(zpath, SKIP);
    } catch (Exception e) {
      String msg = "error removing tablet server lock";
      log.fatal(msg, e);
      Halt.halt(msg, -1);
    }
    getZooCache().clear(zpath);
  }
}
