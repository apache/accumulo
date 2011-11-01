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
    
    TabletClientService.Iface client = null;
    
    public TServerConnection(InetSocketAddress addr) throws TException {
      address = addr;
    }
    
    synchronized private TabletClientService.Iface connect() {
      if (client == null) {
        try {
          client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, ServerConfiguration.getSystemConfiguration());
        } catch (Exception ex) {
          client = null;
          log.error(ex, ex);
        }
      }
      return client;
    }
    
    private String lockString(ZooLock mlock) {
      return mlock.getLockID().serialize(ZooUtil.getRoot(instance) + Constants.ZMASTER_LOCK);
    }
    
    synchronized public void close() {
      if (client != null) {
        ThriftUtil.returnClient(client);
        client = null;
      }
    }
    
    synchronized public void assignTablet(ZooLock lock, KeyExtent extent) throws TException {
      connect().loadTablet(null, SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift());
    }
    
    synchronized public void unloadTablet(ZooLock lock, KeyExtent extent, boolean save) throws TException {
      connect().unloadTablet(null, SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift(), save);
    }
    
    synchronized public TabletServerStatus getTableMap() throws TException, ThriftSecurityException {
      return connect().getTabletServerStatus(null, SecurityConstants.getSystemCredentials());
    }
    
    synchronized public void halt(ZooLock lock) throws TException, ThriftSecurityException {
      if (client != null) {
        client.halt(null, SecurityConstants.getSystemCredentials(), lockString(lock));
      }
    }
    
    public void fastHalt(ZooLock lock) throws TException {
      if (client != null) {
        client.fastHalt(null, SecurityConstants.getSystemCredentials(), lockString(lock));
      }
    }
    
    synchronized public void flush(ZooLock lock, String tableId, byte[] startRow, byte[] endRow) throws TException {
      connect().flush(null, SecurityConstants.getSystemCredentials(), lockString(lock), tableId, startRow == null ? null : ByteBuffer.wrap(startRow),
          endRow == null ? null : ByteBuffer.wrap(endRow));
    }
    
    synchronized public void useLoggers(Set<String> loggers) throws TException {
      connect().useLoggers(null, SecurityConstants.getSystemCredentials(), loggers);
    }
    
    synchronized public void chop(ZooLock lock, KeyExtent extent) throws TException {
      connect().chop(null, SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift());
    }
    
    synchronized public void splitTablet(ZooLock lock, KeyExtent extent, Text splitPoint) throws TException, ThriftSecurityException, NotServingTabletException {
      connect().splitTablet(null, SecurityConstants.getSystemCredentials(), extent.toThrift(), ByteBuffer.wrap(splitPoint.getBytes(), 0, splitPoint.getLength()));
    }
    
    synchronized public void flushTablet(ZooLock lock, KeyExtent extent) throws TException {
      connect().flushTablet(null, SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift());
    }
    
    synchronized public void compact(ZooLock lock, String tableId, byte[] startRow, byte[] endRow) throws TException {
      connect().compact(null, SecurityConstants.getSystemCredentials(), lockString(lock), tableId, startRow == null ? null : ByteBuffer.wrap(startRow),
          endRow == null ? null : ByteBuffer.wrap(endRow));
    }
    
    synchronized public boolean isActive(long tid) throws TException {
      return connect().isActive(null, tid);
    }
    
  }
  
  static class TServerInfo {
    ZooLock lock;
    TServerConnection connection;
    TServerInstance instance;
    TServerLockWatcher watcher;
    
    TServerInfo(ZooLock lock, TServerInstance instance, TServerConnection connection, TServerLockWatcher watcher) {
      this.lock = lock;
      this.connection = connection;
      this.instance = instance;
      this.watcher = watcher;
    }
    
    void cleanup() throws InterruptedException, KeeperException {
      lock.tryToCancelAsyncLockOrUnlock();
      connection.close();
    }
  };
  
  // Map from tserver master service to server information
  private Map<String,TServerInfo> current = new HashMap<String,TServerInfo>();
  
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
    }, 0, 1000);
  }
  
  public synchronized void scanServers() {
    try {
      final Set<TServerInstance> updates = new HashSet<TServerInstance>();
      final Set<TServerInstance> doomed = new HashSet<TServerInstance>();
      
      final String path = ZooUtil.getRoot(instance) + Constants.ZTSERVERS;
      for (String server : getZooCache().getChildren(path)) {
        // See if we have an async lock in place?
        TServerInfo info = current.get(server);
        TServerLockWatcher watcher;
        ZooLock lock;
        final String lockPath = path + "/" + server;
        if (info != null) {
          // yep: get out the lock/watcher so we can check on it
          watcher = info.watcher;
          lock = info.lock;
        } else {
          // nope: create a new lock and watcher
          lock = new ZooLock(lockPath);
          watcher = new TServerLockWatcher();
          lock.lockAsync(watcher, "master".getBytes());
        }
        TServerInstance instance = null;
        // Did we win the lock yet?
        if (!lock.isLocked() && !watcher.gotLock && watcher.failureException == null) {
          // Nope... there's a server out there: is this is a new server?
          if (info == null) {
            // Yep: hold onto the information about this server
            Stat stat = new Stat();
            byte[] lockData = ZooLock.getLockData(lockPath, stat);
            String lockString = new String(lockData == null ? new byte[] {} : lockData);
            if (lockString.length() > 0 && !lockString.equals("master")) {
              ServerServices services = new ServerServices(new String(lockData));
              InetSocketAddress client = services.getAddress(ServerServices.Service.TSERV_CLIENT);
              InetSocketAddress addr = AddressUtil.parseAddress(server, Property.TSERV_CLIENTPORT);
              TServerConnection conn = new TServerConnection(addr);
              instance = new TServerInstance(client, stat.getEphemeralOwner());
              info = new TServerInfo(lock, instance, conn, watcher);
              current.put(server, info);
              updates.add(instance);
            } else {
              lock.tryToCancelAsyncLockOrUnlock();
            }
          }
        } else {
          // Yes... there is no server here any more
          lock.tryToCancelAsyncLockOrUnlock();
          if (info != null) {
            doomed.add(info.instance);
            current.remove(server);
            info.cleanup();
          }
        }
      }
      // log.debug("Current: " + current.keySet());
      if (!doomed.isEmpty() && !updates.isEmpty())
        this.cback.update(this, doomed, updates);
    } catch (Exception ex) {
      log.error(ex, ex);
    }
  }
  
  @Override
  public void process(WatchedEvent event) {
    scanServers();
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
    TServerInfo remove = current.remove(server.hostPort());
    if (remove != null) {
      try {
        remove.cleanup();
      } catch (Exception e) {
        log.info("error cleaning up connection to server", e);
      }
    }
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
