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

import static org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy.SKIP;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;

import org.apache.accumulo.cloudtrace.instrument.Tracer;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
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
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

public class LiveTServerSet implements Watcher {
  
  public interface Listener {
    void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added);
  }
  
  private static final Logger log = Logger.getLogger(LiveTServerSet.class);
  
  private final Listener cback;
  private final Instance instance;
  private final AccumuloConfiguration conf;
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
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.loadTablet(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift());
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void unloadTablet(ZooLock lock, KeyExtent extent, boolean save) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.unloadTablet(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift(), save);
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public TabletServerStatus getTableMap() throws TException, ThriftSecurityException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        return client.getTabletServerStatus(Tracer.traceInfo(), SecurityConstants.getSystemCredentials());
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void halt(ZooLock lock) throws TException, ThriftSecurityException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.halt(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void fastHalt(ZooLock lock) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.fastHalt(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void flush(ZooLock lock, String tableId, byte[] startRow, byte[] endRow) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.flush(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock), tableId, startRow == null ? null : ByteBuffer.wrap(startRow),
            endRow == null ? null : ByteBuffer.wrap(endRow));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void chop(ZooLock lock, KeyExtent extent) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.chop(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift());
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void splitTablet(ZooLock lock, KeyExtent extent, Text splitPoint) throws TException, ThriftSecurityException, NotServingTabletException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client
            .splitTablet(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), extent.toThrift(), ByteBuffer.wrap(splitPoint.getBytes(), 0, splitPoint.getLength()));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void flushTablet(ZooLock lock, KeyExtent extent) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.flushTablet(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift());
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void compact(ZooLock lock, String tableId, byte[] startRow, byte[] endRow) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.compact(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock), tableId, startRow == null ? null : ByteBuffer.wrap(startRow),
            endRow == null ? null : ByteBuffer.wrap(endRow));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public boolean isActive(long tid) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        return client.isActive(Tracer.traceInfo(), tid);
      } finally {
        ThriftUtil.returnClient(client);
      }
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
    }
  };
  
  // Map from tserver master service to server information
  private Map<String,TServerInfo> current = new HashMap<String,TServerInfo>();
  private HashMap<String,Long> serversToDelete = new HashMap<String,Long>();

  public LiveTServerSet(Instance instance, AccumuloConfiguration conf, Listener cback) {
    this.cback = cback;
    this.instance = instance;
    this.conf = conf;
    
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
      
      Iterator<Entry<String,Long>> serversToDelIter = serversToDelete.entrySet().iterator();
      while (serversToDelIter.hasNext()) {
        Entry<String,Long> entry = serversToDelIter.next();
        if (System.currentTimeMillis() - entry.getValue() > 10000) {
          String serverNode = path + "/" + entry.getKey();
          serversToDelIter.remove();
          deleteServerNode(serverNode);
        }
      }

      for (String server : getZooCache().getChildren(path)) {
        if (serversToDelete.containsKey(server))
          continue;

        checkServer(updates, doomed, path, server, 2);
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
  
  private synchronized void checkServer(final Set<TServerInstance> updates, final Set<TServerInstance> doomed, final String path, final String server,
      int recurse)
      throws TException,
      InterruptedException, KeeperException {
    
    if (recurse == 0)
      return;

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
        // a server existed here and went away so delete its node
        doomed.add(info.instance);
        current.remove(server);
        info.cleanup();
        deleteServerNode(lockPath);
      } else {
        // never knew of this server before... it could be a new server that has not created its lock node yet... watch and see if it creates the node or
        // delete it later if it does not
        List<String> children = ZooReaderWriter.getInstance().getChildren(lockPath, new Watcher() {
          @Override
          public void process(WatchedEvent arg0) {
            if (arg0.getType() == EventType.NodeChildrenChanged) {
              Set<TServerInstance> updates = new HashSet<TServerInstance>();
              Set<TServerInstance> doomed = new HashSet<TServerInstance>();
              try {
                checkServer(updates, doomed, path, server, 2);
              } catch (Exception ex) {
                log.error(ex, ex);
              }

              if (!doomed.isEmpty() || !updates.isEmpty())
                cback.update(LiveTServerSet.this, doomed, updates);
            }
          }
        });
        
        if (children.size() > 0) {
          checkServer(updates, doomed, path, server, recurse--);
        } else {
          if (!serversToDelete.containsKey(server))
            serversToDelete.put(server, System.currentTimeMillis());
        }
      }
    }
  }
  
  @Override
  public void process(WatchedEvent event) {
    scanServers();
  }
  
  public synchronized TServerConnection getConnection(TServerInstance server) throws TException {
    if (server == null)
      return null;
    TServerInfo serverInfo = current.get(server.hostPort());
    // lock was lost?
    if (serverInfo == null)
      return null;
    // instance changed?
    if (!serverInfo.instance.equals(server))
      return null;
    TServerConnection result = serverInfo.connection;
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
