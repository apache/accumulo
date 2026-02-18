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
package org.apache.accumulo.manager.fate;

import static org.apache.accumulo.core.util.threads.ThreadPoolNames.IMPORT_TABLE_RENAME_POOL;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.manager.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.manager.EventCoordinator;
import org.apache.accumulo.manager.EventPublisher;
import org.apache.accumulo.manager.EventQueue;
import org.apache.accumulo.manager.split.SplitFileCache;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FateWorkerEnv implements FateEnv {

  private static final Logger log = LoggerFactory.getLogger(FateWorkerEnv.class);

  private final ServerContext ctx;
  private final ExecutorService refreshPool;
  private final ExecutorService renamePool;
  private final ServiceLock serviceLock;
  private final SplitFileCache splitCache;
  private final EventHandler eventHandler;
  private final LiveTServerSet liveTServerSet;

  private final EventQueue queue = new EventQueue();
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final Thread eventSendThread;

  public void stop() {
    stopped.set(true);
    try {
      eventSendThread.join();
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  private class EventSender implements Runnable {
    @Override
    public void run() {
      while (!stopped.get()) {
        try {
          var events = queue.poll(100, TimeUnit.MILLISECONDS);
          if (events.isEmpty()) {
            continue;
          }

          var tEvents = events.stream().map(EventCoordinator.Event::toThrift).toList();

          var client = ThriftClientTypes.MANAGER.getConnection(ctx);
          try {
            if (client != null) {
              client.processEvents(TraceUtil.traceInfo(), ctx.rpcCreds(), tEvents);
            }
          } catch (TException e) {
            log.warn("Failed to send events to manager", e);
          } finally {
            if (client != null) {
              ThriftUtil.close(client, ctx);
            }
          }

        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        }
      }
    }
  }

  private class EventHandler implements EventPublisher {

    @Override
    public void event(String msg, Object... args) {
      log.info(String.format(msg, args));
      queue.add(new EventCoordinator.Event());
    }

    @Override
    public void event(Ample.DataLevel level, String msg, Object... args) {
      log.info(String.format(msg, args));
      queue.add(new EventCoordinator.Event(level));
    }

    @Override
    public void event(TableId tableId, String msg, Object... args) {
      log.info(String.format(msg, args));
      queue.add(new EventCoordinator.Event(tableId));
    }

    @Override
    public void event(KeyExtent extent, String msg, Object... args) {
      log.debug(String.format(msg, args));
      queue.add(new EventCoordinator.Event(extent));
    }

    @Override
    public void event(Collection<KeyExtent> extents, String msg, Object... args) {
      if (!extents.isEmpty()) {
        log.debug(String.format(msg, args));
        extents.forEach(extent -> queue.add(new EventCoordinator.Event(extent)));
      }
    }
  }

  FateWorkerEnv(ServerContext ctx, ServiceLock lock, LiveTServerSet liveTserverSet) {
    this.ctx = ctx;
    this.refreshPool = ThreadPools.getServerThreadPools().getPoolBuilder("Tablet refresh ")
        .numCoreThreads(ctx.getConfiguration().getCount(Property.MANAGER_TABLET_REFRESH_MINTHREADS))
        .numMaxThreads(ctx.getConfiguration().getCount(Property.MANAGER_TABLET_REFRESH_MAXTHREADS))
        .build();
    int poolSize = ctx.getConfiguration().getCount(Property.MANAGER_RENAME_THREADS);
    // FOLLOW_ON this import table name is not correct for the thread pool name, fix in stand alone
    // PR
    this.renamePool = ThreadPools.getServerThreadPools()
        .getPoolBuilder(IMPORT_TABLE_RENAME_POOL.poolName).numCoreThreads(poolSize).build();
    this.serviceLock = lock;
    this.splitCache = new SplitFileCache(ctx);
    this.eventHandler = new EventHandler();
    this.liveTServerSet = liveTserverSet;

    eventSendThread = Threads.createCriticalThread("Fate Worker Event Sender", new EventSender());
    eventSendThread.start();
  }

  @Override
  public ServerContext getContext() {
    return ctx;
  }

  @Override
  public EventPublisher getEventPublisher() {
    return eventHandler;
  }

  @Override
  public void recordCompactionCompletion(ExternalCompactionId ecid) {
    // FOLLOW_ON This data is stored in memory on the manager. This entire feature needs to be
    // examined and potentially reworked. One solution would be to send an RPC to the manager to
    // update it's in memory state. A better solution would be to move away from in memory state
    // that is lost when the manager restarts.
  }

  @Override
  public Set<TServerInstance> onlineTabletServers() {
    return liveTServerSet.getSnapshot().getTservers();
  }

  @Override
  public TableManager getTableManager() {
    return ctx.getTableManager();
  }

  @Override
  public VolumeManager getVolumeManager() {
    return ctx.getVolumeManager();
  }

  @Override
  public void updateBulkImportStatus(String string, BulkImportState bulkImportState) {
    // FOLLOW_ON This data is stored in memory on the manager. This entire feature needs to be
    // examined and potentially reworked. One solution would be to send an RPC to the manager to
    // update it's in memory state. A better solution would be to move away from in memory state
    // that is lost when the manager restarts.
  }

  @Override
  public void removeBulkImportStatus(String sourceDir) {
    // FOLLOW_ON
  }

  @Override
  public ServiceLock getServiceLock() {
    return serviceLock;
  }

  @Override
  public SteadyTime getSteadyTime() {
    try {
      return SteadyTime.from(ctx.instanceOperations().getManagerTime());
    } catch (AccumuloException e) {
      // TODO exceptions, add to to method signature or use a diff type??
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
    // return ctx.get
  }

  @Override
  public ExecutorService getTabletRefreshThreadPool() {
    return refreshPool;
  }

  @Override
  public SplitFileCache getSplitFileCache() {
    return splitCache;
  }

  @Override
  public ExecutorService getRenamePool() {
    return renamePool;
  }
}
