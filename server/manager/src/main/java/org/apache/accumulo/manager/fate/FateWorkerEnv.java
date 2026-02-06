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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.manager.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.manager.EventPublisher;
import org.apache.accumulo.manager.split.SplitFileCache;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.tables.TableManager;

public class FateWorkerEnv implements FateEnv {
  private final ServerContext ctx;
  private final ExecutorService refreshPool;
  private final ExecutorService renamePool;
  private final ServiceLock serviceLock;
  private final LiveTServerSet tservers;
  private final SplitFileCache splitCache;

  FateWorkerEnv(ServerContext ctx, ServiceLock lock) {
    this.ctx = ctx;
    // TODO create the proper way
    this.refreshPool = Executors.newFixedThreadPool(2);
    this.renamePool = Executors.newFixedThreadPool(2);
    this.serviceLock = lock;
    this.tservers = new LiveTServerSet(ctx);
    this.splitCache = new SplitFileCache(ctx);
  }

  @Override
  public ServerContext getContext() {
    return ctx;
  }

  @Override
  public EventPublisher getEventPublisher() {
    // TODO do something w/ the events
    return new EventPublisher() {
      @Override
      public void event(String msg, Object... args) {

      }

      @Override
      public void event(Ample.DataLevel level, String msg, Object... args) {

      }

      @Override
      public void event(TableId tableId, String msg, Object... args) {

      }

      @Override
      public void event(KeyExtent extent, String msg, Object... args) {

      }

      @Override
      public void event(Collection<KeyExtent> extents, String msg, Object... args) {

      }
    };
  }

  @Override
  public void recordCompactionCompletion(ExternalCompactionId ecid) {
    // TODO do something w/ this
  }

  @Override
  public Set<TServerInstance> onlineTabletServers() {
    return tservers.getSnapshot().getTservers();
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
    // TODO
  }

  @Override
  public void removeBulkImportStatus(String sourceDir) {
    // TODO
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
