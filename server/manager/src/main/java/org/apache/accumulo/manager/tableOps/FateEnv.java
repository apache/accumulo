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
package org.apache.accumulo.manager.tableOps;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.manager.EventPublisher;
import org.apache.accumulo.manager.split.FileRangeCache;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tables.TableManager;

public interface FateEnv {
  ServerContext getContext();

  EventPublisher getEventPublisher();

  void recordCompactionCompletion(ExternalCompactionId ecid);

  Set<TServerInstance> onlineTabletServers();

  TableManager getTableManager();

  VolumeManager getVolumeManager();

  ServiceLock getServiceLock();

  SteadyTime getSteadyTime();

  ExecutorService getTabletRefreshThreadPool();

  FileRangeCache getFileRangeCache();

  ExecutorService getRenamePool();

}
