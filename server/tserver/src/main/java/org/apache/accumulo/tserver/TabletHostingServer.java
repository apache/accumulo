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
package org.apache.accumulo.tserver;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.spi.cache.BlockCacheManager;
import org.apache.accumulo.core.spi.scan.ScanServerInfo;
import org.apache.accumulo.server.GarbageCollectionLogger;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.session.Session;
import org.apache.accumulo.tserver.session.SessionManager;
import org.apache.accumulo.tserver.tablet.Tablet;

/**
 * This interface exist to support passing a {@link TabletServer} or {@link ScanServerInfo} to a
 * method that can take either.
 */
public interface TabletHostingServer {

  ServerContext getContext();

  AccumuloConfiguration getConfiguration();

  Tablet getOnlineTablet(KeyExtent extent);

  SessionManager getSessionManager();

  TabletServerResourceManager getResourceManager();

  TabletServerScanMetrics getScanMetrics();

  Session getSession(long scanID);

  TableConfiguration getTableConfiguration(KeyExtent threadPoolExtent);

  ServiceLock getLock();

  ZooCache getManagerLockCache();

  GarbageCollectionLogger getGcLogger();

  BlockCacheManager.Configuration getBlockCacheConfiguration(AccumuloConfiguration acuConf);
}
