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
package org.apache.accumulo.tserver.tablet;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.TabletHostingServer;
import org.apache.accumulo.tserver.TabletServerResourceManager;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tablet that can not be written to and operates off of a snapshot of tablet metadata for its
 * entire lifetime. The set of files and therefore the data backing this tablet will never change
 * for its lifetime.
 */
public class SnapshotTablet extends TabletBase {

  private static final Logger log = LoggerFactory.getLogger(SnapshotTablet.class);

  private final TabletHostingServer server;
  private final SortedMap<StoredTabletFile,DataFileValue> files;
  private final TabletServerResourceManager.TabletResourceManager tabletResources;
  private boolean closed = false;
  private final AtomicLong dataSourceDeletions = new AtomicLong(0);

  public SnapshotTablet(TabletHostingServer server, TabletMetadata metadata,
      TabletServerResourceManager.TabletResourceManager tabletResources) {
    super(server, metadata.getExtent());
    this.server = server;
    this.files = Collections.unmodifiableSortedMap(new TreeMap<>(metadata.getFilesMap()));
    this.tabletResources = tabletResources;
  }

  @Override
  public synchronized boolean isClosed() {
    return closed;
  }

  @Override
  public SortedMap<StoredTabletFile,DataFileValue> getDatafiles() {
    return files;
  }

  @Override
  public void addToYieldMetric(int i) {
    this.server.getScanMetrics().addYield(i);
  }

  @Override
  public long getDataSourceDeletions() {
    return dataSourceDeletions.get();
  }

  @Override
  TabletServerResourceManager.TabletResourceManager getTabletResources() {
    return tabletResources;
  }

  @Override
  public List<InMemoryMap.MemoryIterator> getMemIterators(SamplerConfigurationImpl samplerConfig) {
    return List.of();
  }

  @Override
  public void returnMemIterators(List<InMemoryMap.MemoryIterator> iters) {

  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Pair<Long,Map<TabletFile,DataFileValue>> reserveFilesForScan() {
    return new Pair(0L, getDatafiles());
  }

  @Override
  public void returnFilesForScan(long scanId) {

  }

  @Override
  public TabletServerScanMetrics getScanMetrics() {
    return this.server.getScanMetrics();
  }

  @Override
  public synchronized void close(boolean b) throws IOException {
    if (closed) {
      return;
    }

    closed = true;

    // modify dataSourceDeletions so scans will try to switch data sources and fail because the
    // tablet is closed
    dataSourceDeletions.incrementAndGet();

    for (ScanDataSource activeScan : activeScans) {
      activeScan.interrupt();
    }

    // wait for reads and writes to complete
    while (!activeScans.isEmpty()) {
      try {
        log.debug("Closing tablet {} waiting for {} scans", extent, activeScans.size());
        this.wait(50);
      } catch (InterruptedException e) {
        log.error(e.toString());
      }
    }
  }
}
