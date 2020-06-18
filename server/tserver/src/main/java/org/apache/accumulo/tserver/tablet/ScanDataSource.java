/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.conf.IterConfigUtil;
import org.apache.accumulo.core.conf.IterLoad;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SourceSwitchingIterator.DataSource;
import org.apache.accumulo.core.iteratorsImpl.system.StatsIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.conf.TableConfiguration.ParsedIteratorConfig;
import org.apache.accumulo.tserver.FileManager.ScanFileManager;
import org.apache.accumulo.tserver.InMemoryMap.MemoryIterator;
import org.apache.accumulo.tserver.TabletIteratorEnvironment;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

class ScanDataSource implements DataSource {

  private static final Logger log = LoggerFactory.getLogger(ScanDataSource.class);
  // data source state
  private final Tablet tablet;
  private ScanFileManager fileManager;
  private SortedKeyValueIterator<Key,Value> iter;
  private long expectedDeletionCount;
  private List<MemoryIterator> memIters = null;
  private long fileReservationId;
  private AtomicBoolean interruptFlag;
  private StatsIterator statsIterator;

  private final ScanParameters scanParams;
  private final boolean loadIters;
  private final byte[] defaultLabels;

  ScanDataSource(Tablet tablet, ScanParameters scanParams, boolean loadIters,
      AtomicBoolean interruptFlag) {
    this.tablet = tablet;
    this.expectedDeletionCount = tablet.getDataSourceDeletions();
    this.scanParams = scanParams;
    this.interruptFlag = interruptFlag;
    this.loadIters = loadIters;
    this.defaultLabels = tablet.getDefaultSecurityLabels();
    if (log.isTraceEnabled()) {
      log.trace("new scan data source, tablet: {}, params: {}, loadIterators: {}", this.tablet,
          this.scanParams, this.loadIters);
    }
  }

  @Override
  public DataSource getNewDataSource() {
    if (isCurrent())
      return this;
    else {
      // log.debug("Switching data sources during a scan");
      if (memIters != null) {
        tablet.getTabletMemory().returnIterators(memIters);
        memIters = null;
        tablet.getDatafileManager().returnFilesForScan(fileReservationId);
        fileReservationId = -1;
      }

      if (fileManager != null)
        fileManager.releaseOpenFiles(false);

      expectedDeletionCount = tablet.getDataSourceDeletions();
      iter = null;

      return this;
    }
  }

  @Override
  public boolean isCurrent() {
    return expectedDeletionCount == tablet.getDataSourceDeletions();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> iterator() throws IOException {
    if (iter == null)
      iter = createIterator();
    return iter;
  }

  private SortedKeyValueIterator<Key,Value> createIterator() throws IOException {

    Map<TabletFile,DataFileValue> files;

    SamplerConfigurationImpl samplerConfig = scanParams.getSamplerConfigurationImpl();

    synchronized (tablet) {

      if (memIters != null)
        throw new IllegalStateException("Tried to create new scan iterator w/o releasing memory");

      if (tablet.isClosed())
        throw new TabletClosedException();

      if (interruptFlag.get())
        throw new IterationInterruptedException(
            tablet.getExtent() + " " + interruptFlag.hashCode());

      // only acquire the file manager when we know the tablet is open
      if (fileManager == null) {
        fileManager =
            tablet.getTabletResources().newScanFileManager(scanParams.getScanDirectives());
        tablet.addActiveScans(this);
      }

      if (fileManager.getNumOpenFiles() != 0)
        throw new IllegalStateException("Tried to create new scan iterator w/o releasing files");

      // set this before trying to get iterators in case
      // getIterators() throws an exception
      expectedDeletionCount = tablet.getDataSourceDeletions();

      memIters = tablet.getTabletMemory().getIterators(samplerConfig);
      Pair<Long,Map<TabletFile,DataFileValue>> reservation =
          tablet.getDatafileManager().reserveFilesForScan();
      fileReservationId = reservation.getFirst();
      files = reservation.getSecond();
    }

    Collection<InterruptibleIterator> mapfiles =
        fileManager.openFiles(files, scanParams.isIsolated(), samplerConfig);

    for (SortedKeyValueIterator<Key,Value> skvi : Iterables.concat(mapfiles, memIters))
      ((InterruptibleIterator) skvi).setInterruptFlag(interruptFlag);

    List<SortedKeyValueIterator<Key,Value>> iters =
        new ArrayList<>(mapfiles.size() + memIters.size());

    iters.addAll(mapfiles);
    iters.addAll(memIters);

    MultiIterator multiIter = new MultiIterator(iters, tablet.getExtent());

    TabletIteratorEnvironment iterEnv =
        new TabletIteratorEnvironment(tablet.getTabletServer().getContext(), IteratorScope.scan,
            tablet.getTableConfiguration(), tablet.getExtent().getTableId(), fileManager, files,
            scanParams.getAuthorizations(), samplerConfig, new ArrayList<>());

    statsIterator =
        new StatsIterator(multiIter, TabletServer.seekCount, tablet.getScannedCounter());

    SortedKeyValueIterator<Key,Value> visFilter =
        SystemIteratorUtil.setupSystemScanIterators(statsIterator, scanParams.getColumnSet(),
            scanParams.getAuthorizations(), defaultLabels, tablet.getTableConfiguration());

    if (loadIters) {
      List<IterInfo> iterInfos;
      Map<String,Map<String,String>> iterOpts;

      ParsedIteratorConfig pic =
          tablet.getTableConfiguration().getParsedIteratorConfig(IteratorScope.scan);
      if (scanParams.getSsiList().isEmpty() && scanParams.getSsio().isEmpty()) {
        // No scan time iterator options were set, so can just use the pre-parsed table iterator
        // options.
        iterInfos = pic.getIterInfo();
        iterOpts = pic.getOpts();
      } else {
        // Scan time iterator options were set, so need to merge those with pre-parsed table
        // iterator options.
        iterOpts = new HashMap<>(pic.getOpts().size() + scanParams.getSsio().size());
        iterInfos = new ArrayList<>(pic.getIterInfo().size() + scanParams.getSsiList().size());
        IterConfigUtil.mergeIteratorConfig(iterInfos, iterOpts, pic.getIterInfo(), pic.getOpts(),
            scanParams.getSsiList(), scanParams.getSsio());
      }

      String context;
      if (scanParams.getClassLoaderContext() != null) {
        log.trace("Loading iterators for scan with scan context: {}",
            scanParams.getClassLoaderContext());
        context = scanParams.getClassLoaderContext();
      } else {
        context = pic.getServiceEnv();
        if (context != null) {
          log.trace("Loading iterators for scan with table context: {}",
              scanParams.getClassLoaderContext());
        } else {
          log.trace("Loading iterators for scan");
        }
      }

      IterLoad il = new IterLoad().iters(iterInfos).iterOpts(iterOpts).iterEnv(iterEnv)
          .useAccumuloClassLoader(true).context(context);
      return iterEnv.getTopLevelIterator(IterConfigUtil.loadIterators(visFilter, il));
    } else {
      return visFilter;
    }
  }

  void close(boolean sawErrors) {

    if (memIters != null) {
      tablet.getTabletMemory().returnIterators(memIters);
      memIters = null;
      tablet.getDatafileManager().returnFilesForScan(fileReservationId);
      fileReservationId = -1;
    }

    synchronized (tablet) {
      if (tablet.removeScan(this) == 0)
        tablet.notifyAll();
    }

    if (fileManager != null) {
      fileManager.releaseOpenFiles(sawErrors);
      fileManager = null;
    }

    if (statsIterator != null) {
      statsIterator.report();
    }

  }

  public void interrupt() {
    interruptFlag.set(true);
  }

  @Override
  public DataSource getDeepCopyDataSource(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  public void reattachFileManager() throws IOException {
    if (fileManager != null)
      fileManager.reattach(scanParams.getSamplerConfigurationImpl());
  }

  public void detachFileManager() {
    if (fileManager != null)
      fileManager.detach();
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    throw new UnsupportedOperationException();
  }

}
