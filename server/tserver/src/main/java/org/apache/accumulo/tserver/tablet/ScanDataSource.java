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
package org.apache.accumulo.tserver.tablet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.InterruptibleIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.system.SourceSwitchingIterator.DataSource;
import org.apache.accumulo.core.iterators.system.StatsIterator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.conf.TableConfiguration.ParsedIteratorConfig;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.tserver.FileManager.ScanFileManager;
import org.apache.accumulo.tserver.InMemoryMap.MemoryIterator;
import org.apache.accumulo.tserver.TabletIteratorEnvironment;
import org.apache.accumulo.tserver.TabletServer;

import com.google.common.collect.Iterables;

class ScanDataSource implements DataSource {

  // data source state
  private final Tablet tablet;
  private ScanFileManager fileManager;
  private SortedKeyValueIterator<Key,Value> iter;
  private long expectedDeletionCount;
  private List<MemoryIterator> memIters = null;
  private long fileReservationId;
  private AtomicBoolean interruptFlag;
  private StatsIterator statsIterator;

  private final ScanOptions options;
  private final boolean loadIters;

  private static final Set<Column> EMPTY_COLS = Collections.emptySet();

  ScanDataSource(Tablet tablet, Authorizations authorizations, byte[] defaultLabels, HashSet<Column> columnSet, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, AtomicBoolean interruptFlag, SamplerConfiguration samplerConfig, long batchTimeOut, String context) {
    this.tablet = tablet;
    expectedDeletionCount = tablet.getDataSourceDeletions();
    this.options = new ScanOptions(-1, authorizations, defaultLabels, columnSet, ssiList, ssio, interruptFlag, false, samplerConfig, batchTimeOut, context);
    this.interruptFlag = interruptFlag;
    this.loadIters = true;
  }

  ScanDataSource(Tablet tablet, ScanOptions options) {
    this.tablet = tablet;
    expectedDeletionCount = tablet.getDataSourceDeletions();
    this.options = options;
    this.interruptFlag = options.getInterruptFlag();
    this.loadIters = true;
  }

  ScanDataSource(Tablet tablet, Authorizations authorizations, byte[] defaultLabels, AtomicBoolean iFlag) {
    this.tablet = tablet;
    expectedDeletionCount = tablet.getDataSourceDeletions();
    this.options = new ScanOptions(-1, authorizations, defaultLabels, EMPTY_COLS, null, null, iFlag, false, null, -1, null);
    this.interruptFlag = iFlag;
    this.loadIters = false;
  }

  @Override
  public DataSource getNewDataSource() {
    if (!isCurrent()) {
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
    } else
      return this;
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

    Map<FileRef,DataFileValue> files;

    SamplerConfigurationImpl samplerConfig = options.getSamplerConfigurationImpl();

    synchronized (tablet) {

      if (memIters != null)
        throw new IllegalStateException("Tried to create new scan iterator w/o releasing memory");

      if (tablet.isClosed())
        throw new TabletClosedException();

      if (interruptFlag.get())
        throw new IterationInterruptedException(tablet.getExtent().toString() + " " + interruptFlag.hashCode());

      // only acquire the file manager when we know the tablet is open
      if (fileManager == null) {
        fileManager = tablet.getTabletResources().newScanFileManager();
        tablet.addActiveScans(this);
      }

      if (fileManager.getNumOpenFiles() != 0)
        throw new IllegalStateException("Tried to create new scan iterator w/o releasing files");

      // set this before trying to get iterators in case
      // getIterators() throws an exception
      expectedDeletionCount = tablet.getDataSourceDeletions();

      memIters = tablet.getTabletMemory().getIterators(samplerConfig);
      Pair<Long,Map<FileRef,DataFileValue>> reservation = tablet.getDatafileManager().reserveFilesForScan();
      fileReservationId = reservation.getFirst();
      files = reservation.getSecond();
    }

    Collection<InterruptibleIterator> mapfiles = fileManager.openFiles(files, options.isIsolated(), samplerConfig);

    for (SortedKeyValueIterator<Key,Value> skvi : Iterables.concat(mapfiles, memIters))
      ((InterruptibleIterator) skvi).setInterruptFlag(interruptFlag);

    List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(mapfiles.size() + memIters.size());

    iters.addAll(mapfiles);
    iters.addAll(memIters);

    MultiIterator multiIter = new MultiIterator(iters, tablet.getExtent());

    TabletIteratorEnvironment iterEnv = new TabletIteratorEnvironment(IteratorScope.scan, tablet.getTableConfiguration(), fileManager, files,
        options.getAuthorizations(), samplerConfig);

    statsIterator = new StatsIterator(multiIter, TabletServer.seekCount, tablet.getScannedCounter());

    SortedKeyValueIterator<Key,Value> visFilter = IteratorUtil.setupSystemScanIterators(statsIterator, options.getColumnSet(), options.getAuthorizations(),
        options.getDefaultLabels());

    if (!loadIters) {
      return visFilter;
    } else {
      List<IterInfo> iterInfos;
      Map<String,Map<String,String>> iterOpts;

      ParsedIteratorConfig pic = tablet.getTableConfiguration().getParsedIteratorConfig(IteratorScope.scan);
      if (options.getSsiList().size() == 0 && options.getSsio().size() == 0) {
        iterInfos = pic.getIterInfo();
        iterOpts = pic.getOpts();
      } else {
        iterOpts = new HashMap<>(pic.getOpts().size() + options.getSsio().size());
        iterInfos = new ArrayList<>(pic.getIterInfo().size() + options.getSsiList().size());
        IteratorUtil.mergeIteratorConfig(iterInfos, iterOpts, pic.getIterInfo(), pic.getOpts(), options.getSsiList(), options.getSsio());
      }

      String context;
      if (options.getClassLoaderContext() != null) {
        context = options.getClassLoaderContext();
      } else {
        context = pic.getContext();
      }

      return iterEnv.getTopLevelIterator(IteratorUtil.loadIterators(visFilter, iterInfos, iterOpts, iterEnv, true, context));
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
      fileManager.reattach(options.getSamplerConfigurationImpl());
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
