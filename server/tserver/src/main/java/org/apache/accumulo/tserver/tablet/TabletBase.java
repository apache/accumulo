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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.iteratorsImpl.system.SourceSwitchingIterator;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ShutdownUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.TooManyFilesException;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.TabletHostingServer;
import org.apache.accumulo.tserver.TabletServerResourceManager;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class exists to share code for scanning a tablet between {@link Tablet} and
 * {@link SnapshotTablet}
 */
public abstract class TabletBase {

  private static final Logger log = LoggerFactory.getLogger(TabletBase.class);

  private static final byte[] EMPTY_BYTES = new byte[0];

  protected final KeyExtent extent;
  protected final ServerContext context;
  private final TabletHostingServer server;

  protected AtomicLong lookupCount = new AtomicLong(0);
  protected AtomicLong queryResultCount = new AtomicLong(0);
  protected AtomicLong queryResultBytes = new AtomicLong(0);
  protected final AtomicLong scannedCount = new AtomicLong(0);

  protected final Set<ScanDataSource> activeScans = new HashSet<>();

  private final AccumuloConfiguration.Deriver<byte[]> defaultSecurityLabel;

  protected final TableConfiguration tableConfiguration;

  public TabletBase(TabletHostingServer server, KeyExtent extent) {
    this.context = server.getContext();
    this.server = server;
    this.extent = extent;

    TableConfiguration tblConf = context.getTableConfiguration(extent.tableId());
    if (tblConf == null) {
      context.clearTableListCache();
      tblConf = context.getTableConfiguration(extent.tableId());
      requireNonNull(tblConf, "Could not get table configuration for " + extent.tableId());
    }

    this.tableConfiguration = tblConf;

    if (extent.isMeta()) {
      defaultSecurityLabel = () -> EMPTY_BYTES;
    } else {
      defaultSecurityLabel = tableConfiguration.newDeriver(
          conf -> new ColumnVisibility(conf.get(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY))
              .getExpression());
    }
  }

  public abstract boolean isClosed();

  public abstract SortedMap<StoredTabletFile,DataFileValue> getDatafiles();

  public abstract void addToYieldMetric(int i);

  public abstract long getDataSourceDeletions();

  abstract TabletServerResourceManager.TabletResourceManager getTabletResources();

  public abstract List<InMemoryMap.MemoryIterator>
      getMemIterators(SamplerConfigurationImpl samplerConfig);

  public abstract void returnMemIterators(List<InMemoryMap.MemoryIterator> iters);

  public abstract Pair<Long,Map<TabletFile,DataFileValue>> reserveFilesForScan();

  public abstract void returnFilesForScan(long scanId);

  public abstract TabletServerScanMetrics getScanMetrics();

  protected ScanDataSource createDataSource(ScanParameters scanParams, boolean loadIters,
      AtomicBoolean interruptFlag) {
    return new ScanDataSource(this, scanParams, loadIters, interruptFlag);
  }

  public Scanner createScanner(Range range, ScanParameters scanParams,
      AtomicBoolean interruptFlag) {
    // do a test to see if this range falls within the tablet, if it does not
    // then clip will throw an exception
    extent.toDataRange().clip(range);

    return new Scanner(this, range, scanParams, interruptFlag);
  }

  public AtomicLong getScannedCounter() {
    return this.scannedCount;
  }

  public ServerContext getContext() {
    return context;
  }

  public TableConfiguration getTableConfiguration() {
    return tableConfiguration;
  }

  public KeyExtent getExtent() {
    return extent;
  }

  public byte[] getDefaultSecurityLabels() {
    return defaultSecurityLabel.derive();
  }

  public synchronized void addActiveScans(ScanDataSource scanDataSource) {
    activeScans.add(scanDataSource);
  }

  public int removeScan(ScanDataSource scanDataSource) {
    activeScans.remove(scanDataSource);
    return activeScans.size();
  }

  public abstract void close(boolean b) throws IOException;

  public Tablet.LookupResult lookup(List<Range> ranges, List<KVEntry> results,
      ScanParameters scanParams, long maxResultSize, AtomicBoolean interruptFlag)
      throws IOException {

    if (ranges.isEmpty()) {
      return new Tablet.LookupResult();
    }

    ranges = Range.mergeOverlapping(ranges);
    if (ranges.size() > 1) {
      Collections.sort(ranges);
    }

    Range tabletRange = getExtent().toDataRange();
    for (Range range : ranges) {
      // do a test to see if this range falls within the tablet, if it does not
      // then clip will throw an exception
      tabletRange.clip(range);
    }

    SourceSwitchingIterator.DataSource dataSource =
        createDataSource(scanParams, true, interruptFlag);

    Tablet.LookupResult result = null;

    try {
      SortedKeyValueIterator<Key,Value> iter = new SourceSwitchingIterator(dataSource);
      this.lookupCount.incrementAndGet();
      this.server.getScanMetrics().incrementLookupCount(1);
      result = lookup(iter, ranges, results, scanParams, maxResultSize);
      return result;
    } catch (IOException ioe) {
      dataSource.close(true);
      throw ioe;
    } finally {
      // code in finally block because always want
      // to return mapfiles, even when exception is thrown
      dataSource.close(false);

      synchronized (this) {
        queryResultCount.addAndGet(results.size());
        this.server.getScanMetrics().incrementQueryResultCount(results.size());
        if (result != null) {
          this.queryResultBytes.addAndGet(result.dataSize);
          this.server.getScanMetrics().incrementQueryResultBytes(result.dataSize);
        }
      }
    }
  }

  Batch nextBatch(SortedKeyValueIterator<Key,Value> iter, Range range, ScanParameters scanParams)
      throws IOException {

    // log.info("In nextBatch..");

    long batchTimeOut = scanParams.getBatchTimeOut();

    long timeToRun = TimeUnit.MILLISECONDS.toNanos(batchTimeOut);
    long startNanos = System.nanoTime();

    if (batchTimeOut == Long.MAX_VALUE || batchTimeOut <= 0) {
      batchTimeOut = 0;
    }
    List<KVEntry> results = new ArrayList<>();
    Key key = null;

    Value value;
    long resultSize = 0L;
    long resultBytes = 0L;

    long maxResultsSize = getTableConfiguration().getAsBytes(Property.TABLE_SCAN_MAXMEM);

    Key continueKey = null;
    boolean skipContinueKey = false;

    YieldCallback<Key> yield = new YieldCallback<>();

    // we cannot yield if we are in isolation mode
    if (!scanParams.isIsolated()) {
      iter.enableYielding(yield);
    }

    if (scanParams.getColumnSet().isEmpty()) {
      iter.seek(range, Set.of(), false);
    } else {
      iter.seek(range, LocalityGroupUtil.families(scanParams.getColumnSet()), true);
    }

    while (iter.hasTop()) {
      if (yield.hasYielded()) {
        throw new IOException(
            "Coding error: hasTop returned true but has yielded at " + yield.getPositionAndReset());
      }
      value = iter.getTopValue();
      key = iter.getTopKey();

      KVEntry kvEntry = new KVEntry(key, value); // copies key and value
      results.add(kvEntry);
      resultSize += kvEntry.estimateMemoryUsed();
      resultBytes += kvEntry.numBytes();

      boolean timesUp = batchTimeOut > 0 && (System.nanoTime() - startNanos) >= timeToRun;

      if (resultSize >= maxResultsSize || results.size() >= scanParams.getMaxEntries() || timesUp) {
        continueKey = new Key(key);
        skipContinueKey = true;
        break;
      }

      iter.next();
    }

    if (yield.hasYielded()) {
      continueKey = new Key(yield.getPositionAndReset());
      skipContinueKey = true;
      if (!range.contains(continueKey)) {
        throw new IOException("Underlying iterator yielded to a position outside of its range: "
            + continueKey + " not in " + range);
      }
      if (!results.isEmpty()
          && continueKey.compareTo(results.get(results.size() - 1).getKey()) <= 0) {
        throw new IOException(
            "Underlying iterator yielded to a position that does not follow the last key returned: "
                + continueKey + " <= " + results.get(results.size() - 1).getKey());
      }

      log.debug("Scan yield detected at position " + continueKey);
      addToYieldMetric(1);
    } else if (!iter.hasTop()) {
      // end of tablet has been reached
      continueKey = null;
      if (results.isEmpty()) {
        results = null;
      }
    }

    return new Batch(skipContinueKey, results, continueKey, resultBytes);
  }

  private Tablet.LookupResult lookup(SortedKeyValueIterator<Key,Value> mmfi, List<Range> ranges,
      List<KVEntry> results, ScanParameters scanParams, long maxResultsSize) throws IOException {

    Tablet.LookupResult lookupResult = new Tablet.LookupResult();

    boolean exceededMemoryUsage = false;
    boolean tabletClosed = false;

    Set<ByteSequence> cfset = null;
    if (!scanParams.getColumnSet().isEmpty()) {
      cfset = LocalityGroupUtil.families(scanParams.getColumnSet());
    }

    long batchTimeOut = scanParams.getBatchTimeOut();

    long timeToRun = TimeUnit.MILLISECONDS.toNanos(batchTimeOut);
    long startNanos = System.nanoTime();

    if (batchTimeOut <= 0 || batchTimeOut == Long.MAX_VALUE) {
      batchTimeOut = 0;
    }

    // determine if the iterator supported yielding
    YieldCallback<Key> yield = new YieldCallback<>();
    mmfi.enableYielding(yield);
    boolean yielded = false;

    for (Range range : ranges) {

      boolean timesUp = batchTimeOut > 0 && (System.nanoTime() - startNanos) > timeToRun;

      if (exceededMemoryUsage || tabletClosed || timesUp || yielded) {
        lookupResult.unfinishedRanges.add(range);
        continue;
      }

      int entriesAdded = 0;

      try {
        if (cfset != null) {
          mmfi.seek(range, cfset, true);
        } else {
          mmfi.seek(range, Set.of(), false);
        }

        while (mmfi.hasTop()) {
          if (yield.hasYielded()) {
            throw new IOException("Coding error: hasTop returned true but has yielded at "
                + yield.getPositionAndReset());
          }
          Key key = mmfi.getTopKey();

          KVEntry kve = new KVEntry(key, mmfi.getTopValue());
          results.add(kve);
          entriesAdded++;
          lookupResult.bytesAdded += kve.estimateMemoryUsed();
          lookupResult.dataSize += kve.numBytes();

          exceededMemoryUsage = lookupResult.bytesAdded > maxResultsSize;

          timesUp = batchTimeOut > 0 && (System.nanoTime() - startNanos) > timeToRun;

          if (exceededMemoryUsage || timesUp) {
            addUnfinishedRange(lookupResult, range, key);
            break;
          }

          mmfi.next();
        }

        if (yield.hasYielded()) {
          yielded = true;
          Key yieldPosition = yield.getPositionAndReset();
          if (!range.contains(yieldPosition)) {
            throw new IOException("Underlying iterator yielded to a position outside of its range: "
                + yieldPosition + " not in " + range);
          }
          if (!results.isEmpty()
              && yieldPosition.compareTo(results.get(results.size() - 1).getKey()) <= 0) {
            throw new IOException("Underlying iterator yielded to a position"
                + " that does not follow the last key returned: " + yieldPosition + " <= "
                + results.get(results.size() - 1).getKey());
          }
          addUnfinishedRange(lookupResult, range, yieldPosition);

          log.debug("Scan yield detected at position " + yieldPosition);
          addToYieldMetric(1);
        }
      } catch (TooManyFilesException tmfe) {
        // treat this as a closed tablet, and let the client retry
        log.warn("Tablet {} has too many files, batch lookup can not run", getExtent());
        handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range,
            entriesAdded);
        tabletClosed = true;
      } catch (IOException ioe) {
        if (ShutdownUtil.wasCausedByHadoopShutdown(ioe)) {
          // assume HDFS shutdown hook caused this exception
          log.debug("IOException while shutdown in progress", ioe);
          handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range,
              entriesAdded);
          tabletClosed = true;
        } else {
          throw ioe;
        }
      } catch (IterationInterruptedException iie) {
        if (isClosed()) {
          handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range,
              entriesAdded);
          tabletClosed = true;
        } else {
          throw iie;
        }
      } catch (TabletClosedException tce) {
        handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range,
            entriesAdded);
        tabletClosed = true;
      } catch (RuntimeException re) {
        if (ShutdownUtil.wasCausedByHadoopShutdown(re)) {
          log.debug("RuntimeException while shutdown in progress", re);
          handleTabletClosedDuringScan(results, lookupResult, exceededMemoryUsage, range,
              entriesAdded);
          tabletClosed = true;
        } else {
          throw re;
        }
      }

    }

    return lookupResult;
  }

  private void handleTabletClosedDuringScan(List<KVEntry> results, Tablet.LookupResult lookupResult,
      boolean exceededMemoryUsage, Range range, int entriesAdded) {
    if (exceededMemoryUsage) {
      throw new IllegalStateException(
          "Tablet " + getExtent() + "should not exceed memory usage or close, not both");
    }

    if (entriesAdded > 0) {
      addUnfinishedRange(lookupResult, range, results.get(results.size() - 1).getKey());
    } else {
      lookupResult.unfinishedRanges.add(range);
    }

    lookupResult.closed = true;
  }

  private void addUnfinishedRange(Tablet.LookupResult lookupResult, Range range, Key key) {
    if (range.getEndKey() == null || key.compareTo(range.getEndKey()) < 0) {
      Range nlur = new Range(new Key(key), false, range.getEndKey(), range.isEndKeyInclusive());
      lookupResult.unfinishedRanges.add(nlur);
    }
  }

  public synchronized void updateQueryStats(int size, long numBytes) {
    this.queryResultCount.addAndGet(size);
    this.server.getScanMetrics().incrementQueryResultCount(size);
    this.queryResultBytes.addAndGet(numBytes);
    this.server.getScanMetrics().incrementQueryResultBytes(numBytes);
  }
}
