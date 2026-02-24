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
package org.apache.accumulo.server.compaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileOperations.ReaderBuilder;
import org.apache.accumulo.core.file.FileOperations.WriterBuilder;
import org.apache.accumulo.core.file.FilePrefix;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.core.iteratorsImpl.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.DeletingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionReason;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.iterators.SystemIteratorEnvironment;
import org.apache.accumulo.server.mem.LowMemoryDetector.DetectionScope;
import org.apache.accumulo.server.problems.ProblemReportingIterator;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class FileCompactor implements Callable<CompactionStats> {
  private static final Logger log = LoggerFactory.getLogger(FileCompactor.class);
  private static final AtomicLong nextCompactorID = new AtomicLong(0);

  public static class CompactionCanceledException extends Exception {
    private static final long serialVersionUID = 1L;
  }

  public interface CompactionEnv {

    boolean isCompactionEnabled();

    IteratorScope getIteratorScope();

    SystemIteratorEnvironment createIteratorEnv(ServerContext context,
        AccumuloConfiguration acuTableConf, TableId tableId);

    SortedKeyValueIterator<Key,Value> getMinCIterator();

    TCompactionReason getReason();
  }

  private final Map<StoredTabletFile,DataFileValue> filesToCompact;
  private final ReferencedTabletFile outputFile;
  private final boolean propagateDeletes;
  private final AccumuloConfiguration acuTableConf;
  private final CompactionEnv env;
  private final VolumeManager fs;
  protected final KeyExtent extent;
  private final List<IteratorSetting> iterators;
  private final CryptoService cryptoService;
  private final PausedCompactionMetrics metrics;

  // things to report
  private String currentLocalityGroup = "";
  private volatile Timer startTime;

  private final AtomicInteger timesPaused = new AtomicInteger(0);

  private final AtomicLong currentEntriesRead = new AtomicLong(0);
  private final AtomicLong currentEntriesWritten = new AtomicLong(0);

  // These track the cumulative count of entries (read and written) that has been recorded in
  // the global counts. Their purpose is to avoid double counting of metrics during the update of
  // global statistics.
  private final AtomicLong lastRecordedEntriesRead = new AtomicLong(0);
  private final AtomicLong lastRecordedEntriesWritten = new AtomicLong(0);

  private static final LongAdder totalEntriesRead = new LongAdder();
  private static final LongAdder totalEntriesWritten = new LongAdder();
  private static final Timer lastUpdateTime = Timer.startNew();

  private final DateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

  // a unique id to identify a compactor
  private final long compactorID = nextCompactorID.getAndIncrement();
  private volatile Thread thread;
  private final ServerContext context;

  private final AtomicBoolean interruptFlag = new AtomicBoolean(false);

  public synchronized void interrupt() {
    interruptFlag.set(true);

    if (thread != null) {
      // Never want to interrupt the thread after clearThread was called as the thread could have
      // moved on to something completely different than the compaction. This method and clearThread
      // being synchronized and clearThread setting thread to null prevent this.
      thread.interrupt();
    }
  }

  private class ThreadClearer implements AutoCloseable {
    @Override
    public void close() throws InterruptedException {
      clearThread();
    }
  }

  private synchronized ThreadClearer setThread() {
    thread = Thread.currentThread();
    return new ThreadClearer();
  }

  private synchronized void clearThread() throws InterruptedException {
    Preconditions.checkState(thread == Thread.currentThread());
    thread = null;
    // If the thread was interrupted during compaction do not want to allow the thread to continue
    // w/ the interrupt status set as this could impact code unrelated to the compaction. For
    // internal compactions the thread will execute metadata update code after the compaction and
    // would not want the interrupt status set for that.
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  Thread getThread() {
    return thread;
  }

  public long getCompactorID() {
    return compactorID;
  }

  private synchronized void setLocalityGroup(String name) {
    this.currentLocalityGroup = name;
  }

  public synchronized String getCurrentLocalityGroup() {
    return currentLocalityGroup;
  }

  private void clearCurrentEntryCounts() {
    currentEntriesRead.set(0);
    currentEntriesWritten.set(0);
    timesPaused.set(0);
  }

  private void updateGlobalEntryCounts() {
    updateTotalEntries(currentEntriesRead, lastRecordedEntriesRead, totalEntriesRead);
    updateTotalEntries(currentEntriesWritten, lastRecordedEntriesWritten, totalEntriesWritten);
  }

  /**
   * Updates the total count of entries by adding the difference between the current count and the
   * last recorded count to the total.
   *
   * @param current The current count of entries
   * @param recorded The last recorded count of entries
   * @param total The total count to add the difference to
   */
  private void updateTotalEntries(AtomicLong current, AtomicLong recorded, LongAdder total) {
    long currentCount = current.get();
    long lastRecorded =
        recorded.getAndUpdate(recordedValue -> Math.max(recordedValue, currentCount));
    if (lastRecorded < currentCount) {
      total.add(currentCount - lastRecorded);
    }
  }

  /**
   * @return the total entries written by compactions over the lifetime of this process.
   */
  public static long getTotalEntriesWritten() {
    updateTotalEntries();
    return totalEntriesWritten.sum();
  }

  /**
   * @return the total entries read by compactions over the lifetime of this process.
   */
  public static long getTotalEntriesRead() {
    updateTotalEntries();
    return totalEntriesRead.sum();
  }

  /**
   * Updates total entries read and written for all currently running compactions. Compactions will
   * update the global stats when they finish. This can be called to update them sooner. This method
   * is rate limited, so it will not cause issues if called too frequently.
   */
  private static void updateTotalEntries() {
    if (!lastUpdateTime.hasElapsed(100, MILLISECONDS)) {
      return;
    }
    runningCompactions.forEach(FileCompactor::updateGlobalEntryCounts);
    lastUpdateTime.restart();
  }

  protected static final Set<FileCompactor> runningCompactions =
      Collections.synchronizedSet(new HashSet<>());

  public static List<CompactionInfo> getRunningCompactions() {
    ArrayList<CompactionInfo> compactions = new ArrayList<>();

    runningCompactions.forEach(compactor -> compactions.add(new CompactionInfo(compactor)));

    return compactions;
  }

  public FileCompactor(ServerContext context, KeyExtent extent,
      Map<StoredTabletFile,DataFileValue> files, ReferencedTabletFile outputFile,
      boolean propagateDeletes, CompactionEnv env, List<IteratorSetting> iterators,
      AccumuloConfiguration tableConfiguation, CryptoService cs, PausedCompactionMetrics metrics) {
    this.context = context;
    this.extent = extent;
    this.fs = context.getVolumeManager();
    this.acuTableConf = tableConfiguation;
    this.filesToCompact = files;
    this.outputFile = outputFile;
    this.propagateDeletes = propagateDeletes;
    this.env = env;
    this.iterators = iterators;
    this.cryptoService = cs;
    this.metrics = metrics;
  }

  public VolumeManager getVolumeManager() {
    return fs;
  }

  public KeyExtent getExtent() {
    return extent;
  }

  protected StoredTabletFile getOutputFile() {
    return outputFile.insert();
  }

  protected Map<String,Set<ByteSequence>> getLocalityGroups(AccumuloConfiguration acuTableConf)
      throws IOException {
    try {
      return LocalityGroupUtil.getLocalityGroups(acuTableConf);
    } catch (LocalityGroupConfigurationError e) {
      throw new IOException(e);
    }
  }

  @Override
  public CompactionStats call() throws IOException, CompactionCanceledException,
      InterruptedException, ReflectiveOperationException {

    FileSKVWriter mfw = null;

    CompactionStats majCStats = new CompactionStats();

    startTime = Timer.startNew();

    boolean remove = runningCompactions.add(this);

    String threadStartDate = dateFormatter.format(new Date());

    clearCurrentEntryCounts();

    final boolean isMinC = env.getIteratorScope() == IteratorUtil.IteratorScope.minc;

    StringBuilder newThreadName = new StringBuilder();
    if (isMinC) {
      newThreadName.append("MinC ");
    } else {
      newThreadName.append("MajC ");
    }

    String oldThreadName = Thread.currentThread().getName();
    newThreadName.append("compacting ").append(extent).append(" started ").append(threadStartDate)
        .append(" file: ").append(outputFile);
    Thread.currentThread().setName(newThreadName.toString());
    // Use try w/ resources for clearing the thread instead of finally because clearing may throw an
    // exception. Java's handling of exceptions thrown in finally blocks is not good.
    try (var ignored = setThread()) {
      FileOperations fileFactory = FileOperations.getInstance();
      FileSystem ns = this.fs.getFileSystemByPath(outputFile.getPath());

      // Normally you would not want the DataNode to continue to
      // cache blocks in the page cache for compaction input files
      // as these files are normally marked for deletion after a
      // compaction occurs. However there can be cases where the
      // compaction input files will continue to be used, like in
      // the case of bulk import files which may be assigned to many
      // tablets and will still be needed until all of the tablets
      // have compacted, or in the case of cloned tables where one
      // of the tables has compacted the input file but the other
      // has not.
      final String dropCachePrefixProperty =
          acuTableConf.get(Property.TABLE_COMPACTION_INPUT_DROP_CACHE_BEHIND);
      final EnumSet<FilePrefix> dropCacheFileTypes =
          ConfigurationTypeHelper.getDropCacheBehindFilePrefixes(dropCachePrefixProperty);

      final boolean dropCacheBehindOutput =
          !SystemTables.ROOT.tableId().equals(this.extent.tableId())
              && !SystemTables.METADATA.tableId().equals(this.extent.tableId())
              && ((isMinC && acuTableConf.getBoolean(Property.TABLE_MINC_OUTPUT_DROP_CACHE))
                  || (!isMinC && acuTableConf.getBoolean(Property.TABLE_MAJC_OUTPUT_DROP_CACHE)));

      WriterBuilder outBuilder = fileFactory.newWriterBuilder().forTable(this.extent.tableId())
          .forFile(outputFile, ns, ns.getConf(), cryptoService)
          .withTableConfiguration(acuTableConf);

      if (dropCacheBehindOutput) {
        outBuilder.dropCachesBehind();
      }
      mfw = outBuilder.build();

      Map<String,Set<ByteSequence>> lGroups = getLocalityGroups(acuTableConf);

      long t1 = System.currentTimeMillis();

      HashSet<ByteSequence> allColumnFamilies = new HashSet<>();

      if (mfw.supportsLocalityGroups()) {
        for (Entry<String,Set<ByteSequence>> entry : lGroups.entrySet()) {
          setLocalityGroup(entry.getKey());
          compactLocalityGroup(entry.getKey(), entry.getValue(), true, mfw, majCStats,
              dropCacheFileTypes);
          allColumnFamilies.addAll(entry.getValue());
        }
      }

      setLocalityGroup("");
      compactLocalityGroup(null, allColumnFamilies, false, mfw, majCStats, dropCacheFileTypes);

      long t2 = System.currentTimeMillis();

      FileSKVWriter mfwTmp = mfw;
      mfw = null; // set this to null so we do not try to close it again in finally if the close
                  // fails
      try {
        mfwTmp.close(); // if the close fails it will cause the compaction to fail
      } catch (IOException ex) {
        if (!fs.deleteRecursively(outputFile.getPath())) {
          if (fs.exists(outputFile.getPath())) {
            log.error("Unable to delete {}", outputFile);
          }
        }
        throw ex;
      }

      log.trace(String.format(
          "Compaction %s %,d read | %,d written | %,6d entries/sec"
              + " | %,6.3f secs | %,12d bytes | %9.3f byte/sec | %,d paused",
          extent, majCStats.getEntriesRead(), majCStats.getEntriesWritten(),
          (int) (majCStats.getEntriesRead() / ((t2 - t1) / 1000.0)), (t2 - t1) / 1000.0,
          mfwTmp.getLength(), mfwTmp.getLength() / ((t2 - t1) / 1000.0),
          majCStats.getTimesPaused()));

      majCStats.setFileSize(mfwTmp.getLength());
      return majCStats;
    } catch (CompactionCanceledException e) {
      log.debug("Compaction canceled {}", extent);
      throw e;
    } catch (IterationInterruptedException iie) {
      if (!env.isCompactionEnabled()) {
        log.debug("Compaction canceled {}", extent);
        throw new CompactionCanceledException();
      }
      log.debug("RFile interrupted {}", extent);
      throw iie;
    } catch (IOException | RuntimeException e) {
      Collection<String> inputFileNames =
          Collections2.transform(getFilesToCompact(), StoredTabletFile::getFileName);
      String outputFileName = outputFile.getFileName();
      log.error(
          "Compaction error. Compaction info: "
              + "extent: {}, input files: {}, output file: {}, iterators: {}, start date: {}",
          getExtent(), inputFileNames, outputFileName, getIterators(), threadStartDate, e);
      throw e;
    } finally {
      Thread.currentThread().setName(oldThreadName);
      if (remove) {
        runningCompactions.remove(this);
      }

      updateGlobalEntryCounts();

      try {
        if (mfw != null) {
          // compaction must not have finished successfully, so close its output file
          try {
            mfw.close();
          } finally {
            if (!fs.deleteRecursively(outputFile.getPath())) {
              if (fs.exists(outputFile.getPath())) {
                log.error("Unable to delete {}", outputFile);
              }
            }
          }
        }
      } catch (IOException | RuntimeException e) {
        /*
         * If compaction is enabled then the compaction didn't finish due to a real error condition
         * so log any errors on the output file close as a warning. However, if not enabled, then
         * the compaction was canceled due to something like tablet split, user cancellation, or
         * table deletion which is not an error so log any errors on output file close as a debug as
         * this may happen due to an InterruptedException thrown due to the cancellation.
         */
        if (env.isCompactionEnabled()) {
          log.warn("{}", e.getMessage(), e);
        } else {
          log.debug("{}", e.getMessage(), e);
        }
      }
    }
  }

  private List<SortedKeyValueIterator<Key,Value>> openMapDataFiles(
      ArrayList<FileSKVIterator> readers, EnumSet<FilePrefix> dropCacheFilePrefixes)
      throws IOException {

    List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(filesToCompact.size());

    for (StoredTabletFile dataFile : filesToCompact.keySet()) {
      try {

        FileOperations fileFactory = FileOperations.getInstance();
        FileSystem fs = this.fs.getFileSystemByPath(dataFile.getPath());
        FileSKVIterator reader;

        boolean dropCacheBehindCompactionInputFile = false;
        if (dropCacheFilePrefixes.containsAll(EnumSet.allOf(FilePrefix.class))) {
          dropCacheBehindCompactionInputFile = true;
        } else {
          FilePrefix type = FilePrefix.fromFileName(dataFile.getFileName());
          if (dropCacheFilePrefixes.contains(type)) {
            dropCacheBehindCompactionInputFile = true;
          }
        }

        ReaderBuilder readerBuilder =
            fileFactory.newReaderBuilder().forFile(dataFile, fs, fs.getConf(), cryptoService)
                .withTableConfiguration(acuTableConf);
        if (dropCacheBehindCompactionInputFile) {
          readerBuilder.dropCachesBehind();
        }
        reader = readerBuilder.build();

        readers.add(reader);

        InterruptibleIterator iter = new ProblemReportingIterator(extent.tableId(),
            dataFile.getNormalizedPathStr(), false, reader);
        iter.setInterruptFlag(interruptFlag);

        iter = filesToCompact.get(dataFile).wrapFileIterator(iter);

        iters.add(iter);

      } catch (Exception e) {
        TabletLogger.fileReadFailed(dataFile.toString(), extent, e);
        // failed to open some data file... close the ones that were opened
        for (FileSKVIterator reader : readers) {
          try {
            reader.close();
          } catch (Exception e2) {
            log.warn("Failed to close data file", e2);
          }
        }

        readers.clear();

        if (e instanceof IOException) {
          throw (IOException) e;
        }
        throw new IOException("Failed to open data files", e);
      }
    }

    return iters;
  }

  private void compactLocalityGroup(String lgName, Set<ByteSequence> columnFamilies,
      boolean inclusive, FileSKVWriter mfw, CompactionStats majCStats,
      EnumSet<FilePrefix> dropCacheFilePrefixes)
      throws IOException, CompactionCanceledException, ReflectiveOperationException {
    ArrayList<FileSKVIterator> readers = new ArrayList<>(filesToCompact.size());
    Span compactSpan = TraceUtil.startSpan(this.getClass(), "compact");
    try (Scope span = compactSpan.makeCurrent()) {
      long entriesCompacted = 0;
      List<SortedKeyValueIterator<Key,Value>> iters =
          openMapDataFiles(readers, dropCacheFilePrefixes);

      if (env.getIteratorScope() == IteratorScope.minc) {
        iters.add(env.getMinCIterator());
      }

      CountingIterator citr =
          new CountingIterator(new MultiIterator(iters, extent.toDataRange()), currentEntriesRead);
      SortedKeyValueIterator<Key,Value> delIter =
          DeletingIterator.wrap(citr, propagateDeletes, DeletingIterator.getBehavior(acuTableConf));
      ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(delIter);

      SystemIteratorEnvironment iterEnv =
          env.createIteratorEnv(context, acuTableConf, getExtent().tableId());

      SortedKeyValueIterator<Key,Value> itr = iterEnv.getTopLevelIterator(IteratorConfigUtil
          .convertItersAndLoad(env.getIteratorScope(), cfsi, acuTableConf, iterators, iterEnv));
      itr.seek(extent.toDataRange(), columnFamilies, inclusive);

      if (inclusive) {
        mfw.startNewLocalityGroup(lgName, columnFamilies);
      } else {
        mfw.startDefaultLocalityGroup();
      }

      DetectionScope scope =
          env.getIteratorScope() == IteratorScope.minc ? DetectionScope.MINC : DetectionScope.MAJC;
      Span writeSpan = TraceUtil.startSpan(this.getClass(), "write");
      try (Scope write = writeSpan.makeCurrent()) {
        while (itr.hasTop() && env.isCompactionEnabled()) {

          while (context.getLowMemoryDetector().isRunningLowOnMemory(context, scope, () -> {
            return !extent.isMeta();
          }, () -> {
            log.info("Pausing compaction because low on memory, extent: {}", extent);
            timesPaused.incrementAndGet();
            if (scope == DetectionScope.MINC) {
              metrics.incrementMinCPause();
            } else {
              metrics.incrementMajCPause();
            }
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(
                  "Interrupted while waiting for low memory condition to resolve", e);
            }
          })) {}

          mfw.append(itr.getTopKey(), itr.getTopValue());
          itr.next();
          entriesCompacted++;

          if (entriesCompacted % 1024 == 0) {
            // Periodically update stats, do not want to do this too often since its volatile
            currentEntriesWritten.addAndGet(1024);
          }
        }

        if (itr.hasTop() && !env.isCompactionEnabled()) {
          // cancel major compaction operation
          try {
            try {
              mfw.close();
            } catch (IOException e) {
              log.warn("{}", e.getMessage());
              log.debug("{}", e.getMessage(), e);
            }
            fs.deleteRecursively(outputFile.getPath());
          } catch (Exception e) {
            log.warn("Failed to delete Canceled compaction output file {}", outputFile, e);
          }
          throw new CompactionCanceledException();
        }

      } finally {
        CompactionStats lgMajcStats =
            new CompactionStats(citr.getCount(), entriesCompacted, timesPaused.get());
        majCStats.add(lgMajcStats);
        writeSpan.end();
      }
    } catch (IOException | CompactionCanceledException e) {
      TraceUtil.setException(compactSpan, e, true);
      throw e;
    } finally {
      // close sequence files opened
      for (FileSKVIterator reader : readers) {
        try {
          reader.close();
        } catch (Exception e) {
          log.warn("Failed to close data file", e);
        }
      }
      compactSpan.end();
    }
  }

  Collection<StoredTabletFile> getFilesToCompact() {
    return filesToCompact.keySet();
  }

  boolean hasIMM() {
    return env.getIteratorScope() == IteratorScope.minc;
  }

  boolean willPropagateDeletes() {
    return propagateDeletes;
  }

  long getEntriesRead() {
    return currentEntriesRead.get();
  }

  long getEntriesWritten() {
    return currentEntriesWritten.get();
  }

  long getTimesPaused() {
    return timesPaused.get();
  }

  /**
   * @return the duration since {@link #call()} was called
   */
  Duration getAge() {
    if (startTime == null) {
      // call() has not been called yet
      return Duration.ZERO;
    }
    return startTime.elapsed();
  }

  Iterable<IteratorSetting> getIterators() {
    return this.iterators;
  }

  public TCompactionReason getReason() {
    return env.getReason();
  }

}
