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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileOperations.WriterBuilder;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.core.iteratorsImpl.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.DeletingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.iteratorsImpl.system.TimeSettingIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionReason;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.iterators.SystemIteratorEnvironment;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReportingIterator;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    RateLimiter getReadLimiter();

    RateLimiter getWriteLimiter();

    SystemIteratorEnvironment createIteratorEnv(ServerContext context,
        AccumuloConfiguration acuTableConf, TableId tableId);

    SortedKeyValueIterator<Key,Value> getMinCIterator();

    TCompactionReason getReason();
  }

  private final Map<StoredTabletFile,DataFileValue> filesToCompact;
  private final TabletFile outputFile;
  private final boolean propagateDeletes;
  private final AccumuloConfiguration acuTableConf;
  private final CompactionEnv env;
  private final VolumeManager fs;
  protected final KeyExtent extent;
  private final List<IteratorSetting> iterators;
  private final CryptoService cryptoService;

  // things to report
  private String currentLocalityGroup = "";
  private final long startTime;

  private final AtomicLong entriesRead = new AtomicLong(0);
  private final AtomicLong entriesWritten = new AtomicLong(0);
  private final DateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

  // a unique id to identify a compactor
  private final long compactorID = nextCompactorID.getAndIncrement();
  protected volatile Thread thread;
  private final ServerContext context;

  public long getCompactorID() {
    return compactorID;
  }

  private synchronized void setLocalityGroup(String name) {
    this.currentLocalityGroup = name;
  }

  public synchronized String getCurrentLocalityGroup() {
    return currentLocalityGroup;
  }

  private void clearStats() {
    entriesRead.set(0);
    entriesWritten.set(0);
  }

  protected static final Set<FileCompactor> runningCompactions =
      Collections.synchronizedSet(new HashSet<>());

  public static List<CompactionInfo> getRunningCompactions() {
    ArrayList<CompactionInfo> compactions = new ArrayList<>();

    synchronized (runningCompactions) {
      for (FileCompactor compactor : runningCompactions) {
        compactions.add(new CompactionInfo(compactor));
      }
    }

    return compactions;
  }

  public FileCompactor(ServerContext context, KeyExtent extent,
      Map<StoredTabletFile,DataFileValue> files, TabletFile outputFile, boolean propagateDeletes,
      CompactionEnv env, List<IteratorSetting> iterators, AccumuloConfiguration tableConfiguation,
      CryptoService cs) {
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

    startTime = System.currentTimeMillis();
  }

  public VolumeManager getVolumeManager() {
    return fs;
  }

  public KeyExtent getExtent() {
    return extent;
  }

  protected String getOutputFile() {
    return outputFile.toString();
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
  public CompactionStats call() throws IOException, CompactionCanceledException {

    FileSKVWriter mfw = null;

    CompactionStats majCStats = new CompactionStats();

    boolean remove = runningCompactions.add(this);

    clearStats();

    String oldThreadName = Thread.currentThread().getName();
    String newThreadName = "MajC compacting " + extent + " started "
        + dateFormatter.format(new Date()) + " file: " + outputFile;
    Thread.currentThread().setName(newThreadName);
    thread = Thread.currentThread();
    try {
      FileOperations fileFactory = FileOperations.getInstance();
      FileSystem ns = this.fs.getFileSystemByPath(outputFile.getPath());

      boolean dropCacheBehindMajcOutput = !RootTable.ID.equals(this.extent.tableId())
          && !MetadataTable.ID.equals(this.extent.tableId())
          && acuTableConf.getBoolean(Property.TABLE_MAJC_OUTPUT_DROP_CACHE);

      WriterBuilder outBuilder = fileFactory.newWriterBuilder()
          .forFile(outputFile.getMetaInsert(), ns, ns.getConf(), cryptoService)
          .withTableConfiguration(acuTableConf).withRateLimiter(env.getWriteLimiter());
      if (dropCacheBehindMajcOutput) {
        outBuilder.dropCachesBehind();
      }
      mfw = outBuilder.build();

      Map<String,Set<ByteSequence>> lGroups = getLocalityGroups(acuTableConf);

      long t1 = System.currentTimeMillis();

      HashSet<ByteSequence> allColumnFamilies = new HashSet<>();

      if (mfw.supportsLocalityGroups()) {
        for (Entry<String,Set<ByteSequence>> entry : lGroups.entrySet()) {
          setLocalityGroup(entry.getKey());
          compactLocalityGroup(entry.getKey(), entry.getValue(), true, mfw, majCStats);
          allColumnFamilies.addAll(entry.getValue());
        }
      }

      setLocalityGroup("");
      compactLocalityGroup(null, allColumnFamilies, false, mfw, majCStats);

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
              + " | %,6.3f secs | %,12d bytes | %9.3f byte/sec",
          extent, majCStats.getEntriesRead(), majCStats.getEntriesWritten(),
          (int) (majCStats.getEntriesRead() / ((t2 - t1) / 1000.0)), (t2 - t1) / 1000.0,
          mfwTmp.getLength(), mfwTmp.getLength() / ((t2 - t1) / 1000.0)));

      majCStats.setFileSize(mfwTmp.getLength());
      return majCStats;
    } catch (CompactionCanceledException e) {
      log.debug("Compaction canceled {}", extent);
      throw e;
    } catch (IOException | RuntimeException e) {
      log.error("{}", e.getMessage(), e);
      throw e;
    } finally {
      Thread.currentThread().setName(oldThreadName);
      if (remove) {
        thread = null;
        runningCompactions.remove(this);
      }

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

  private List<SortedKeyValueIterator<Key,Value>>
      openMapDataFiles(ArrayList<FileSKVIterator> readers) throws IOException {

    List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(filesToCompact.size());

    for (TabletFile mapFile : filesToCompact.keySet()) {
      try {

        FileOperations fileFactory = FileOperations.getInstance();
        FileSystem fs = this.fs.getFileSystemByPath(mapFile.getPath());
        FileSKVIterator reader;

        reader = fileFactory.newReaderBuilder()
            .forFile(mapFile.getPathStr(), fs, fs.getConf(), cryptoService)
            .withTableConfiguration(acuTableConf).withRateLimiter(env.getReadLimiter())
            .dropCachesBehind().build();

        readers.add(reader);

        SortedKeyValueIterator<Key,Value> iter = new ProblemReportingIterator(context,
            extent.tableId(), mapFile.getPathStr(), false, reader);

        if (filesToCompact.get(mapFile).isTimeSet()) {
          iter = new TimeSettingIterator(iter, filesToCompact.get(mapFile).getTime());
        }

        iters.add(iter);

      } catch (Exception e) {

        ProblemReports.getInstance(context).report(
            new ProblemReport(extent.tableId(), ProblemType.FILE_READ, mapFile.getPathStr(), e));

        log.warn("Some problem opening map file {} {}", mapFile, e.getMessage(), e);
        // failed to open some map file... close the ones that were opened
        for (FileSKVIterator reader : readers) {
          try {
            reader.close();
          } catch (Exception e2) {
            log.warn("Failed to close map file", e2);
          }
        }

        readers.clear();

        if (e instanceof IOException) {
          throw (IOException) e;
        }
        throw new IOException("Failed to open map data files", e);
      }
    }

    return iters;
  }

  private void compactLocalityGroup(String lgName, Set<ByteSequence> columnFamilies,
      boolean inclusive, FileSKVWriter mfw, CompactionStats majCStats)
      throws IOException, CompactionCanceledException {
    ArrayList<FileSKVIterator> readers = new ArrayList<>(filesToCompact.size());
    Span compactSpan = TraceUtil.startSpan(this.getClass(), "compact");
    try (Scope span = compactSpan.makeCurrent()) {
      long entriesCompacted = 0;
      List<SortedKeyValueIterator<Key,Value>> iters = openMapDataFiles(readers);

      if (env.getIteratorScope() == IteratorScope.minc) {
        iters.add(env.getMinCIterator());
      }

      CountingIterator citr =
          new CountingIterator(new MultiIterator(iters, extent.toDataRange()), entriesRead);
      SortedKeyValueIterator<Key,Value> delIter =
          DeletingIterator.wrap(citr, propagateDeletes, DeletingIterator.getBehavior(acuTableConf));
      ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(delIter);

      // if(env.getIteratorScope() )

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

      Span writeSpan = TraceUtil.startSpan(this.getClass(), "write");
      try (Scope write = writeSpan.makeCurrent()) {
        while (itr.hasTop() && env.isCompactionEnabled()) {
          mfw.append(itr.getTopKey(), itr.getTopValue());
          itr.next();
          entriesCompacted++;

          if (entriesCompacted % 1024 == 0) {
            // Periodically update stats, do not want to do this too often since its volatile
            entriesWritten.addAndGet(1024);
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
        CompactionStats lgMajcStats = new CompactionStats(citr.getCount(), entriesCompacted);
        majCStats.add(lgMajcStats);
        writeSpan.end();
      }

    } catch (Exception e) {
      TraceUtil.setException(compactSpan, e, true);
      throw e;
    } finally {
      // close sequence files opened
      for (FileSKVIterator reader : readers) {
        try {
          reader.close();
        } catch (Exception e) {
          log.warn("Failed to close map file", e);
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
    return entriesRead.get();
  }

  long getEntriesWritten() {
    return entriesWritten.get();
  }

  long getStartTime() {
    return startTime;
  }

  Iterable<IteratorSetting> getIterators() {
    return this.iterators;
  }

  public TCompactionReason getReason() {
    return env.getReason();
  }

}
