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
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.system.TimeSettingIterator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReportingIterator;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.MinorCompactionReason;
import org.apache.accumulo.tserver.TabletIteratorEnvironment;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Compactor implements Callable<CompactionStats> {
  private static final Logger log = LoggerFactory.getLogger(Compactor.class);
  private static final AtomicLong nextCompactorID = new AtomicLong(0);

  public static class CompactionCanceledException extends Exception {
    private static final long serialVersionUID = 1L;
  }

  public interface CompactionEnv {

    boolean isCompactionEnabled();

    IteratorScope getIteratorScope();

    RateLimiter getReadLimiter();

    RateLimiter getWriteLimiter();
  }

  private final Map<FileRef,DataFileValue> filesToCompact;
  private final InMemoryMap imm;
  private final FileRef outputFile;
  private final boolean propogateDeletes;
  private final AccumuloConfiguration acuTableConf;
  private final CompactionEnv env;
  private final VolumeManager fs;
  protected final KeyExtent extent;
  private final List<IteratorSetting> iterators;

  // things to report
  private String currentLocalityGroup = "";
  private final long startTime;

  private int reason;

  private final AtomicLong entriesRead = new AtomicLong(0);
  private final AtomicLong entriesWritten = new AtomicLong(0);
  private final DateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

  // a unique id to identify a compactor
  private final long compactorID = nextCompactorID.getAndIncrement();
  protected volatile Thread thread;
  private final AccumuloServerContext context;

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

  protected static final Set<Compactor> runningCompactions = Collections.synchronizedSet(new HashSet<Compactor>());

  public static List<CompactionInfo> getRunningCompactions() {
    ArrayList<CompactionInfo> compactions = new ArrayList<>();

    synchronized (runningCompactions) {
      for (Compactor compactor : runningCompactions) {
        compactions.add(new CompactionInfo(compactor));
      }
    }

    return compactions;
  }

  public Compactor(AccumuloServerContext context, Tablet tablet, Map<FileRef,DataFileValue> files, InMemoryMap imm, FileRef outputFile,
      boolean propogateDeletes, CompactionEnv env, List<IteratorSetting> iterators, int reason, AccumuloConfiguration tableConfiguation) {
    this.context = context;
    this.extent = tablet.getExtent();
    this.fs = tablet.getTabletServer().getFileSystem();
    this.acuTableConf = tableConfiguation;
    this.filesToCompact = files;
    this.imm = imm;
    this.outputFile = outputFile;
    this.propogateDeletes = propogateDeletes;
    this.env = env;
    this.iterators = iterators;
    this.reason = reason;

    startTime = System.currentTimeMillis();
  }

  public VolumeManager getFileSystem() {
    return fs;
  }

  KeyExtent getExtent() {
    return extent;
  }

  String getOutputFile() {
    return outputFile.toString();
  }

  MajorCompactionReason getMajorCompactionReason() {
    return MajorCompactionReason.values()[reason];
  }

  @Override
  public CompactionStats call() throws IOException, CompactionCanceledException {

    FileSKVWriter mfw = null;

    CompactionStats majCStats = new CompactionStats();

    boolean remove = runningCompactions.add(this);

    clearStats();

    final Path outputFilePath = outputFile.path();
    final String outputFilePathName = outputFilePath.toString();
    String oldThreadName = Thread.currentThread().getName();
    String newThreadName = "MajC compacting " + extent.toString() + " started " + dateFormatter.format(new Date()) + " file: " + outputFile;
    Thread.currentThread().setName(newThreadName);
    thread = Thread.currentThread();
    try {
      FileOperations fileFactory = FileOperations.getInstance();
      FileSystem ns = this.fs.getVolumeByPath(outputFilePath).getFileSystem();
      mfw = fileFactory.newWriterBuilder().forFile(outputFilePathName, ns, ns.getConf()).withTableConfiguration(acuTableConf)
          .withRateLimiter(env.getWriteLimiter()).build();

      Map<String,Set<ByteSequence>> lGroups;
      try {
        lGroups = LocalityGroupUtil.getLocalityGroups(acuTableConf);
      } catch (LocalityGroupConfigurationError e) {
        throw new IOException(e);
      }

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
      mfw = null; // set this to null so we do not try to close it again in finally if the close fails
      try {
        mfwTmp.close(); // if the close fails it will cause the compaction to fail
      } catch (IOException ex) {
        if (!fs.deleteRecursively(outputFile.path())) {
          if (fs.exists(outputFile.path())) {
            log.error("Unable to delete {}", outputFile);
          }
        }
        throw ex;
      }

      log.debug(String.format("Compaction %s %,d read | %,d written | %,6d entries/sec | %,6.3f secs | %,12d bytes | %9.3f byte/sec", extent,
          majCStats.getEntriesRead(), majCStats.getEntriesWritten(), (int) (majCStats.getEntriesRead() / ((t2 - t1) / 1000.0)), (t2 - t1) / 1000.0,
          mfwTmp.getLength(), mfwTmp.getLength() / ((t2 - t1) / 1000.0)));

      majCStats.setFileSize(mfwTmp.getLength());
      return majCStats;
    } catch (IOException e) {
      log.error("{}", e.getMessage(), e);
      throw e;
    } catch (RuntimeException e) {
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
            if (!fs.deleteRecursively(outputFile.path()))
              if (fs.exists(outputFile.path()))
                log.error("Unable to delete {}", outputFile);
          }
        }
      } catch (IOException e) {
        log.warn("{}", e.getMessage(), e);
      } catch (RuntimeException exception) {
        log.warn("{}", exception.getMessage(), exception);
      }
    }
  }

  private List<SortedKeyValueIterator<Key,Value>> openMapDataFiles(String lgName, ArrayList<FileSKVIterator> readers) throws IOException {

    List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(filesToCompact.size());

    for (FileRef mapFile : filesToCompact.keySet()) {
      try {

        FileOperations fileFactory = FileOperations.getInstance();
        FileSystem fs = this.fs.getVolumeByPath(mapFile.path()).getFileSystem();
        FileSKVIterator reader;

        reader = fileFactory.newReaderBuilder().forFile(mapFile.path().toString(), fs, fs.getConf()).withTableConfiguration(acuTableConf)
            .withRateLimiter(env.getReadLimiter()).build();

        readers.add(reader);

        SortedKeyValueIterator<Key,Value> iter = new ProblemReportingIterator(context, extent.getTableId(), mapFile.path().toString(), false, reader);

        if (filesToCompact.get(mapFile).isTimeSet()) {
          iter = new TimeSettingIterator(iter, filesToCompact.get(mapFile).getTime());
        }

        iters.add(iter);

      } catch (Throwable e) {

        ProblemReports.getInstance(context).report(new ProblemReport(extent.getTableId(), ProblemType.FILE_READ, mapFile.path().toString(), e));

        log.warn("Some problem opening map file {} {}", mapFile, e.getMessage(), e);
        // failed to open some map file... close the ones that were opened
        for (FileSKVIterator reader : readers) {
          try {
            reader.close();
          } catch (Throwable e2) {
            log.warn("Failed to close map file", e2);
          }
        }

        readers.clear();

        if (e instanceof IOException)
          throw (IOException) e;
        throw new IOException("Failed to open map data files", e);
      }
    }

    return iters;
  }

  private void compactLocalityGroup(String lgName, Set<ByteSequence> columnFamilies, boolean inclusive, FileSKVWriter mfw, CompactionStats majCStats)
      throws IOException, CompactionCanceledException {
    ArrayList<FileSKVIterator> readers = new ArrayList<>(filesToCompact.size());
    Span span = Trace.start("compact");
    try {
      long entriesCompacted = 0;
      List<SortedKeyValueIterator<Key,Value>> iters = openMapDataFiles(lgName, readers);

      if (imm != null) {
        iters.add(imm.compactionIterator());
      }

      CountingIterator citr = new CountingIterator(new MultiIterator(iters, extent.toDataRange()), entriesRead);
      DeletingIterator delIter = new DeletingIterator(citr, propogateDeletes);
      ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(delIter);

      // if(env.getIteratorScope() )

      TabletIteratorEnvironment iterEnv;
      if (env.getIteratorScope() == IteratorScope.majc)
        iterEnv = new TabletIteratorEnvironment(IteratorScope.majc, !propogateDeletes, acuTableConf);
      else if (env.getIteratorScope() == IteratorScope.minc)
        iterEnv = new TabletIteratorEnvironment(IteratorScope.minc, acuTableConf);
      else
        throw new IllegalArgumentException();

      SortedKeyValueIterator<Key,Value> itr = iterEnv.getTopLevelIterator(IteratorUtil.loadIterators(env.getIteratorScope(), cfsi, extent, acuTableConf,
          iterators, iterEnv));

      itr.seek(extent.toDataRange(), columnFamilies, inclusive);

      if (!inclusive) {
        mfw.startDefaultLocalityGroup();
      } else {
        mfw.startNewLocalityGroup(lgName, columnFamilies);
      }

      Span write = Trace.start("write");
      try {
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
              log.error("{}", e.getMessage(), e);
            }
            fs.deleteRecursively(outputFile.path());
          } catch (Exception e) {
            log.warn("Failed to delete Canceled compaction output file {}", outputFile, e);
          }
          throw new CompactionCanceledException();
        }

      } finally {
        CompactionStats lgMajcStats = new CompactionStats(citr.getCount(), entriesCompacted);
        majCStats.add(lgMajcStats);
        write.stop();
      }

    } finally {
      // close sequence files opened
      for (FileSKVIterator reader : readers) {
        try {
          reader.close();
        } catch (Throwable e) {
          log.warn("Failed to close map file", e);
        }
      }
      span.stop();
    }
  }

  Collection<FileRef> getFilesToCompact() {
    return filesToCompact.keySet();
  }

  boolean hasIMM() {
    return imm != null;
  }

  boolean willPropogateDeletes() {
    return propogateDeletes;
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

  MinorCompactionReason getMinCReason() {
    return MinorCompactionReason.values()[reason];
  }

}
