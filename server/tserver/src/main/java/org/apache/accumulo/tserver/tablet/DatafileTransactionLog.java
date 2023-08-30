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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.server.conf.TableConfiguration;

public class DatafileTransactionLog {
  private final KeyExtent extent;
  private final AccumuloConfiguration.Deriver<MaxLogSize> maxSize;
  private TransactionLog log;

  public DatafileTransactionLog(KeyExtent extent, Set<StoredTabletFile> initialFiles,
      TableConfiguration configuration) {
    this.extent = extent;
    this.maxSize = configuration.newDeriver(MaxLogSize::new);
    this.log = new TransactionLog(initialFiles);
  }

  public void reset(Set<StoredTabletFile> files) {
    this.log = new TransactionLog(files);
  }

  private int getMaxSize() {
    return maxSize.derive().getMaxSize();
  }

  public Date getInitialDate() {
    return this.log.getInitialDate();
  }

  public List<DatafileTransaction> getTransactions() {
    return this.log.getTransactions();
  }

  public boolean isExpectedFiles(Set<StoredTabletFile> files) {
    return this.log.isExpectedFiles(files);
  }

  public void compacted(Set<StoredTabletFile> files, Optional<StoredTabletFile> output) {
    addTransaction(new DatafileTransaction.Compacted(files, output));
  }

  public void flushed(Optional<StoredTabletFile> newDatafile) {
    addTransaction(new DatafileTransaction.Flushed(newDatafile));
  }

  public void bulkImported(StoredTabletFile file) {
    addTransaction(new DatafileTransaction.BulkImported(file));
  }

  private void addTransaction(DatafileTransaction transaction) {
    TransactionLog log = this.log;
    TransactionLog newLog = new TransactionLog(log, transaction, getMaxSize());
    while (!updateLog(log, newLog)) {
      log = this.log;
      newLog = new TransactionLog(log, transaction, getMaxSize());
    }
  }

  /**
   * This is the only place the log actually gets updated, minimizing synchronization
   *
   * @param origLog The original log used to determine whether the log was changed out from
   *        underneath us
   * @param newLog The new log
   * @return true if we were able to update the log, false otherwise
   */
  private synchronized boolean updateLog(TransactionLog origLog, TransactionLog newLog) {
    // only if this log is the original log as expected to we update
    if (this.log == origLog) {
      this.log = newLog;
      return true;
    }
    return false;
  }

  public String dumpLog() {
    return this.log.dumpLog(extent);
  }

  @Override
  public String toString() {
    return dumpLog();
  }

  private static class MaxLogSize {
    private final int maxSize;

    public MaxLogSize(AccumuloConfiguration config) {
      maxSize = config.getCount(Property.TABLE_OPERATION_LOG_MAX_SIZE);
    }

    public int getMaxSize() {
      return maxSize;
    }
  }

  /**
   * A transaction log consists of the original file set and its timestamp, a set of transactions,
   * and the final set of files after applying the transations. This class is immutable.
   */
  private static class TransactionLog {
    private final long initialTs;
    private final StoredTabletFile[] initialFiles;
    private final DatafileTransaction[] tabletLog;
    private final StoredTabletFile[] finalFiles;

    public TransactionLog(Set<StoredTabletFile> files) {
      this(files.toArray(new StoredTabletFile[0]));
    }

    private TransactionLog(StoredTabletFile[] files) {
      this(System.currentTimeMillis(), files, new DatafileTransaction[0], files);
    }

    private TransactionLog(long initialTs, StoredTabletFile[] initialFiles,
        DatafileTransaction[] tabletLog, StoredTabletFile[] finalFiles) {
      this.initialTs = initialTs;
      this.initialFiles = initialFiles;
      this.tabletLog = tabletLog;
      this.finalFiles = finalFiles;
    }

    public TransactionLog(TransactionLog log, DatafileTransaction transaction, int maxSize) {
      if (log.tabletLog.length < maxSize) {
        this.initialTs = log.initialTs;
        this.initialFiles = log.initialFiles;
        this.tabletLog = Arrays.copyOf(log.tabletLog, log.tabletLog.length + 1);
        this.tabletLog[log.tabletLog.length] = transaction;
        this.finalFiles = applyTransaction(log.finalFiles, transaction);
      } else if (maxSize == 0) {
        this.initialTs = transaction.ts;
        this.initialFiles = this.finalFiles = applyTransaction(log.finalFiles, transaction);
        this.tabletLog = new DatafileTransaction[0];
      } else {
        this.tabletLog = new DatafileTransaction[maxSize];
        System.arraycopy(log.tabletLog, log.tabletLog.length - maxSize + 1, this.tabletLog, 0,
            maxSize - 1);
        this.tabletLog[maxSize - 1] = transaction;
        this.initialFiles =
            applyTransactions(log.initialFiles, log.tabletLog, log.tabletLog.length - maxSize + 1);
        this.initialTs = log.tabletLog[log.tabletLog.length - maxSize].ts;
        this.finalFiles = applyTransaction(log.finalFiles, transaction);
      }
    }

    private static StoredTabletFile[] applyTransaction(StoredTabletFile[] files,
        DatafileTransaction transaction) {
      Set<StoredTabletFile> newFiles = new HashSet<>(Arrays.asList(files));
      transaction.apply(newFiles);
      return newFiles.toArray(new StoredTabletFile[0]);
    }

    private static StoredTabletFile[] applyTransactions(StoredTabletFile[] files,
        DatafileTransaction[] transactions, int length) {
      Set<StoredTabletFile> newFiles = new HashSet<>(Arrays.asList(files));
      for (int i = 0; i < length; i++) {
        transactions[i].apply(newFiles);
      }
      return newFiles.toArray(new StoredTabletFile[0]);
    }

    Date getInitialDate() {
      return Date.from(Instant.ofEpochMilli(initialTs));
    }

    SortedSet<StoredTabletFile> getSortedInitialFiles() {
      return Collections.unmodifiableSortedSet(new TreeSet<>(Arrays.asList(this.finalFiles)));
    }

    List<DatafileTransaction> getTransactions() {
      return Collections.unmodifiableList(Arrays.asList(tabletLog));
    }

    boolean isExpectedFiles(Set<StoredTabletFile> expected) {
      return new HashSet<>(Arrays.asList(finalFiles)).equals(new HashSet<>(expected));
    }

    SortedSet<StoredTabletFile> getSortedFinalFiles() {
      return Collections.unmodifiableSortedSet(new TreeSet<>(Arrays.asList(this.finalFiles)));
    }

    String dumpLog(KeyExtent extent) {
      StringBuilder builder = new StringBuilder();
      builder.append(String.format("%s: Initial files for %s : %s\n", getInitialDate(), extent,
          getSortedInitialFiles()));
      getTransactions().stream().forEach(t -> builder.append(t).append('\n'));
      builder.append("Final files: ").append(getSortedFinalFiles());
      return builder.toString();
    }
  }
}
