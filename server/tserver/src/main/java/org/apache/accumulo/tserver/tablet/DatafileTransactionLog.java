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

/**
 * This is a transaction log that will maintain the last N transactions. It is used to be able to
 * log and review the transactions when issues are detected.
 */
public class DatafileTransactionLog {
  // The tablet extent for which we are logging
  private final KeyExtent extent;
  // The max size of the log
  private final AccumuloConfiguration.Deriver<MaxLogSize> maxSize;
  // The current log
  private TransactionLog log;

  public DatafileTransactionLog(KeyExtent extent, Set<StoredTabletFile> initialFiles,
      TableConfiguration configuration) {
    this.extent = extent;
    this.maxSize = configuration.newDeriver(MaxLogSize::new);
    this.log = new TransactionLog(initialFiles);
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

  /**
   * Add a transaction to the log. This will trim the size of the log if needed.
   *
   * @param transaction The transaction to add
   */
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
    // only if this log is the original log as expected do we update
    if (this.log == origLog) {
      this.log = newLog;
      return true;
    }
    return false;
  }

  /**
   * Get a string that provides a list of the transactions.
   *
   * @return a log dump
   */
  public String dumpLog() {
    return this.log.dumpLog(extent);
  }

  @Override
  public String toString() {
    return dumpLog();
  }

  /**
   * A transaction log consists of the original file set and its timestamp, a set of transactions,
   * and the final set of files after applying the transations. This class is immutable.
   */
  private static class TransactionLog {
    // The time stamp of the initial file set
    private final long initialTs;
    // the initial file set
    private final StoredTabletFile[] initialFiles;
    // the transactions
    private final DatafileTransaction[] tabletLog;
    // the final file set derived be applying the transactions to the initial file set
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

    /**
     * This constructor will be the passed in log, adding the passed in transaction, and trimming
     * the log to maxSize if needed.
     *
     * @param log The starting log
     * @param transaction The new transaction
     * @param maxSize The max transaction log size
     */
    public TransactionLog(TransactionLog log, DatafileTransaction transaction, int maxSize) {
      // if the starting log is smaller than maxSize, then simply add the transaction to the end
      if (log.tabletLog.length < maxSize) {
        this.initialTs = log.initialTs;
        this.initialFiles = log.initialFiles;
        this.tabletLog = Arrays.copyOf(log.tabletLog, log.tabletLog.length + 1);
        this.tabletLog[log.tabletLog.length] = transaction;
        this.finalFiles = applyTransaction(log.finalFiles, transaction);
      } else if (maxSize <= 0) {
        // if the max size is 0, then return a log of 0 size, applying the transaction to the file
        // set
        this.initialTs = transaction.ts;
        this.initialFiles = this.finalFiles = applyTransaction(log.finalFiles, transaction);
        this.tabletLog = new DatafileTransaction[0];
      } else {
        // otherwise we are over the max size limit. Trim the transaction log and apply transactions
        // appropriately to the initial and final file sets.
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

    /**
     * Apply a transaction to the set of files and return an updated file set
     *
     * @param files The initial files
     * @param transaction The transaction
     * @return The final files
     */
    private static StoredTabletFile[] applyTransaction(StoredTabletFile[] files,
        DatafileTransaction transaction) {
      Set<StoredTabletFile> newFiles = new HashSet<>(Arrays.asList(files));
      transaction.apply(newFiles);
      return newFiles.toArray(new StoredTabletFile[0]);
    }

    /**
     * Apply a set of transactions to a set of files and return the update file set
     *
     * @param files The initial files
     * @param transactions The transactions
     * @param length The number of transactions to apply
     * @return The final files
     */
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

    /**
     * Return a human readable dump of the log
     *
     * @param extent The tablet extent
     * @return A dump of the log
     */
    String dumpLog(KeyExtent extent) {
      StringBuilder builder = new StringBuilder();
      builder.append(String.format("%s: Initial files for %s : %s\n", getInitialDate(), extent,
          getSortedInitialFiles()));
      getTransactions().stream().forEach(t -> builder.append(t).append('\n'));
      builder.append("Final files: ").append(getSortedFinalFiles());
      return builder.toString();
    }
  }

  /**
   * The max size of the log as derived from the configuration
   */
  private static class MaxLogSize {
    private final int maxSize;

    public MaxLogSize(AccumuloConfiguration config) {
      maxSize = config.getCount(Property.TABLE_OPERATION_LOG_MAX_SIZE);
    }

    public int getMaxSize() {
      return maxSize;
    }
  }

}
