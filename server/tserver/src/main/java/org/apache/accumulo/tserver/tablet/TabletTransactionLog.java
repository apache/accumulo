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

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.server.conf.TableConfiguration;

/**
 * This is a transaction log that will maintain the last N transactions. It is used to be able to
 * log and review the transactions when issues are detected. The modifications to this log are NOT
 * thread safe. However one can get the list of transactions or dump the log without having to
 * synchronize.
 */
public class TabletTransactionLog {
  // The tablet extent for which we are logging
  private final KeyExtent extent;
  // The max size of the log
  private final AccumuloConfiguration.Deriver<MaxLogSize> maxSize;
  // The current log
  private final TransactionLog log;

  public TabletTransactionLog(KeyExtent extent, Set<StoredTabletFile> initialFiles,
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

  public List<TabletTransaction> getTransactions() {
    return this.log.getTransactions();
  }

  public boolean isEmpty() {
    return this.log.isEmpty();
  }

  public Set<StoredTabletFile> getExpectedFiles() {
    return this.log.getExpectedFiles();
  }

  public void compacted(Set<StoredTabletFile> files, Optional<StoredTabletFile> output) {
    addTransaction(new TabletTransaction.Compacted(files, output));
  }

  public void flushed(Optional<StoredTabletFile> newDatafile) {
    addTransaction(new TabletTransaction.Flushed(newDatafile));
  }

  public void bulkImported(StoredTabletFile file) {
    addTransaction(new TabletTransaction.BulkImported(file));
  }

  /**
   * Add a transaction to the log. This will trim the size of the log if needed.
   *
   * @param transaction The transaction to add
   */
  private void addTransaction(TabletTransaction transaction) {
    this.log.setCapacity(getMaxSize());
    this.log.addTransaction(transaction);
  }

  /**
   * Get a string that provides a list of the transactions.
   *
   * @return a log dump
   */
  public String dumpLog() {
    return this.log.dumpLog(extent, false);
  }

  public void clearLog() {
    this.log.clear();
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
    private static final String DATE_FORMAT = "yyyyMMdd'T'HH:mm:ss.SSS";
    private volatile long updateCount;
    // The time stamp of the initial file set
    private volatile long initialTs;
    // the initial file set
    private StoredTabletFile[] initialFiles;
    // the transactions
    private Ring<TabletTransaction> tabletLog;
    // the final file set derived be applying the transactions to the initial file set
    private StoredTabletFile[] finalFiles;

    public TransactionLog(Set<StoredTabletFile> files) {
      this(files.toArray(new StoredTabletFile[0]));
    }

    private TransactionLog(StoredTabletFile[] files) {
      this(System.currentTimeMillis(), files, new Ring<>(0), files);
    }

    private TransactionLog(long initialTs, StoredTabletFile[] initialFiles,
        Ring<TabletTransaction> tabletLog, StoredTabletFile[] finalFiles) {
      this.initialTs = initialTs;
      this.initialFiles = initialFiles;
      this.tabletLog = tabletLog;
      this.finalFiles = finalFiles;
      this.updateCount = tabletLog.getUpdateCount();
    }

    public void setCapacity(int maxSize) {
      if (this.tabletLog.capacity() == maxSize) {
        return;
      }
      long initialTs = 0;
      Set<StoredTabletFile> initialFileSet = null;
      Ring<TabletTransaction> newTabletLog = new Ring<>(maxSize);
      for (TabletTransaction t : this.tabletLog.toList()) {
        TabletTransaction removed = newTabletLog.add(t);
        if (removed != null) {
          if (initialFileSet == null) {
            initialFileSet = new HashSet<>(Arrays.asList(this.initialFiles));
          }
          initialTs = removed.ts;
          removed.apply(initialFileSet);
        }
      }
      this.tabletLog = newTabletLog;
      if (initialFileSet != null) {
        this.initialTs = initialTs;
        this.initialFiles = initialFileSet.toArray(new StoredTabletFile[0]);
      }
      this.updateCount = this.tabletLog.getUpdateCount();
    }

    public void clear() {
      this.tabletLog.clear();
      this.initialTs = System.currentTimeMillis();
      this.initialFiles = this.finalFiles;
      this.updateCount = this.tabletLog.getUpdateCount();
    }

    public void reset(Set<StoredTabletFile> files) {
      this.tabletLog.clear();
      this.initialTs = System.currentTimeMillis();
      this.initialFiles = this.finalFiles = files.toArray(new StoredTabletFile[0]);
      this.updateCount = this.tabletLog.getUpdateCount();
    }

    /**
     * Add the passed in transaction, adjusting the log size as needed.
     *
     * @param transaction The new transaction
     */
    public void addTransaction(TabletTransaction transaction) {
      TabletTransaction removed = this.tabletLog.add(transaction);
      if (removed != null) {
        this.initialTs = removed.ts;
        this.initialFiles = applyTransaction(this.initialFiles, removed);
      }
      this.finalFiles = applyTransaction(this.finalFiles, transaction);
      this.updateCount = this.tabletLog.getUpdateCount();
    }

    /**
     * Apply a transaction to the set of files and return an updated file set
     *
     * @param files The initial files
     * @param transaction The transaction
     * @return The final files
     */
    private static StoredTabletFile[] applyTransaction(StoredTabletFile[] files,
        TabletTransaction transaction) {
      Set<StoredTabletFile> newFiles = new HashSet<>(Arrays.asList(files));
      transaction.apply(newFiles);
      return newFiles.toArray(new StoredTabletFile[0]);
    }

    Date getInitialDate() {
      return Date.from(Instant.ofEpochMilli(initialTs));
    }

    int getNumTransactions() {
      return tabletLog.size();
    }

    List<TabletTransaction> getTransactions() {
      return tabletLog.toList();
    }

    Set<StoredTabletFile> getExpectedFiles() {
      return new HashSet<>(Arrays.asList(finalFiles));
    }

    boolean isEmpty() {
      return tabletLog.isEmpty();
    }

    /**
     * Return a human readable dump of the log
     *
     * @param extent The tablet extent
     * @return A dump of the log
     */
    String dumpLog(KeyExtent extent, boolean clear) {
      // first lets get a consistent set of files and transactions
      long initialTs;
      StoredTabletFile[] initialFiles;
      List<TabletTransaction> transactions;
      StoredTabletFile[] finalFiles;
      boolean consistent;
      do {
        long updateCount = this.updateCount;
        initialTs = this.initialTs;
        initialFiles = this.initialFiles;
        finalFiles = this.finalFiles;
        transactions = this.tabletLog.toList();
        consistent =
            (updateCount == this.updateCount && updateCount == this.tabletLog.getUpdateCount());
      } while (!consistent);

      // now clear out the log if requested
      if (clear) {
        clear();
      }

      // now we can build our dump string
      StringBuilder builder = new StringBuilder();
      SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
      String initialDate = format.format(Date.from(Instant.ofEpochMilli(initialTs)));

      builder.append(String.format("\n%s: Initial files for %s : %s", initialDate, extent,
          new TreeSet<>(Arrays.asList(initialFiles))));
      transactions.stream().forEach(t -> builder.append('\n').append(t.toString(format)));
      if (!transactions.isEmpty()) {
        builder.append("\nFinal files: ").append(new TreeSet<>(Arrays.asList(finalFiles)));
      }

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

  /**
   * A simple implementation of a ring buffer. Note that this is not thread safe when it comes to
   * modifications. However the read methods can be done concurrently with a write method.
   */
  public static class Ring<T> {
    private final Object[] ring;
    private volatile int first;
    private volatile int last;
    private static final int overrunThreshold = (Integer.MAX_VALUE / 2);

    public Ring(int size) {
      ring = new Object[size];
      first = 0;
      last = -1;
    }

    @SuppressWarnings("unchecked")
    public T add(T object) {
      Object removed = null;
      if (ring.length > 0) {
        if (size() == ring.length) {
          removed = ring[first++ % ring.length];
        }
        int newEnd = last + 1;
        ring[newEnd % ring.length] = object;
        last = newEnd;
      } else {
        removed = object;
      }
      avoidOverrun();
      return (T) removed;
    }

    public int size() {
      return last - first + 1;
    }

    public int capacity() {
      return ring.length;
    }

    public boolean isEmpty() {
      return last < first;
    }

    public void avoidOverrun() {
      if (last > overrunThreshold) {
        int max = Math.max(first, last);
        first -= max;
        last -= max;
      } else if (first > overrunThreshold) {
        int max = Math.max(first, last);
        last -= max;
        first -= max;
      }
    }

    public void clear() {
      last = -1;
      first = 0;
    }

    @SuppressWarnings("unchecked")
    public List<T> toList() {
      Object[] data = null;
      // consistency is defined as the last position and the first position being
      // the same before and after pulling the list.
      boolean consistent = false;
      do {
        long updateCount = getUpdateCount();
        int lastPos = last;
        int firstPos = first;
        // if either is over the threshold, then we must be in the middle of a modification
        // but before the overrun threshold is called. try again.
        if (lastPos <= overrunThreshold && firstPos <= overrunThreshold) {
          data = new Object[lastPos - firstPos + 1];
          int index = 0;
          for (int i = firstPos; i <= lastPos; i++) {
            data[index++] = ring[i % ring.length];
          }
          consistent = (updateCount == getUpdateCount());
        }
      } while (!consistent);

      return (List<T>) List.of(data);
    }

    public long getUpdateCount() {
      return first + last;
    }

  }
}
