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
import java.util.Collections;
import java.util.ConcurrentModificationException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a transaction log that will maintain the last N transactions. It is used to be able to
 * log and review the transactions when issues are detected. This class is thread safe. The inner
 * classes however are not.
 */
public class TabletTransactionLog {
  private static final Logger logger = LoggerFactory.getLogger(TabletTransactionLog.class);
  // The tablet extent for which we are logging
  private final KeyExtent extent;
  // The max size of the log
  private final AccumuloConfiguration.Deriver<MaxLogSize> maxSize;
  // is the transaction log enabled
  private final AccumuloConfiguration.Deriver<LogEnabled> enabled;
  // The current log
  private final TransactionLog log;

  public TabletTransactionLog(KeyExtent extent, Set<StoredTabletFile> initialFiles,
      TableConfiguration configuration) {
    this.extent = extent;
    this.maxSize = configuration.newDeriver(MaxLogSize::new);
    this.enabled = configuration.newDeriver(LogEnabled::new);
    this.log = new TransactionLog(initialFiles);
  }

  /**
   * Get the max size of the transaction log currently configured.
   *
   * @return the max size
   */
  private int getMaxSize() {
    return maxSize.derive().getMaxSize();
  }

  /**
   * Is this log enabled
   *
   * @return true if enabled.
   */
  private boolean isEnabled() {
    return enabled.derive().isEnabled();
  }

  /**
   * Get the date of the first transaction in the log
   *
   * @return the initial date
   */
  public Date getInitialDate() {
    return this.log.getInitialDate();
  }

  /**
   * Get the expected list of files after all transactions are applied the the initial set.
   */
  public Set<StoredTabletFile> getInitialFiles() {
    return this.log.getInitialFiles();
  }

  /**
   * Get the list of transactions.
   *
   * @return the list of transactions
   */
  public List<TabletTransaction> getTransactions() {
    return this.log.getTransactions();
  }

  /**
   * Is the transaction log empty
   *
   * @return true if empty
   */
  public boolean isEmpty() {
    return this.log.isEmpty();
  }

  /**
   * Get the current list of expected files after all transactions are applied.
   *
   * @return the list of expected files
   */
  public Set<StoredTabletFile> getExpectedFiles() {
    return this.log.getExpectedFiles();
  }

  /**
   * Add a compaction transaction.
   *
   * @param files The files that were compacted
   * @param output The destination file
   */
  public void compacted(Set<StoredTabletFile> files, Optional<StoredTabletFile> output,
      Set<StoredTabletFile> newFiles) {
    addTransaction(new TabletTransaction.Compacted(files, output), newFiles);
  }

  /**
   * Add a flush transaction
   *
   * @param newDatafile The new flushed file
   */
  public void flushed(Optional<StoredTabletFile> newDatafile, Set<StoredTabletFile> newFiles) {
    addTransaction(new TabletTransaction.Flushed(newDatafile), newFiles);
  }

  /**
   * Add a bulk import transaction
   *
   * @param file the new bulk import file
   */
  public void bulkImported(StoredTabletFile file, Set<StoredTabletFile> newFiles) {
    addTransaction(new TabletTransaction.BulkImported(file), newFiles);
  }

  /**
   * Add a transaction to the log. This will trim the size of the log if needed.
   *
   * @param transaction The transaction to add
   */
  private synchronized void addTransaction(TabletTransaction transaction,
      Set<StoredTabletFile> newFiles) {
    if (isEnabled()) {
      this.log.setCapacity(getMaxSize());
      this.log.addTransaction(transaction);
      checkTransactionLog(newFiles);
    } else {
      this.log.reset(newFiles);
    }
  }

  /**
   * Dump the transaction log
   */
  public void logTransactions() {
    logger.error("Operation log: {}", dumpLog());
  }

  /**
   * Check the transaction log against the expected files. Log the transactions if they do not
   * match.
   *
   * @param files the current set of files
   */
  private void checkTransactionLog(Set<StoredTabletFile> files) {
    Set<StoredTabletFile> expected = log.getExpectedFiles();
    if (!expected.equals(files)) {
      logger.error("In-memory files {} do not match transaction log {}", new TreeSet<>(files),
          new TreeSet<>(expected));
      logTransactions();
    }
  }

  /**
   * Clear the log
   */
  public synchronized void clearLog() {
    this.log.clear();
  }

  /**
   * Get a string that provides a list of the transactions.
   *
   * @return a log dump
   */
  public String dumpLog() {
    return this.log.dumpLog(extent, false);
  }

  @Override
  public String toString() {
    return dumpLog();
  }

  /**
   * A transaction log consists of the original file set and its timestamp, a set of transactions,
   * and the final set of files after applying the transations. The modification methods of this
   * class are NOT thread safe. However the read methods can be called concurrently with the write
   * methods.
   */
  private static class TransactionLog {
    private static final String DATE_FORMAT = "yyyyMMdd'T'HH:mm:ss.SSS";
    private volatile long updateCount;
    // The time stamp of the initial file set
    private volatile long initialTs;
    // the initial file set
    private StoredTabletFile[] initialFiles;
    // the transactions
    private Ring<TabletTransaction> tabletLog = new Ring<>(0);
    // the final file set derived be applying the transactions to the initial file set
    private StoredTabletFile[] finalFiles;

    public TransactionLog(Set<StoredTabletFile> files) {
      reset(files);
    }

    public void reset(Set<StoredTabletFile> files) {
      this.initialTs = System.currentTimeMillis();
      this.initialFiles = files.toArray(new StoredTabletFile[0]);
      this.finalFiles = this.initialFiles;
      if (tabletLog.capacity() != 0) {
        this.tabletLog = new Ring<>(0);
      }
      this.updateCount = tabletLog.getUpdateCount();
    }

    /**
     * Update the capacity of the ring.
     *
     * @param capacity The new capacity
     */
    public void setCapacity(int capacity) {

      if (capacity > Ring.MAX_ALLOWED_SIZE) {
        logger.warn(
            "Attempting to set transaction log capacity larger than max allowed size of {}, capping at max allowed size",
            Ring.MAX_ALLOWED_SIZE);
        capacity = Ring.MAX_ALLOWED_SIZE;
      }

      // short circuit if not change
      if (this.tabletLog.capacity() == capacity) {
        return;
      }

      // quick update setting capacity to 0
      if (capacity == 0) {
        this.tabletLog = new Ring<>(0);
        this.initialTs = System.currentTimeMillis();
        this.initialFiles = this.finalFiles;
        this.updateCount = this.tabletLog.getUpdateCount();
        return;
      }

      long initialTs = 0;
      Set<StoredTabletFile> initialFileSet = null;
      Ring<TabletTransaction> newTabletLog = new Ring<>(capacity);
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

    /**
     * Clear the log, keeping the final list of files and timestamp.
     */
    public void clear() {
      this.tabletLog.clear();
      this.initialTs = System.currentTimeMillis();
      this.initialFiles = this.finalFiles;
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

    /**
     * Get the date of the initial files.
     *
     * @return the date
     */
    Date getInitialDate() {
      return Date.from(Instant.ofEpochMilli(initialTs));
    }

    /**
     * Get the expected list of files after all transactions are applied the the initial set.
     */
    Set<StoredTabletFile> getInitialFiles() {
      return new HashSet<>(Arrays.asList(initialFiles));
    }

    /**
     * Get the list of transactions. This can be called concurrently with the modification methods.
     *
     * @return the list of transactions
     */
    List<TabletTransaction> getTransactions() {
      while (true) {
        try {
          return tabletLog.toList();
        } catch (ConcurrentModificationException cme) {
          // try again.
        }
      }
    }

    /**
     * Get the expected list of files after all transactions are applied the the initial set.
     */
    Set<StoredTabletFile> getExpectedFiles() {
      return new HashSet<>(Arrays.asList(finalFiles));
    }

    /**
     * Is the log empty
     *
     * @return true if empty
     */
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
      long initialTs = this.initialTs;
      StoredTabletFile[] initialFiles = this.initialFiles;
      StoredTabletFile[] finalFiles = this.finalFiles;
      List<TabletTransaction> transactions = Collections.emptyList();
      boolean consistent = false;
      do {
        long updateCount = this.updateCount;
        try {
          initialTs = this.initialTs;
          initialFiles = this.initialFiles;
          finalFiles = this.finalFiles;
          transactions = this.tabletLog.toList();
          consistent =
              (updateCount == this.updateCount && updateCount == this.tabletLog.getUpdateCount());
        } catch (ConcurrentModificationException e) {
          // try again
        }
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
      maxSize = config.getCount(Property.TABLE_TRANSACTION_LOG_MAX_SIZE);
    }

    public int getMaxSize() {
      return maxSize;
    }
  }

  /**
   * The max size of the log as derived from the configuration
   */
  private static class LogEnabled {
    private final boolean enabled;

    public LogEnabled(AccumuloConfiguration config) {
      enabled = config.getBoolean(Property.TABLE_TRANSACTION_LOG_ENABLED);
    }

    public boolean isEnabled() {
      return enabled;
    }
  }

  /**
   * A simple implementation of a ring buffer. Note that this is NOT thread safe. However the toList
   * method can be called concurrently and it will throw a ConcurrentModificationException if any
   * modification is made concurrently. Hence the toList method can be called without having to
   * synchronize with the write methods provided the exception is handled appropriately.
   */
  static class Ring<T> {
    private final Object[] ring;
    private volatile int first;
    private volatile int last;
    public static final int OVERRUN_THRESHOLD = (Integer.MAX_VALUE / 2);
    public static final int MAX_ALLOWED_SIZE = (OVERRUN_THRESHOLD / 2 - 1);

    /**
     * Create a new ring with the specified capacity
     *
     * @param capacity The capacity of the ring
     * @throws IllegalArgumentException if the size is too large
     */
    public Ring(int capacity) {
      if (capacity > MAX_ALLOWED_SIZE) {
        throw new IllegalArgumentException("Size cannot be larger than " + MAX_ALLOWED_SIZE);
      }
      ring = new Object[capacity];
      first = 0;
      last = -1;
    }

    /**
     * Add an item to the ring
     *
     * @param object The object to add
     * @return the item removed from the back of the ring if any
     */
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

    /**
     * The size of the ring
     *
     * @return the size
     */
    public int size() {
      return last - first + 1;
    }

    /**
     * The capacity of the ring
     *
     * @return the capacity
     */
    public int capacity() {
      return ring.length;
    }

    /**
     * Is the ring empty
     *
     * @return true if the ring is empty
     */
    public boolean isEmpty() {
      return last < first;
    }

    /**
     * Clear/empty the ring.
     */
    public void clear() {
      last = first - 1;
    }

    /**
     * Return the list of objects in the ring.
     *
     * @return the list of objects
     * @throws ConcurrentModificationException if the list is modified while gathering the list
     */
    @SuppressWarnings("unchecked")
    public List<T> toList() throws ConcurrentModificationException {
      Object[] data = null;
      long updateCount = getUpdateCount();
      int lastPos = last;
      int firstPos = first;
      // if either is over the threshold, then we must be in the middle of a modification
      // but before the overrun threshold is called. try again.
      int size = lastPos - firstPos + 1;
      if (size >= 0 && size <= ring.length && lastPos <= OVERRUN_THRESHOLD
          && firstPos <= OVERRUN_THRESHOLD) {
        data = new Object[size];

        int index = 0;
        for (int i = firstPos; i <= lastPos; i++) {
          data[index++] = ring[i % ring.length];
        }
        // if the update count is not the same, then we are in the process of modifying the ring
        if (updateCount != getUpdateCount()) {
          throw new ConcurrentModificationException("Update count changed");
        }
      } else {
        throw new ConcurrentModificationException("Overrun threshold detected");
      }
      return (List<T>) List.of(data);
    }

    /**
     * This will return a value that can be used to determine when changes have made to the ring.
     *
     * @return first + last
     */
    public long getUpdateCount() {
      return first + last;
    }

    /**
     * This will adjust the first and last values to ensure we stay under the overrun threshold
     */
    private void avoidOverrun() {
      if (last > OVERRUN_THRESHOLD) {
        // start with max adjustment we can make with is the minimum of the positions
        int adjustment = Math.min(first, last);
        // then adjust that amount to ensure we keep the same position in the ring.
        adjustment -= adjustment % ring.length;
        // modify the first and then the last so that this can be detected in the toList call
        first -= adjustment;
        last -= adjustment;
      } else if (first > OVERRUN_THRESHOLD) {
        // start with max adjustment we can make with is the minimum of the positions
        int adjustment = Math.min(first, last);
        // then adjust that amount to ensure we keep the same position in the ring.
        adjustment -= adjustment % ring.length;
        // modify the last and then the first so that this can be detected in the toList call
        last -= adjustment;
        first -= adjustment;
      }
    }

    /**
     * The first position (used in tests)
     *
     * @return first
     */
    int first() {
      return first;
    }

    /**
     * The last position (used in tests)
     *
     * @return last
     */
    int last() {
      return last;
    }

  }
}
