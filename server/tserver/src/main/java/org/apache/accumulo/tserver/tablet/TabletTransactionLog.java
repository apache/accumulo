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
import java.util.function.Supplier;

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
  private static final String DATE_FORMAT = "yyyyMMdd'T'HH:mm:ss.SSS";
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
  public synchronized List<TabletTransaction> getTransactions() {
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
  public void compacted(final Set<StoredTabletFile> files, final Optional<StoredTabletFile> output,
      final Set<StoredTabletFile> newFiles) {
    addTransaction(() -> new TabletTransaction.Compacted(files, output), newFiles);
  }

  /**
   * Add a flush transaction
   *
   * @param newDatafile The new flushed file
   */
  public void flushed(final Optional<StoredTabletFile> newDatafile,
      final Set<StoredTabletFile> newFiles) {
    addTransaction(() -> new TabletTransaction.Flushed(newDatafile), newFiles);
  }

  /**
   * Add a bulk import transaction
   *
   * @param file the new bulk import file
   */
  public void bulkImported(final StoredTabletFile file, final Set<StoredTabletFile> newFiles) {
    addTransaction(() -> new TabletTransaction.BulkImported(file), newFiles);
  }

  /**
   * Add a transaction to the log. This will trim the size of the log if needed.
   *
   * @param transactionFactory The transaction supplier
   */
  private synchronized void addTransaction(Supplier<TabletTransaction> transactionFactory,
      Set<StoredTabletFile> newFiles) {
    if (isEnabled()) {
      this.log.setCapacity(getMaxSize());
      this.log.addTransaction(transactionFactory.get());
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
   * Return a human readable dump of the log
   *
   * @return A dump of the log
   */
  public String dumpLog() {
    final Date initialTs;
    final Set<StoredTabletFile> initialFiles;
    final Set<StoredTabletFile> finalFiles;
    final List<TabletTransaction> transactions;

    // first lets get a consistent set of files and transactions
    synchronized (this) {
      initialTs = log.getInitialDate();
      initialFiles = log.getInitialFiles();
      finalFiles = log.getExpectedFiles();
      transactions = log.getTransactions();
    }

    // now we can build our dump string
    StringBuilder builder = new StringBuilder();
    SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
    String initialDate = format.format(initialTs);

    builder.append(
        String.format("\n%s: Initial files for %s : %s", initialDate, extent, initialFiles));
    transactions.stream().forEach(t -> builder.append('\n').append(t.toString(format)));
    if (!transactions.isEmpty()) {
      builder.append("\nFinal files: ").append(finalFiles);
    }

    return builder.toString();
  }

  @Override
  public String toString() {
    return dumpLog();
  }

  /**
   * A transaction log consists of the original file set and its timestamp, a set of transactions,
   * and the final set of files after applying the transations. This class is NOT thread safe.
   */
  private static class TransactionLog {
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
    }

    /**
     * Clear the log, keeping the final list of files and timestamp.
     */
    public void clear() {
      this.tabletLog.clear();
      this.initialTs = System.currentTimeMillis();
      this.initialFiles = this.finalFiles;
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
      return new TreeSet<>(Arrays.asList(initialFiles));
    }

    /**
     * Get the list of transactions. This can be called concurrently with the modification methods.
     *
     * @return the list of transactions
     */
    List<TabletTransaction> getTransactions() {
      return tabletLog.toList();
    }

    /**
     * Get the expected list of files after all transactions are applied the the initial set.
     */
    Set<StoredTabletFile> getExpectedFiles() {
      return new TreeSet<>(Arrays.asList(finalFiles));
    }

    /**
     * Is the log empty
     *
     * @return true if empty
     */
    boolean isEmpty() {
      return tabletLog.isEmpty();
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
   * A simple implementation of a ring buffer. Note that this is NOT thread safe.
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
        ring[++last % ring.length] = object;
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
     */
    @SuppressWarnings("unchecked")
    public List<T> toList() {
      Object[] data = new Object[size()];
      int index = 0;
      for (int i = first; i <= last; i++) {
        data[index++] = ring[i % ring.length];
      }
      return (List<T>) List.of(data);
    }

    /**
     * This will adjust the first and last values to ensure we stay under the overrun threshold
     */
    private void avoidOverrun() {
      if (last > OVERRUN_THRESHOLD || first > OVERRUN_THRESHOLD) {
        // start with max adjustment we can make with is the minimum of the positions
        int adjustment = Math.min(first, last);
        // then adjust that amount to ensure we keep the same position in the ring.
        adjustment -= adjustment % ring.length;
        // modify the pointers
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
