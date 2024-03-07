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
package org.apache.accumulo.core.fate;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.FateIdStatus;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.ReadOnlyFateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.zookeeper.FateLock;
import org.apache.accumulo.core.fate.zookeeper.FateLock.FateLockPath;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLock.ServiceLockPath;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A utility to administer FATE operations
 */
public class AdminUtil<T> {
  private static final Logger log = LoggerFactory.getLogger(AdminUtil.class);

  private final boolean exitOnError;

  /**
   * Constructor
   *
   * @param exitOnError <code>System.exit(1)</code> on error if true
   */
  public AdminUtil(boolean exitOnError) {
    this.exitOnError = exitOnError;
  }

  /**
   * FATE transaction status, including lock information.
   */
  public static class TransactionStatus {

    private final FateId fateId;
    private final FateInstanceType instanceType;
    private final TStatus status;
    private final String txName;
    private final List<String> hlocks;
    private final List<String> wlocks;
    private final String top;
    private final long timeCreated;

    private TransactionStatus(FateId fateId, FateInstanceType instanceType, TStatus status,
        String txName, List<String> hlocks, List<String> wlocks, String top, Long timeCreated) {

      this.fateId = fateId;
      this.instanceType = instanceType;
      this.status = status;
      this.txName = txName;
      this.hlocks = Collections.unmodifiableList(hlocks);
      this.wlocks = Collections.unmodifiableList(wlocks);
      this.top = top;
      this.timeCreated = timeCreated;

    }

    /**
     * @return This fate operations transaction id, formatted in the same way as FATE transactions
     *         are in the Accumulo logs.
     */
    public FateId getFateId() {
      return fateId;
    }

    public FateInstanceType getInstanceType() {
      return instanceType;
    }

    public TStatus getStatus() {
      return status;
    }

    /**
     * @return The name of the transaction running.
     */
    public String getTxName() {
      return txName;
    }

    /**
     * @return list of namespace and table ids locked
     */
    public List<String> getHeldLocks() {
      return hlocks;
    }

    /**
     * @return list of namespace and table ids locked
     */
    public List<String> getWaitingLocks() {
      return wlocks;
    }

    /**
     * @return The operation on the top of the stack for this Fate operation.
     */
    public String getTop() {
      return top;
    }

    /**
     * @return The timestamp of when the operation was created in ISO format with UTC timezone.
     */
    public String getTimeCreatedFormatted() {
      return timeCreated > 0 ? new Date(timeCreated).toInstant().atZone(ZoneOffset.UTC)
          .format(DateTimeFormatter.ISO_DATE_TIME) : "ERROR";
    }

    /**
     * @return The unformatted form of the timestamp.
     */
    public long getTimeCreated() {
      return timeCreated;
    }
  }

  public static class FateStatus {

    private final List<TransactionStatus> transactions;
    private final Map<FateId,List<String>> danglingHeldLocks;
    private final Map<FateId,List<String>> danglingWaitingLocks;

    private FateStatus(List<TransactionStatus> transactions,
        Map<FateId,List<String>> danglingHeldLocks, Map<FateId,List<String>> danglingWaitingLocks) {
      this.transactions = Collections.unmodifiableList(transactions);
      this.danglingHeldLocks = danglingHeldLocks;
      this.danglingWaitingLocks = danglingWaitingLocks;
    }

    public List<TransactionStatus> getTransactions() {
      return transactions;
    }

    /**
     * Get locks that are held by non-existent FATE transactions. These are table or namespace
     * locks.
     *
     * @return map where keys are transaction ids and values are a list of table IDs and/or
     *         namespace IDs. The transaction IDs are in the same format as transaction IDs in the
     *         Accumulo logs.
     */
    public Map<FateId,List<String>> getDanglingHeldLocks() {
      return danglingHeldLocks;
    }

    /**
     * Get locks that are waiting to be acquired by non-existent FATE transactions. These are table
     * or namespace locks.
     *
     * @return map where keys are transaction ids and values are a list of table IDs and/or
     *         namespace IDs. The transaction IDs are in the same format as transaction IDs in the
     *         Accumulo logs.
     */
    public Map<FateId,List<String>> getDanglingWaitingLocks() {
      return danglingWaitingLocks;
    }
  }

  /**
   * Returns a list of the FATE transactions, optionally filtered by fate id, status, and fate
   * instance type. This method does not process lock information, if lock information is desired,
   * use {@link #getStatus(ReadOnlyFateStore, ZooReader, ServiceLockPath, Set, EnumSet, EnumSet)}
   *
   * @param fateStores read-only fate stores
   * @param fateIdFilter filter results to include only provided fate transaction ids
   * @param statusFilter filter results to include only provided status types
   * @param typesFilter filter results to include only provided fate instance types
   * @return list of FATE transactions that match filter criteria
   */
  public List<TransactionStatus> getTransactionStatus(
      Map<FateInstanceType,ReadOnlyFateStore<T>> fateStores, Set<FateId> fateIdFilter,
      EnumSet<TStatus> statusFilter, EnumSet<FateInstanceType> typesFilter) {

    FateStatus status = getTransactionStatus(fateStores, fateIdFilter, statusFilter, typesFilter,
        Collections.<FateId,List<String>>emptyMap(), Collections.<FateId,List<String>>emptyMap());

    return status.getTransactions();
  }

  /**
   * Get the FATE transaction status and lock information stored in zookeeper, optionally filtered
   * by fate id, status, and fate instance type
   *
   * @param zs read-only zoostore
   * @param zk zookeeper reader.
   * @param lockPath the zookeeper path for locks
   * @param fateIdFilter filter results to include only provided fate transaction ids
   * @param statusFilter filter results to include only provided status types
   * @param typesFilter filter results to include only provided fate instance types
   * @return a summary container of the fate transactions.
   * @throws KeeperException if zookeeper exception occurs
   * @throws InterruptedException if process is interrupted.
   */
  public FateStatus getStatus(ReadOnlyFateStore<T> zs, ZooReader zk,
      ServiceLock.ServiceLockPath lockPath, Set<FateId> fateIdFilter, EnumSet<TStatus> statusFilter,
      EnumSet<FateInstanceType> typesFilter) throws KeeperException, InterruptedException {
    Map<FateId,List<String>> heldLocks = new HashMap<>();
    Map<FateId,List<String>> waitingLocks = new HashMap<>();

    findLocks(zk, lockPath, heldLocks, waitingLocks);

    return getTransactionStatus(Map.of(FateInstanceType.META, zs), fateIdFilter, statusFilter,
        typesFilter, heldLocks, waitingLocks);
  }

  public FateStatus getStatus(ReadOnlyFateStore<T> as, Set<FateId> fateIdFilter,
      EnumSet<TStatus> statusFilter, EnumSet<FateInstanceType> typesFilter)
      throws KeeperException, InterruptedException {

    return getTransactionStatus(Map.of(FateInstanceType.USER, as), fateIdFilter, statusFilter,
        typesFilter, new HashMap<>(), new HashMap<>());
  }

  public FateStatus getStatus(Map<FateInstanceType,ReadOnlyFateStore<T>> fateStores, ZooReader zk,
      ServiceLock.ServiceLockPath lockPath, Set<FateId> fateIdFilter, EnumSet<TStatus> statusFilter,
      EnumSet<FateInstanceType> typesFilter) throws KeeperException, InterruptedException {
    Map<FateId,List<String>> heldLocks = new HashMap<>();
    Map<FateId,List<String>> waitingLocks = new HashMap<>();

    findLocks(zk, lockPath, heldLocks, waitingLocks);

    return getTransactionStatus(fateStores, fateIdFilter, statusFilter, typesFilter, heldLocks,
        waitingLocks);
  }

  /**
   * Walk through the lock nodes in zookeeper to find and populate held locks and waiting locks.
   *
   * @param zk zookeeper reader
   * @param lockPath the zookeeper path for locks
   * @param heldLocks map for returning transactions with held locks
   * @param waitingLocks map for returning transactions with waiting locks
   * @throws KeeperException if initial lock list cannot be read.
   * @throws InterruptedException if thread interrupt detected while processing.
   */
  private void findLocks(ZooReader zk, final ServiceLock.ServiceLockPath lockPath,
      final Map<FateId,List<String>> heldLocks, final Map<FateId,List<String>> waitingLocks)
      throws KeeperException, InterruptedException {

    // stop with exception if lock ids cannot be retrieved from zookeeper
    List<String> lockedIds = zk.getChildren(lockPath.toString());

    for (String id : lockedIds) {

      try {

        FateLockPath fLockPath = FateLock.path(lockPath + "/" + id);
        List<String> lockNodes =
            FateLock.validateAndSort(fLockPath, zk.getChildren(fLockPath.toString()));

        int pos = 0;
        boolean sawWriteLock = false;

        for (String node : lockNodes) {
          try {
            byte[] data = zk.getData(lockPath + "/" + id + "/" + node);
            // Example data: "READ:<FateId>". FateId contains ':' hence the limit of 2
            String[] lda = new String(data, UTF_8).split(":", 2);
            FateId fateId = FateId.from(lda[1]);

            if (lda[0].charAt(0) == 'W') {
              sawWriteLock = true;
            }

            Map<FateId,List<String>> locks;

            if (pos == 0) {
              locks = heldLocks;
            } else if (lda[0].charAt(0) == 'R' && !sawWriteLock) {
              locks = heldLocks;
            } else {
              locks = waitingLocks;
            }

            locks.computeIfAbsent(fateId, k -> new ArrayList<>()).add(lda[0].charAt(0) + ":" + id);

          } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
          }
          pos++;
        }

      } catch (KeeperException ex) {
        /*
         * could be transient zk error. Log, but try to process rest of list rather than throwing
         * exception here
         */
        log.error("Failed to read locks for " + id + " continuing.", ex);

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw ex;
      }
    }
  }

  /**
   * Returns fate status, possibly filtered
   *
   * @param fateStores read-only access to populated transaction stores.
   * @param fateIdFilter Optional. List of transactions to filter results - if null, all
   *        transactions are returned
   * @param statusFilter Optional. List of status types to filter results - if null, all
   *        transactions are returned.
   * @param typesFilter Optional. List of fate instance types to filter results - if null, all
   *        transactions are returned
   * @param heldLocks populated list of locks held by transaction - or an empty map if none.
   * @param waitingLocks populated list of locks held by transaction - or an empty map if none.
   * @return current fate and lock status
   */
  private FateStatus getTransactionStatus(Map<FateInstanceType,ReadOnlyFateStore<T>> fateStores,
      Set<FateId> fateIdFilter, EnumSet<TStatus> statusFilter,
      EnumSet<FateInstanceType> typesFilter, Map<FateId,List<String>> heldLocks,
      Map<FateId,List<String>> waitingLocks) {
    final List<TransactionStatus> statuses = new ArrayList<>();

    fateStores.forEach((type, store) -> {
      try (Stream<FateId> fateIds = store.list().map(FateIdStatus::getFateId)) {
        fateIds.forEach(fateId -> {

          ReadOnlyFateTxStore<T> txStore = store.read(fateId);

          String txName = (String) txStore.getTransactionInfo(Fate.TxInfo.TX_NAME);

          List<String> hlocks = heldLocks.remove(fateId);

          if (hlocks == null) {
            hlocks = Collections.emptyList();
          }

          List<String> wlocks = waitingLocks.remove(fateId);

          if (wlocks == null) {
            wlocks = Collections.emptyList();
          }

          String top = null;
          ReadOnlyRepo<T> repo = txStore.top();
          if (repo != null) {
            top = repo.getName();
          }

          TStatus status = txStore.getStatus();

          long timeCreated = txStore.timeCreated();

          if (includeByStatus(status, statusFilter) && includeByFateId(fateId, fateIdFilter)
              && includeByInstanceType(fateId.getType(), typesFilter)) {
            statuses.add(new TransactionStatus(fateId, type, status, txName, hlocks, wlocks, top,
                timeCreated));
          }
        });
      }
    });
    return new FateStatus(statuses, heldLocks, waitingLocks);
  }

  private boolean includeByStatus(TStatus status, EnumSet<TStatus> statusFilter) {
    return statusFilter == null || statusFilter.isEmpty() || statusFilter.contains(status);
  }

  private boolean includeByFateId(FateId fateId, Set<FateId> fateIdFilter) {
    return fateIdFilter == null || fateIdFilter.isEmpty() || fateIdFilter.contains(fateId);
  }

  private boolean includeByInstanceType(FateInstanceType type,
      EnumSet<FateInstanceType> typesFilter) {
    return typesFilter == null || typesFilter.isEmpty() || typesFilter.contains(type);
  }

  public void printAll(Map<FateInstanceType,ReadOnlyFateStore<T>> fateStores, ZooReader zk,
      ServiceLock.ServiceLockPath tableLocksPath) throws KeeperException, InterruptedException {
    print(fateStores, zk, tableLocksPath, new Formatter(System.out), null, null, null);
  }

  public void print(Map<FateInstanceType,ReadOnlyFateStore<T>> fateStores, ZooReader zk,
      ServiceLock.ServiceLockPath tableLocksPath, Formatter fmt, Set<FateId> fateIdFilter,
      EnumSet<TStatus> statusFilter, EnumSet<FateInstanceType> typesFilter)
      throws KeeperException, InterruptedException {
    FateStatus fateStatus =
        getStatus(fateStores, zk, tableLocksPath, fateIdFilter, statusFilter, typesFilter);

    for (TransactionStatus txStatus : fateStatus.getTransactions()) {
      fmt.format(
          "%-15s fateId: %s  status: %-18s locked: %-15s locking: %-15s op: %-15s created: %s%n",
          txStatus.getTxName(), txStatus.getFateId(), txStatus.getStatus(), txStatus.getHeldLocks(),
          txStatus.getWaitingLocks(), txStatus.getTop(), txStatus.getTimeCreatedFormatted());
    }
    fmt.format(" %s transactions", fateStatus.getTransactions().size());

    if (!fateStatus.getDanglingHeldLocks().isEmpty()
        || !fateStatus.getDanglingWaitingLocks().isEmpty()) {
      fmt.format("%nThe following locks did not have an associated FATE operation%n");
      for (Entry<FateId,List<String>> entry : fateStatus.getDanglingHeldLocks().entrySet()) {
        fmt.format("fateId: %s  locked: %s%n", entry.getKey(), entry.getValue());
      }

      for (Entry<FateId,List<String>> entry : fateStatus.getDanglingWaitingLocks().entrySet()) {
        fmt.format("fateId: %s  locking: %s%n", entry.getKey(), entry.getValue());
      }
    }
  }

  public boolean prepDelete(Map<FateInstanceType,FateStore<T>> stores, ZooReaderWriter zk,
      ServiceLockPath path, String fateIdStr) {
    if (!checkGlobalLock(zk, path)) {
      return false;
    }

    FateId fateId;
    try {
      fateId = FateId.from(fateIdStr);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
      return false;
    }
    boolean state = false;

    // determine which store to use
    FateStore<T> store = stores.get(fateId.getType());

    FateTxStore<T> txStore = store.reserve(fateId);
    try {
      TStatus ts = txStore.getStatus();
      switch (ts) {
        case UNKNOWN:
          System.out.println("Invalid transaction ID: " + fateId);
          break;

        case SUBMITTED:
        case IN_PROGRESS:
        case NEW:
        case FAILED:
        case FAILED_IN_PROGRESS:
        case SUCCESSFUL:
          System.out.printf("Deleting transaction: %s (%s)%n", fateIdStr, ts);
          txStore.delete();
          state = true;
          break;
      }
    } finally {
      txStore.unreserve(0, TimeUnit.MILLISECONDS);
    }
    return state;
  }

  public boolean prepFail(Map<FateInstanceType,FateStore<T>> stores, ZooReaderWriter zk,
      ServiceLockPath zLockManagerPath, String fateIdStr) {
    if (!checkGlobalLock(zk, zLockManagerPath)) {
      return false;
    }

    FateId fateId;
    try {
      fateId = FateId.from(fateIdStr);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
      return false;
    }
    boolean state = false;

    // determine which store to use
    FateStore<T> store = stores.get(fateId.getType());

    FateTxStore<T> txStore = store.reserve(fateId);
    try {
      TStatus ts = txStore.getStatus();
      switch (ts) {
        case UNKNOWN:
          System.out.println("Invalid fate ID: " + fateId);
          break;

        case SUBMITTED:
        case IN_PROGRESS:
        case NEW:
          System.out.printf("Failing transaction: %s (%s)%n", fateId, ts);
          txStore.setStatus(TStatus.FAILED_IN_PROGRESS);
          state = true;
          break;

        case SUCCESSFUL:
          System.out.printf("Transaction already completed: %s (%s)%n", fateId, ts);
          break;

        case FAILED:
        case FAILED_IN_PROGRESS:
          System.out.printf("Transaction already failed: %s (%s)%n", fateId, ts);
          state = true;
          break;
      }
    } finally {
      txStore.unreserve(0, TimeUnit.MILLISECONDS);
    }

    return state;
  }

  public void deleteLocks(ZooReaderWriter zk, ServiceLock.ServiceLockPath path, String fateIdStr)
      throws KeeperException, InterruptedException {
    // delete any locks assoc w/ fate operation
    List<String> lockedIds = zk.getChildren(path.toString());

    for (String id : lockedIds) {
      List<String> lockNodes = zk.getChildren(path + "/" + id);
      for (String node : lockNodes) {
        String lockPath = path + "/" + id + "/" + node;
        byte[] data = zk.getData(path + "/" + id + "/" + node);
        // Example data: "READ:<FateId>". FateId contains ':' hence the limit of 2
        String[] lda = new String(data, UTF_8).split(":", 2);
        if (lda[1].equals(fateIdStr)) {
          zk.recursiveDelete(lockPath, NodeMissingPolicy.SKIP);
        }
      }
    }
  }

  @SuppressFBWarnings(value = "DM_EXIT",
      justification = "TODO - should probably avoid System.exit here; "
          + "this code is used by the fate admin shell command")
  public boolean checkGlobalLock(ZooReaderWriter zk, ServiceLockPath zLockManagerPath) {
    try {
      if (ServiceLock.getLockData(zk.getZooKeeper(), zLockManagerPath).isPresent()) {
        System.err.println("ERROR: Manager lock is held, not running");
        if (this.exitOnError) {
          System.exit(1);
        } else {
          return false;
        }
      }
    } catch (KeeperException e) {
      System.err.println("ERROR: Could not read manager lock, not running " + e.getMessage());
      if (this.exitOnError) {
        System.exit(1);
      } else {
        return false;
      }
    } catch (InterruptedException e) {
      System.err.println("ERROR: Could not read manager lock, not running" + e.getMessage());
      if (this.exitOnError) {
        System.exit(1);
      } else {
        return false;
      }
    }
    return true;
  }
}
