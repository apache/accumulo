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

import java.time.Duration;
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
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.FateIdStatus;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.ReadOnlyFateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.fate.zookeeper.FateLock;
import org.apache.accumulo.core.fate.zookeeper.FateLock.FateLockPath;
import org.apache.accumulo.core.fate.zookeeper.LockRange;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * A utility to administer FATE operations
 */
public class AdminUtil<T> {
  private static final Logger log = LoggerFactory.getLogger(AdminUtil.class);

  /**
   * FATE transaction status, including lock information.
   */
  public static class TransactionStatus {

    private final FateId fateId;
    private final FateInstanceType instanceType;
    private final TStatus status;
    private final Fate.FateOperation fateOp;
    private final List<String> hlocks;
    private final List<String> wlocks;
    private final String top;
    private final long timeCreated;
    private final LockRange lockRange;

    private TransactionStatus(FateId fateId, FateInstanceType instanceType, TStatus status,
        Fate.FateOperation fateOp, List<String> hlocks, List<String> wlocks, String top,
        Long timeCreated, LockRange lockRange) {

      this.fateId = fateId;
      this.instanceType = instanceType;
      this.status = status;
      this.fateOp = fateOp;
      this.hlocks = Collections.unmodifiableList(hlocks);
      this.wlocks = Collections.unmodifiableList(wlocks);
      this.top = top;
      this.timeCreated = timeCreated;
      this.lockRange = lockRange;

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
    public Fate.FateOperation getFateOp() {
      return fateOp;
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

    public LockRange getLockRange() {
      return lockRange;
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
   * use {@link #getStatus(ReadOnlyFateStore, ZooSession, ServiceLockPath, Set, EnumSet, EnumSet)}
   *
   * @param readOnlyFateStores read-only fate stores
   * @param fateIdFilter filter results to include only provided fate transaction ids
   * @param statusFilter filter results to include only provided status types
   * @param typesFilter filter results to include only provided fate instance types
   * @return list of FATE transactions that match filter criteria
   */
  public List<TransactionStatus> getTransactionStatus(
      Map<FateInstanceType,ReadOnlyFateStore<T>> readOnlyFateStores, Set<FateId> fateIdFilter,
      EnumSet<TStatus> statusFilter, EnumSet<FateInstanceType> typesFilter) {

    FateStatus status = getTransactionStatus(readOnlyFateStores, fateIdFilter, statusFilter,
        typesFilter, Collections.<FateId,List<String>>emptyMap(),
        Collections.<FateId,List<String>>emptyMap(), Map.of());

    return status.getTransactions();
  }

  /**
   * Get the FATE transaction status and lock information stored in zookeeper, optionally filtered
   * by fate id, status, and fate instance type
   *
   * @param readOnlyMFS read-only MetaFateStore
   * @param zk zookeeper reader.
   * @param lockPath the zookeeper path for locks
   * @param fateIdFilter filter results to include only provided fate transaction ids
   * @param statusFilter filter results to include only provided status types
   * @param typesFilter filter results to include only provided fate instance types
   * @return a summary container of the fate transactions.
   * @throws KeeperException if zookeeper exception occurs
   * @throws InterruptedException if process is interrupted.
   */
  public FateStatus getStatus(ReadOnlyFateStore<T> readOnlyMFS, ZooSession zk,
      ServiceLockPath lockPath, Set<FateId> fateIdFilter, EnumSet<TStatus> statusFilter,
      EnumSet<FateInstanceType> typesFilter) throws KeeperException, InterruptedException {
    Map<FateId,List<String>> heldLocks = new HashMap<>();
    Map<FateId,List<String>> waitingLocks = new HashMap<>();
    Map<FateId,LockRange> lockRanges = new HashMap<>();

    findLocks(zk, lockPath, heldLocks, waitingLocks, lockRanges);

    return getTransactionStatus(Map.of(FateInstanceType.META, readOnlyMFS), fateIdFilter,
        statusFilter, typesFilter, heldLocks, waitingLocks, lockRanges);
  }

  public FateStatus getStatus(ReadOnlyFateStore<T> readOnlyUFS, Set<FateId> fateIdFilter,
      EnumSet<TStatus> statusFilter, EnumSet<FateInstanceType> typesFilter)
      throws KeeperException, InterruptedException {

    return getTransactionStatus(Map.of(FateInstanceType.USER, readOnlyUFS), fateIdFilter,
        statusFilter, typesFilter, new HashMap<>(), new HashMap<>(), Map.of());
  }

  public FateStatus getStatus(Map<FateInstanceType,ReadOnlyFateStore<T>> readOnlyFateStores,
      ZooSession zk, ServiceLockPath lockPath, Set<FateId> fateIdFilter,
      EnumSet<TStatus> statusFilter, EnumSet<FateInstanceType> typesFilter)
      throws KeeperException, InterruptedException {
    Map<FateId,List<String>> heldLocks = new HashMap<>();
    Map<FateId,List<String>> waitingLocks = new HashMap<>();
    Map<FateId,LockRange> lockRanges = new HashMap<>();

    findLocks(zk, lockPath, heldLocks, waitingLocks, lockRanges);

    return getTransactionStatus(readOnlyFateStores, fateIdFilter, statusFilter, typesFilter,
        heldLocks, waitingLocks, lockRanges);
  }

  /**
   * Walk through the lock nodes in zookeeper to find and populate held locks and waiting locks.
   *
   * @param zk zookeeper client
   * @param lockPath the zookeeper path for locks
   * @param heldLocks map for returning transactions with held locks
   * @param waitingLocks map for returning transactions with waiting locks
   * @throws KeeperException if initial lock list cannot be read.
   * @throws InterruptedException if thread interrupt detected while processing.
   */
  public static void findLocks(ZooSession zk, final ServiceLockPath lockPath,
      final Map<FateId,List<String>> heldLocks, final Map<FateId,List<String>> waitingLocks,
      final Map<FateId,LockRange> lockRanges) throws KeeperException, InterruptedException {

    var zr = zk.asReader();

    // stop with exception if lock ids cannot be retrieved from zookeeper
    List<String> lockedIds = zr.getChildren(lockPath.toString());

    for (String id : lockedIds) {
      try {
        FateLockPath fLockPath = FateLock.path(lockPath + "/" + id);
        SortedSet<FateLock.NodeName> lockNodes =
            FateLock.validateAndWarn(fLockPath, zr.getChildren(fLockPath.toString()));

        ArrayList<FateLock.FateLockEntry> previous = new ArrayList<>(lockNodes.size());

        for (FateLock.NodeName node : lockNodes) {
          try {
            FateLock.FateLockEntry fateLockEntry = node.fateLockEntry.get();
            var fateId = fateLockEntry.getFateId();
            var lockType = fateLockEntry.getLockType();

            lockRanges.put(fateId, fateLockEntry.getRange());

            Map<FateId,List<String>> locks = heldLocks;

            if (lockType == LockType.WRITE) {
              for (var prev : Lists.reverse(previous)) {
                if (prev.getRange().overlaps(fateLockEntry.getRange())) {
                  locks = waitingLocks;
                  break;
                }
              }
            } else {
              for (var prev : Lists.reverse(previous)) {
                if (prev.getLockType() == LockType.WRITE
                    && prev.getRange().overlaps(fateLockEntry.getRange())) {
                  locks = waitingLocks;
                  break;
                }
              }
            }

            locks.computeIfAbsent(fateId, k -> new ArrayList<>())
                .add(lockType.name().charAt(0) + ":" + id);

            previous.add(fateLockEntry);
          } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
          }
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
   * @param readOnlyFateStores read-only access to populated transaction stores.
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
  public static <T> FateStatus getTransactionStatus(
      Map<FateInstanceType,ReadOnlyFateStore<T>> readOnlyFateStores, Set<FateId> fateIdFilter,
      EnumSet<TStatus> statusFilter, EnumSet<FateInstanceType> typesFilter,
      Map<FateId,List<String>> heldLocks, Map<FateId,List<String>> waitingLocks,
      Map<FateId,LockRange> lockRanges) {
    final List<TransactionStatus> statuses = new ArrayList<>();

    readOnlyFateStores.forEach((type, store) -> {
      try (Stream<FateId> fateIds = store.list().map(FateIdStatus::getFateId)) {
        fateIds.forEach(fateId -> {

          ReadOnlyFateTxStore<T> txStore = store.read(fateId);
          // fate op will not be set if the tx is not seeded with work (it is NEW)
          Fate.FateOperation fateOp = txStore.getTransactionInfo(Fate.TxInfo.FATE_OP) == null ? null
              : ((Fate.FateOperation) txStore.getTransactionInfo(Fate.TxInfo.FATE_OP));

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
            statuses.add(new TransactionStatus(fateId, type, status, fateOp, hlocks, wlocks, top,
                timeCreated, lockRanges.getOrDefault(fateId, LockRange.infinite())));
          }
        });
      }
    });
    return new FateStatus(statuses, heldLocks, waitingLocks);
  }

  private static boolean includeByStatus(TStatus status, EnumSet<TStatus> statusFilter) {
    return statusFilter == null || statusFilter.isEmpty() || statusFilter.contains(status);
  }

  private static boolean includeByFateId(FateId fateId, Set<FateId> fateIdFilter) {
    return fateIdFilter == null || fateIdFilter.isEmpty() || fateIdFilter.contains(fateId);
  }

  private static boolean includeByInstanceType(FateInstanceType type,
      EnumSet<FateInstanceType> typesFilter) {
    return typesFilter == null || typesFilter.isEmpty() || typesFilter.contains(type);
  }

  public void print(Map<FateInstanceType,ReadOnlyFateStore<T>> readOnlyFateStores, ZooSession zk,
      ServiceLockPath tableLocksPath, Formatter fmt, Set<FateId> fateIdFilter,
      EnumSet<TStatus> statusFilter, EnumSet<FateInstanceType> typesFilter)
      throws KeeperException, InterruptedException {
    FateStatus fateStatus =
        getStatus(readOnlyFateStores, zk, tableLocksPath, fateIdFilter, statusFilter, typesFilter);

    for (TransactionStatus txStatus : fateStatus.getTransactions()) {
      fmt.format(
          "%-15s fateId: %s  status: %-18s locked: %-15s locking: %-15s op: %-15s created: %s lock range: (%s,%s]%n",
          txStatus.getFateOp(), txStatus.getFateId(), txStatus.getStatus(), txStatus.getHeldLocks(),
          txStatus.getWaitingLocks(), txStatus.getTop(), txStatus.getTimeCreatedFormatted(),
          txStatus.getLockRange().getStartRow(), txStatus.getLockRange().getEndRow());
    }
    fmt.format(" %s transactions", fateStatus.getTransactions().size());
  }

  public boolean prepDelete(Map<FateInstanceType,FateStore<T>> stores, String fateIdStr) {

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

    Optional<FateTxStore<T>> opTxStore = tryReserve(store, fateId, "delete");
    if (opTxStore.isPresent()) {
      var txStore = opTxStore.orElseThrow();

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
            txStore.forceDelete();
            state = true;
            break;
        }
      } finally {
        txStore.unreserve(Duration.ZERO);
      }
    }

    return state;
  }

  public boolean prepFail(Map<FateInstanceType,FateStore<T>> stores, String fateIdStr) {

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

    Optional<FateTxStore<T>> opTxStore = tryReserve(store, fateId, "fail");
    if (opTxStore.isPresent()) {
      var txStore = opTxStore.orElseThrow();

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
        txStore.unreserve(Duration.ZERO);
      }
    }

    return state;
  }

  /**
   * Try to reserve the transaction for a minute. If it could not be reserved, return an empty
   * optional
   */
  private Optional<FateTxStore<T>> tryReserve(FateStore<T> store, FateId fateId, String op) {
    var retry = Retry.builder().maxRetriesWithinDuration(Duration.ofMinutes(1))
        .retryAfter(Duration.ofMillis(25)).incrementBy(Duration.ofMillis(25))
        .maxWait(Duration.ofSeconds(15)).backOffFactor(1.5).logInterval(Duration.ofSeconds(15))
        .createRetry();

    Optional<FateTxStore<T>> reserveAttempt = store.tryReserve(fateId);
    while (reserveAttempt.isEmpty() && retry.canRetry()) {
      retry.useRetry();
      try {
        retry.waitForNextAttempt(log, "Attempting to reserve " + fateId);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalArgumentException(e);
      }
      reserveAttempt = store.tryReserve(fateId);
    }
    if (reserveAttempt.isPresent()) {
      retry.logCompletion(log, "Attempting to reserve " + fateId);
    } else {
      log.error("Could not {} {} in a reasonable time. This indicates the Manager is currently "
          + "working on it. The Manager may need to be stopped and the command rerun to complete "
          + "this.", op, fateId);
    }

    return reserveAttempt;
  }

  public void deleteLocks(ZooSession zk, ServiceLockPath path, String fateIdStr)
      throws KeeperException, InterruptedException {
    var zrw = zk.asReaderWriter();

    // delete any locks assoc w/ fate operation
    List<String> lockedIds = zrw.getChildren(path.toString());

    for (String id : lockedIds) {
      List<String> lockNodes = zrw.getChildren(path + "/" + id);
      for (String node : lockNodes) {
        String lockPath = path + "/" + id + "/" + node;
        byte[] data = zrw.getData(path + "/" + id + "/" + node);
        // Example data: "READ:<FateId>". FateId contains ':' hence the limit of 2
        String[] lda = new String(data, UTF_8).split(":", 2);
        if (lda[1].equals(fateIdStr)) {
          zrw.recursiveDelete(lockPath, NodeMissingPolicy.SKIP);
        }
      }
    }
  }
}
