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
package org.apache.accumulo.fate;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.fate.zookeeper.IZooReader;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
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
   * Default constructor
   */
  public AdminUtil() {
    this(true);
  }

  /**
   * Constructor
   *
   * @param exitOnError
   *          <code>System.exit(1)</code> on error if true
   */
  public AdminUtil(boolean exitOnError) {
    super();
    this.exitOnError = exitOnError;
  }

  /**
   * FATE transaction status, including lock information.
   */
  public static class TransactionStatus {

    private final long txid;
    private final TStatus status;
    private final String debug;
    private final List<String> hlocks;
    private final List<String> wlocks;
    private final String top;

    private TransactionStatus(Long tid, TStatus status, String debug, List<String> hlocks,
        List<String> wlocks, String top) {

      this.txid = tid;
      this.status = status;
      this.debug = debug;
      this.hlocks = Collections.unmodifiableList(hlocks);
      this.wlocks = Collections.unmodifiableList(wlocks);
      this.top = top;

    }

    /**
     * @return This fate operations transaction id, formatted in the same way as FATE transactions
     *         are in the Accumulo logs.
     */
    public String getTxid() {
      return String.format("%016x", txid);
    }

    public TStatus getStatus() {
      return status;
    }

    /**
     * @return The debug info for the operation on the top of the stack for this Fate operation.
     */
    public String getDebug() {
      return debug;
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
  }

  public static class FateStatus {

    private final List<TransactionStatus> transactions;
    private final Map<String,List<String>> danglingHeldLocks;
    private final Map<String,List<String>> danglingWaitingLocks;

    /**
     * Convert FATE transactions IDs in keys of map to format that used in printing and logging FATE
     * transactions ids. This is done so that if the map is printed, the output can be used to
     * search Accumulo's logs.
     */
    private static Map<String,List<String>> convert(Map<Long,List<String>> danglocks) {
      if (danglocks.isEmpty()) {
        return Collections.emptyMap();
      }

      Map<String,List<String>> ret = new HashMap<>();
      for (Entry<Long,List<String>> entry : danglocks.entrySet()) {
        ret.put(String.format("%016x", entry.getKey()),
            Collections.unmodifiableList(entry.getValue()));
      }
      return Collections.unmodifiableMap(ret);
    }

    private FateStatus(List<TransactionStatus> transactions,
        Map<Long,List<String>> danglingHeldLocks, Map<Long,List<String>> danglingWaitingLocks) {
      this.transactions = Collections.unmodifiableList(transactions);
      this.danglingHeldLocks = convert(danglingHeldLocks);
      this.danglingWaitingLocks = convert(danglingWaitingLocks);
    }

    public List<TransactionStatus> getTransactions() {
      return transactions;
    }

    /**
     * Get locks that are held by non existent FATE transactions. These are table or namespace
     * locks.
     *
     * @return map where keys are transaction ids and values are a list of table IDs and/or
     *         namespace IDs. The transaction IDs are in the same format as transaction IDs in the
     *         Accumulo logs.
     */
    public Map<String,List<String>> getDanglingHeldLocks() {
      return danglingHeldLocks;
    }

    /**
     * Get locks that are waiting to be acquired by non existent FATE transactions. These are table
     * or namespace locks.
     *
     * @return map where keys are transaction ids and values are a list of table IDs and/or
     *         namespace IDs. The transaction IDs are in the same format as transaction IDs in the
     *         Accumulo logs.
     */
    public Map<String,List<String>> getDanglingWaitingLocks() {
      return danglingWaitingLocks;
    }
  }

  /**
   * Returns a list of the FATE transactions, optionally filtered by transaction id and status. This
   * method does not process lock information, if lock information is desired, use
   * {@link #getStatus(ReadOnlyTStore, IZooReader, String, Set, EnumSet)}
   *
   * @param zs
   *          read-only zoostore
   * @param filterTxid
   *          filter results to include for provided transaction ids.
   * @param filterStatus
   *          filter results to include only provided status types
   * @return list of FATE transactions that match filter criteria
   */
  public List<TransactionStatus> getTransactionStatus(ReadOnlyTStore<T> zs, Set<Long> filterTxid,
      EnumSet<TStatus> filterStatus) {

    FateStatus status = getTransactionStatus(zs, filterTxid, filterStatus,
        Collections.<Long,List<String>> emptyMap(), Collections.<Long,List<String>> emptyMap());

    return status.getTransactions();
  }

  /**
   * Get the FATE transaction status and lock information stored in zookeeper, optionally filtered
   * by transaction id and filter status.
   *
   * @param zs
   *          read-only zoostore
   * @param zk
   *          zookeeper reader.
   * @param lockPath
   *          the zookeeper path for locks
   * @param filterTxid
   *          filter results to include for provided transaction ids.
   * @param filterStatus
   *          filter results to include only provided status types
   * @return a summary container of the fate transactions.
   * @throws KeeperException
   *           if zookeeper exception occurs
   * @throws InterruptedException
   *           if process is interrupted.
   */
  public FateStatus getStatus(ReadOnlyTStore<T> zs, IZooReader zk, String lockPath,
      Set<Long> filterTxid, EnumSet<TStatus> filterStatus)
      throws KeeperException, InterruptedException {

    Map<Long,List<String>> heldLocks = new HashMap<>();
    Map<Long,List<String>> waitingLocks = new HashMap<>();

    findLocks(zk, lockPath, heldLocks, waitingLocks);

    return getTransactionStatus(zs, filterTxid, filterStatus, heldLocks, waitingLocks);

  }

  /**
   * Walk through the lock nodes in zookeeper to find and populate held locks and waiting locks.
   *
   * @param zk
   *          zookeeper reader
   * @param lockPath
   *          the zookeeper path for locks
   * @param heldLocks
   *          map for returning transactions with held locks
   * @param waitingLocks
   *          map for returning transactions with waiting locks
   * @throws KeeperException
   *           if initial lock list cannot be read.
   * @throws InterruptedException
   *           if thread interrupt detected while processing.
   */
  private void findLocks(IZooReader zk, final String lockPath,
      final Map<Long,List<String>> heldLocks, final Map<Long,List<String>> waitingLocks)
      throws KeeperException, InterruptedException {

    // stop with exception if lock ids cannot be retrieved from zookeeper
    List<String> lockedIds = zk.getChildren(lockPath);

    for (String id : lockedIds) {

      try {

        List<String> lockNodes = zk.getChildren(lockPath + "/" + id);
        lockNodes = new ArrayList<>(lockNodes);
        Collections.sort(lockNodes);

        int pos = 0;
        boolean sawWriteLock = false;

        for (String node : lockNodes) {
          try {
            byte[] data = zk.getData(lockPath + "/" + id + "/" + node, null);
            String[] lda = new String(data, UTF_8).split(":");

            if (lda[0].charAt(0) == 'W')
              sawWriteLock = true;

            Map<Long,List<String>> locks;

            if (pos == 0) {
              locks = heldLocks;
            } else {
              if (lda[0].charAt(0) == 'R' && !sawWriteLock) {
                locks = heldLocks;
              } else {
                locks = waitingLocks;
              }
            }

            List<String> tables = locks.get(Long.parseLong(lda[1], 16));
            if (tables == null) {
              tables = new ArrayList<>();
              locks.put(Long.parseLong(lda[1], 16), tables);
            }

            tables.add(lda[0].charAt(0) + ":" + id);

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
   * @param zs
   *          read-only access to a populated transaction store.
   * @param filterTxid
   *          Optional. List of transactions to filter results - if null, all transactions are
   *          returned
   * @param filterStatus
   *          Optional. List of status types to filter results - if null, all transactions are
   *          returned.
   * @param heldLocks
   *          populated list of locks held by transaction - or an empty map if none.
   * @param waitingLocks
   *          populated list of locks held by transaction - or an empty map if none.
   * @return current fate and lock status
   */
  private FateStatus getTransactionStatus(ReadOnlyTStore<T> zs, Set<Long> filterTxid,
      EnumSet<TStatus> filterStatus, Map<Long,List<String>> heldLocks,
      Map<Long,List<String>> waitingLocks) {

    List<Long> transactions = zs.list();
    List<TransactionStatus> statuses = new ArrayList<>(transactions.size());

    for (Long tid : transactions) {

      zs.reserve(tid);

      String debug = (String) zs.getProperty(tid, "debug");

      List<String> hlocks = heldLocks.remove(tid);

      if (hlocks == null) {
        hlocks = Collections.emptyList();
      }

      List<String> wlocks = waitingLocks.remove(tid);

      if (wlocks == null) {
        wlocks = Collections.emptyList();
      }

      String top = null;
      ReadOnlyRepo<T> repo = zs.top(tid);
      if (repo != null)
        top = repo.getDescription();

      TStatus status = zs.getStatus(tid);

      zs.unreserve(tid, 0);

      if ((filterTxid != null && !filterTxid.contains(tid))
          || (filterStatus != null && !filterStatus.contains(status)))
        continue;

      statuses.add(new TransactionStatus(tid, status, debug, hlocks, wlocks, top));
    }

    return new FateStatus(statuses, heldLocks, waitingLocks);

  }

  public void print(ReadOnlyTStore<T> zs, IZooReader zk, String lockPath)
      throws KeeperException, InterruptedException {
    print(zs, zk, lockPath, new Formatter(System.out), null, null);
  }

  public void print(ReadOnlyTStore<T> zs, IZooReader zk, String lockPath, Formatter fmt,
      Set<Long> filterTxid, EnumSet<TStatus> filterStatus)
      throws KeeperException, InterruptedException {

    FateStatus fateStatus = getStatus(zs, zk, lockPath, filterTxid, filterStatus);

    for (TransactionStatus txStatus : fateStatus.getTransactions()) {
      fmt.format("txid: %s  status: %-18s  op: %-15s  locked: %-15s locking: %-15s top: %s%n",
          txStatus.getTxid(), txStatus.getStatus(), txStatus.getDebug(), txStatus.getHeldLocks(),
          txStatus.getWaitingLocks(), txStatus.getTop());
    }
    fmt.format(" %s transactions", fateStatus.getTransactions().size());

    if (fateStatus.getDanglingHeldLocks().size() != 0
        || fateStatus.getDanglingWaitingLocks().size() != 0) {
      fmt.format("%nThe following locks did not have an associated FATE operation%n");
      for (Entry<String,List<String>> entry : fateStatus.getDanglingHeldLocks().entrySet())
        fmt.format("txid: %s  locked: %s%n", entry.getKey(), entry.getValue());

      for (Entry<String,List<String>> entry : fateStatus.getDanglingWaitingLocks().entrySet())
        fmt.format("txid: %s  locking: %s%n", entry.getKey(), entry.getValue());
    }
  }

  public boolean prepDelete(TStore<T> zs, IZooReaderWriter zk, String path, String txidStr) {
    if (!checkGlobalLock(zk, path)) {
      return false;
    }

    long txid;
    try {
      txid = Long.parseLong(txidStr, 16);
    } catch (NumberFormatException nfe) {
      System.out.printf("Invalid transaction ID format: %s%n", txidStr);
      return false;
    }
    boolean state = false;
    zs.reserve(txid);
    TStatus ts = zs.getStatus(txid);
    switch (ts) {
      case UNKNOWN:
        System.out.printf("Invalid transaction ID: %016x%n", txid);
        break;

      case IN_PROGRESS:
      case NEW:
      case FAILED:
      case FAILED_IN_PROGRESS:
      case SUCCESSFUL:
        System.out.printf("Deleting transaction: %016x (%s)%n", txid, ts);
        zs.delete(txid);
        state = true;
        break;
    }

    zs.unreserve(txid, 0);
    return state;
  }

  public boolean prepFail(TStore<T> zs, IZooReaderWriter zk, String path, String txidStr) {
    if (!checkGlobalLock(zk, path)) {
      return false;
    }

    long txid;
    try {
      txid = Long.parseLong(txidStr, 16);
    } catch (NumberFormatException nfe) {
      System.out.printf("Invalid transaction ID format: %s%n", txidStr);
      return false;
    }
    boolean state = false;
    zs.reserve(txid);
    TStatus ts = zs.getStatus(txid);
    switch (ts) {
      case UNKNOWN:
        System.out.printf("Invalid transaction ID: %016x%n", txid);
        break;

      case IN_PROGRESS:
      case NEW:
        System.out.printf("Failing transaction: %016x (%s)%n", txid, ts);
        zs.setStatus(txid, TStatus.FAILED_IN_PROGRESS);
        state = true;
        break;

      case SUCCESSFUL:
        System.out.printf("Transaction already completed: %016x (%s)%n", txid, ts);
        break;

      case FAILED:
      case FAILED_IN_PROGRESS:
        System.out.printf("Transaction already failed: %016x (%s)%n", txid, ts);
        state = true;
        break;
    }

    zs.unreserve(txid, 0);
    return state;
  }

  public void deleteLocks(IZooReaderWriter zk, String path, String txidStr)
      throws KeeperException, InterruptedException {
    // delete any locks assoc w/ fate operation
    List<String> lockedIds = zk.getChildren(path);

    for (String id : lockedIds) {
      List<String> lockNodes = zk.getChildren(path + "/" + id);
      for (String node : lockNodes) {
        String lockPath = path + "/" + id + "/" + node;
        byte[] data = zk.getData(path + "/" + id + "/" + node, null);
        String[] lda = new String(data, UTF_8).split(":");
        if (lda[1].equals(txidStr))
          zk.recursiveDelete(lockPath, NodeMissingPolicy.SKIP);
      }
    }
  }

  @SuppressFBWarnings(value = "DM_EXIT",
      justification = "TODO - should probably avoid System.exit here; "
          + "this code is used by the fate admin shell command")
  public boolean checkGlobalLock(IZooReaderWriter zk, String path) {
    try {
      if (ZooLock.getLockData(zk.getZooKeeper(), path) != null) {
        System.err.println("ERROR: Master lock is held, not running");
        if (this.exitOnError)
          System.exit(1);
        else
          return false;
      }
    } catch (KeeperException e) {
      System.err.println("ERROR: Could not read master lock, not running " + e.getMessage());
      if (this.exitOnError)
        System.exit(1);
      else
        return false;
    } catch (InterruptedException e) {
      System.err.println("ERROR: Could not read master lock, not running" + e.getMessage());
      if (this.exitOnError)
        System.exit(1);
      else
        return false;
    }
    return true;
  }
}
