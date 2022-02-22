/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.shell.commands.fate;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.fate.FateTransactionStatus;
import org.apache.accumulo.fate.FateZooStore;
import org.apache.accumulo.fate.zookeeper.FateLock;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util for running FateCommand
 */
public class FateCommandHelper {
  private final static Logger log = LoggerFactory.getLogger(FateCommandHelper.class);
  private final boolean exitOnError;
  private final ZooReaderWriter zk;
  private ServiceLock.ServiceLockPath managerLockPath;
  private ServiceLock.ServiceLockPath tableLocksPath;
  private final FateZooStore zs;

  public FateCommandHelper(FateZooStore zs, ClientContext context, ZooReaderWriter zk,
      boolean exitOnError) {
    this.zs = zs;
    this.zk = zk;
    this.exitOnError = exitOnError;
    managerLockPath = ServiceLock.path(context.getZooKeeperRoot() + Constants.ZMANAGER_LOCK);
    tableLocksPath = ServiceLock.path(context.getZooKeeperRoot() + Constants.ZTABLE_LOCKS);
  }

  public void setManagerLockPath(ServiceLock.ServiceLockPath managerLockPath) {
    this.managerLockPath = managerLockPath;
  }

  public void setTableLocksPath(ServiceLock.ServiceLockPath tableLocksPath) {
    this.tableLocksPath = tableLocksPath;
  }

  protected boolean isManagerLockValid() throws AccumuloException {
    try {
      if (ServiceLock.getLockData(zk.getZooKeeper(), managerLockPath) != null) {
        System.err.println("ERROR: Manager lock is held, not running");
        if (this.exitOnError)
          throw new AccumuloException("ERROR: Manager lock is held");
        else
          return false;
      }
    } catch (KeeperException | InterruptedException e) {
      System.err.println("ERROR: Could not read manager lock" + e.getMessage());
      if (this.exitOnError)
        throw new AccumuloException("ERROR: Could not read manager lock.", e);
      else
        return false;
    }
    return true;
  }

  public boolean prepFail(String txidStr) throws AccumuloException {
    if (!isManagerLockValid()) {
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
    FateTransactionStatus ts = zs.getTStatus(txid);
    switch (ts) {
      case UNKNOWN:
        System.out.printf("Invalid transaction ID: %016x%n", txid);
        break;

      case SUBMITTED:
      case IN_PROGRESS:
      case NEW:
        System.out.printf("Failing transaction: %016x (%s)%n", txid, ts);
        zs.setStatus(txid, FateTransactionStatus.FAILED_IN_PROGRESS);
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

    zs.unreserve(txid);
    return state;
  }

  public boolean prepDelete(String txidStr) throws AccumuloException {
    if (!isManagerLockValid()) {
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
    FateTransactionStatus ts = zs.getTStatus(txid);
    switch (ts) {
      case UNKNOWN:
        System.out.printf("Invalid transaction ID: %016x%n", txid);
        break;

      case SUBMITTED:
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

    zs.unreserve(txid);
    return state;
  }

  public void deleteLocks(String txidStr) throws InterruptedException, KeeperException {
    // delete any locks assoc w/ fate operation
    List<String> lockedIds = zk.getChildren(tableLocksPath.toString());

    for (String id : lockedIds) {
      List<String> lockNodes = zk.getChildren(tableLocksPath + "/" + id);
      for (String node : lockNodes) {
        String lockPath = tableLocksPath + "/" + id + "/" + node;
        byte[] data = zk.getData(tableLocksPath + "/" + id + "/" + node);
        String[] lda = new String(data, UTF_8).split(":");
        if (lda[1].equals(txidStr))
          zk.recursiveDelete(lockPath, ZooUtil.NodeMissingPolicy.SKIP);
      }
    }
  }

  public void print(Formatter fmt, Set<Long> filterTxid,
      EnumSet<FateTransactionStatus> filterStatus) throws KeeperException, InterruptedException {

    FateStatus fateStatus = getStatus(zs, zk, tableLocksPath, filterTxid, filterStatus);

    for (var txStatus : fateStatus.getTransactions()) {
      fmt.format(
          "txid: %s  status: %-18s  op: %-15s  locked: %-15s locking: %-15s top: %-15s created: %s%n",
          txStatus.getTxid(), txStatus.getStatus(), txStatus.getDebug(), txStatus.getHeldLocks(),
          txStatus.getWaitingLocks(), txStatus.getTop(), txStatus.getTimeCreatedFormatted());
    }
    fmt.format(" %s transactions", fateStatus.getTransactions().size());

    if (!fateStatus.getDanglingHeldLocks().isEmpty()
        || !fateStatus.getDanglingWaitingLocks().isEmpty()) {
      fmt.format("%nThe following locks did not have an associated FATE operation%n");
      for (Map.Entry<String,List<String>> entry : fateStatus.getDanglingHeldLocks().entrySet())
        fmt.format("txid: %s  locked: %s%n", entry.getKey(), entry.getValue());

      for (Map.Entry<String,List<String>> entry : fateStatus.getDanglingWaitingLocks().entrySet())
        fmt.format("txid: %s  locking: %s%n", entry.getKey(), entry.getValue());
    }
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
  public FateStatus getStatus(FateZooStore zs, ZooReader zk, ServiceLock.ServiceLockPath lockPath,
      Set<Long> filterTxid, EnumSet<FateTransactionStatus> filterStatus)
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
  private void findLocks(ZooReader zk, final ServiceLock.ServiceLockPath lockPath,
      final Map<Long,List<String>> heldLocks, final Map<Long,List<String>> waitingLocks)
      throws KeeperException, InterruptedException {

    // stop with exception if lock ids cannot be retrieved from zookeeper
    List<String> lockedIds = zk.getChildren(lockPath.toString());

    for (String id : lockedIds) {

      try {

        FateLock.FateLockPath fLockPath = FateLock.path(lockPath + "/" + id);
        List<String> lockNodes =
            FateLock.validateAndSort(fLockPath, zk.getChildren(fLockPath.toString()));

        int pos = 0;
        boolean sawWriteLock = false;

        for (String node : lockNodes) {
          try {
            byte[] data = zk.getData(lockPath + "/" + id + "/" + node);
            String[] lda = new String(data, UTF_8).split(":");

            if (lda[0].charAt(0) == 'W')
              sawWriteLock = true;

            Map<Long,List<String>> locks;

            if (pos == 0) {
              locks = heldLocks;
            } else if (lda[0].charAt(0) == 'R' && !sawWriteLock) {
              locks = heldLocks;
            } else {
              locks = waitingLocks;
            }

            locks.computeIfAbsent(Long.parseLong(lda[1], 16), k -> new ArrayList<>())
                .add(lda[0].charAt(0) + ":" + id);

          } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
          }
          pos++;
        }

      } catch (KeeperException ex) {
        // Could be transient zk error. Log, but try to process the rest of list
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
  private FateStatus getTransactionStatus(FateZooStore zs, Set<Long> filterTxid,
      EnumSet<FateTransactionStatus> filterStatus, Map<Long,List<String>> heldLocks,
      Map<Long,List<String>> waitingLocks) {

    List<Long> transactions = zs.listTransactions();
    List<FateCommandStatus> statuses = new ArrayList<>(transactions.size());

    for (Long tid : transactions) {
      zs.reserve(tid);
      var debug = zs.getProperty(tid, "debug");
      List<String> hlocks = heldLocks.remove(tid);
      if (hlocks == null) {
        hlocks = Collections.emptyList();
      }

      List<String> wlocks = waitingLocks.remove(tid);

      if (wlocks == null) {
        wlocks = Collections.emptyList();
      }

      Object top = zs.top(tid);
      FateTransactionStatus status = zs.getTStatus(tid);
      long timeCreated = zs.timeCreated(tid);

      zs.unreserve(tid);

      if ((filterTxid != null && !filterTxid.contains(tid))
          || (filterStatus != null && !filterStatus.contains(status)))
        continue;

      statuses.add(new FateCommandStatus(tid, status, debug, hlocks, wlocks, top, timeCreated));
    }
    return new FateStatus(statuses, heldLocks, waitingLocks);
  }

  public static class FateStatus {

    private final List<FateCommandStatus> transactions;
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
      for (Map.Entry<Long,List<String>> entry : danglocks.entrySet()) {
        ret.put(String.format("%016x", entry.getKey()),
            Collections.unmodifiableList(entry.getValue()));
      }
      return Collections.unmodifiableMap(ret);
    }

    private FateStatus(List<FateCommandStatus> transactions,
        Map<Long,List<String>> danglingHeldLocks, Map<Long,List<String>> danglingWaitingLocks) {
      this.transactions = Collections.unmodifiableList(transactions);
      this.danglingHeldLocks = convert(danglingHeldLocks);
      this.danglingWaitingLocks = convert(danglingWaitingLocks);
    }

    public List<FateCommandStatus> getTransactions() {
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
}
