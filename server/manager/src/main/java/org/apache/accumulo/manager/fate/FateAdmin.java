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
package org.apache.accumulo.manager.fate;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.fate.FateStatus;
import org.apache.accumulo.fate.FateTransactionStatus;
import org.apache.accumulo.fate.TransactionStatus;
import org.apache.accumulo.fate.zookeeper.FateLock;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A utility to administer FATE operations
 */
@SuppressFBWarnings(value = "DM_EXIT", justification = "System.exit okay for CLI tool")
public class FateAdmin {
  private static final Logger log = LoggerFactory.getLogger(FateAdmin.class);

  private final boolean exitOnError;

  /**
   * Constructor
   *
   * @param exitOnError
   *          <code>System.exit(1)</code> on error if true
   */
  public FateAdmin(boolean exitOnError) {
    this.exitOnError = exitOnError;
  }

  static class TxOpts {
    @Parameter(description = "<txid>...", required = true)
    List<String> txids = new ArrayList<>();
  }

  @Parameters(commandDescription = "Stop an existing FATE by transaction id")
  static class FailOpts extends TxOpts {}

  @Parameters(commandDescription = "Delete an existing FATE by transaction id")
  static class DeleteOpts extends TxOpts {}

  @Parameters(commandDescription = "List the existing FATE transactions")
  static class PrintOpts {}

  public static void main(String[] args) throws Exception {
    Help opts = new Help();
    JCommander jc = new JCommander(opts);
    jc.setProgramName(FateAdmin.class.getName());
    LinkedHashMap<String,TxOpts> txOpts = new LinkedHashMap<>(2);
    txOpts.put("fail", new FailOpts());
    txOpts.put("delete", new DeleteOpts());
    for (Entry<String,TxOpts> entry : txOpts.entrySet()) {
      jc.addCommand(entry.getKey(), entry.getValue());
    }
    jc.addCommand("print", new PrintOpts());
    jc.parse(args);
    if (opts.help || jc.getParsedCommand() == null) {
      jc.usage();
      System.exit(1);
    }

    System.err.printf("This tool has been deprecated%nFATE administration now"
        + " available within 'accumulo shell'%n$ fate fail <txid>... | delete"
        + " <txid>... | print [<txid>...]%n%n");

    FateAdmin admin = new FateAdmin(true);

    try (var context = new ServerContext(SiteConfiguration.auto())) {
      final String zkRoot = context.getZooKeeperRoot();
      String path = zkRoot + Constants.ZFATE;
      var zLockManagerPath = ServiceLock.path(zkRoot + Constants.ZMANAGER_LOCK);
      var zTableLocksPath = ServiceLock.path(zkRoot + Constants.ZTABLE_LOCKS);
      ZooReaderWriter zk = context.getZooReaderWriter();
      ZooStore zs = new ZooStore(path, zk);

      if (jc.getParsedCommand().equals("fail")) {
        for (String txid : txOpts.get(jc.getParsedCommand()).txids) {
          if (!admin.prepFail(zs, zk, zLockManagerPath, txid)) {
            System.exit(1);
          }
        }
      } else if (jc.getParsedCommand().equals("delete")) {
        for (String txid : txOpts.get(jc.getParsedCommand()).txids) {
          if (!admin.prepDelete(zs, zk, zLockManagerPath, txid)) {
            System.exit(1);
          }
          admin.deleteLocks(zk, zTableLocksPath, txid);
        }
      } else if (jc.getParsedCommand().equals("print")) {
        admin.print(new ReadOnlyStore(zs), zk, zTableLocksPath);
      }
    }
  }

  /**
   * Returns a list of the FATE transactions, optionally filtered by transaction id and status. This
   * method does not process lock information, if lock information is desired, use
   * {@link #getStatus(ReadOnlyTStore, ZooReader, ServiceLock.ServiceLockPath, Set, EnumSet)}
   *
   * @param zs
   *          read-only zoostore
   * @param filterTxid
   *          filter results to include for provided transaction ids.
   * @param filterStatus
   *          filter results to include only provided status types
   * @return list of FATE transactions that match filter criteria
   */
  public List<TransactionStatus> getTransactionStatus(ReadOnlyTStore zs, Set<Long> filterTxid,
      EnumSet<FateTransactionStatus> filterStatus) {

    FateStatus status = getTransactionStatus(zs, filterTxid, filterStatus, Collections.emptyMap(),
        Collections.emptyMap());

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
  public FateStatus getStatus(ReadOnlyTStore zs, ZooReader zk, ServiceLock.ServiceLockPath lockPath,
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
  private FateStatus getTransactionStatus(ReadOnlyTStore zs, Set<Long> filterTxid,
      EnumSet<FateTransactionStatus> filterStatus, Map<Long,List<String>> heldLocks,
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
      ReadOnlyRepo repo = zs.top(tid);
      if (repo != null)
        top = repo.getDescription();

      FateTransactionStatus status = zs.getStatus(tid);

      long timeCreated = zs.timeCreated(tid);

      zs.unreserve(tid, 0);

      if ((filterTxid != null && !filterTxid.contains(tid))
          || (filterStatus != null && !filterStatus.contains(status)))
        continue;

      statuses.add(new TransactionStatus(tid, status, debug, hlocks, wlocks, top, timeCreated));
    }

    return new FateStatus(statuses, heldLocks, waitingLocks);

  }

  public void print(ReadOnlyTStore zs, ZooReader zk, ServiceLock.ServiceLockPath lockPath)
      throws KeeperException, InterruptedException {
    print(zs, zk, lockPath, new Formatter(System.out), null, null);
  }

  public void print(ReadOnlyTStore zs, ZooReader zk, ServiceLock.ServiceLockPath lockPath,
      Formatter fmt, Set<Long> filterTxid, EnumSet<FateTransactionStatus> filterStatus)
      throws KeeperException, InterruptedException {

    FateStatus fateStatus = getStatus(zs, zk, lockPath, filterTxid, filterStatus);

    for (TransactionStatus txStatus : fateStatus.getTransactions()) {
      fmt.format(
          "txid: %s  status: %-18s  op: %-15s  locked: %-15s locking: %-15s top: %-15s created: %s%n",
          txStatus.getTxid(), txStatus.getStatus(), txStatus.getDebug(), txStatus.getHeldLocks(),
          txStatus.getWaitingLocks(), txStatus.getTop(), txStatus.getTimeCreatedFormatted());
    }
    fmt.format(" %s transactions", fateStatus.getTransactions().size());

    if (!fateStatus.getDanglingHeldLocks().isEmpty()
        || !fateStatus.getDanglingWaitingLocks().isEmpty()) {
      fmt.format("%nThe following locks did not have an associated FATE operation%n");
      for (Entry<String,List<String>> entry : fateStatus.getDanglingHeldLocks().entrySet())
        fmt.format("txid: %s  locked: %s%n", entry.getKey(), entry.getValue());

      for (Entry<String,List<String>> entry : fateStatus.getDanglingWaitingLocks().entrySet())
        fmt.format("txid: %s  locking: %s%n", entry.getKey(), entry.getValue());
    }
  }

  public boolean prepDelete(TStore zs, ZooReaderWriter zk, ServiceLock.ServiceLockPath path,
      String txidStr) throws AccumuloException {
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
    FateTransactionStatus ts = zs.getStatus(txid);
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

    zs.unreserve(txid, 0);
    return state;
  }

  public boolean prepFail(TStore zs, ZooReaderWriter zk,
      ServiceLock.ServiceLockPath zLockManagerPath, String txidStr) throws AccumuloException {
    if (!checkGlobalLock(zk, zLockManagerPath)) {
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
    FateTransactionStatus ts = zs.getStatus(txid);
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

    zs.unreserve(txid, 0);
    return state;
  }

  public void deleteLocks(ZooReaderWriter zk, ServiceLock.ServiceLockPath path, String txidStr)
      throws KeeperException, InterruptedException {
    // delete any locks assoc w/ fate operation
    List<String> lockedIds = zk.getChildren(path.toString());

    for (String id : lockedIds) {
      List<String> lockNodes = zk.getChildren(path + "/" + id);
      for (String node : lockNodes) {
        String lockPath = path + "/" + id + "/" + node;
        byte[] data = zk.getData(path + "/" + id + "/" + node);
        String[] lda = new String(data, UTF_8).split(":");
        if (lda[1].equals(txidStr))
          zk.recursiveDelete(lockPath, ZooUtil.NodeMissingPolicy.SKIP);
      }
    }
  }

  @SuppressFBWarnings(value = "DM_EXIT",
      justification = "TODO - should probably avoid System.exit here; "
          + "this code is used by the fate admin shell command")
  public boolean checkGlobalLock(ZooReaderWriter zk, ServiceLock.ServiceLockPath zLockManagerPath)
      throws AccumuloException {
    try {
      if (ServiceLock.getLockData(zk.getZooKeeper(), zLockManagerPath) != null) {
        System.err.println("ERROR: Manager lock is held, not running");
        if (this.exitOnError)
          throw new AccumuloException("ERROR: Manager lock is held, not running");
        else
          return false;
      }
    } catch (KeeperException e) {
      System.err.println("ERROR: Could not read manager lock, not running " + e.getMessage());
      if (this.exitOnError)
        System.exit(1);
      else
        return false;
    } catch (InterruptedException e) {
      System.err.println("ERROR: Could not read manager lock, not running" + e.getMessage());
      if (this.exitOnError)
        System.exit(1);
      else
        return false;
    }
    return true;
  }
}
