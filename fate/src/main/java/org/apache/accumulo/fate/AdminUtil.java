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

import static com.google.common.base.Charsets.UTF_8;

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
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.KeeperException;

/**
 * A utility to administer FATE operations
 */
public class AdminUtil<T> {

  private boolean exitOnError = false;

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

  public void print(ReadOnlyTStore<T> zs, IZooReaderWriter zk, String lockPath) throws KeeperException, InterruptedException {
    print(zs, zk, lockPath, new Formatter(System.out), null, null);
  }

  public void print(ReadOnlyTStore<T> zs, IZooReaderWriter zk, String lockPath, Formatter fmt, Set<Long> filterTxid, EnumSet<TStatus> filterStatus)
      throws KeeperException, InterruptedException {
    Map<Long,List<String>> heldLocks = new HashMap<Long,List<String>>();
    Map<Long,List<String>> waitingLocks = new HashMap<Long,List<String>>();

    List<String> lockedIds = zk.getChildren(lockPath);

    for (String id : lockedIds) {
      try {
        List<String> lockNodes = zk.getChildren(lockPath + "/" + id);
        lockNodes = new ArrayList<String>(lockNodes);
        Collections.sort(lockNodes);

        int pos = 0;
        boolean sawWriteLock = false;

        for (String node : lockNodes) {
          try {
            byte[] data = zk.getData(lockPath + "/" + id + "/" + node, null);
            String lda[] = new String(data, UTF_8).split(":");

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
              tables = new ArrayList<String>();
              locks.put(Long.parseLong(lda[1], 16), tables);
            }

            tables.add(lda[0].charAt(0) + ":" + id);

          } catch (Exception e) {
            e.printStackTrace();
          }
          pos++;
        }

      } catch (Exception e) {
        e.printStackTrace();
        fmt.format("Failed to read locks for %s continuing", id);
      }
    }

    List<Long> transactions = zs.list();

    long txCount = 0;
    for (Long tid : transactions) {

      zs.reserve(tid);

      String debug = (String) zs.getProperty(tid, "debug");

      List<String> hlocks = heldLocks.remove(tid);
      if (hlocks == null)
        hlocks = Collections.emptyList();

      List<String> wlocks = waitingLocks.remove(tid);
      if (wlocks == null)
        wlocks = Collections.emptyList();

      String top = null;
      ReadOnlyRepo<T> repo = zs.top(tid);
      if (repo != null)
        top = repo.getDescription();

      TStatus status = null;
      status = zs.getStatus(tid);

      zs.unreserve(tid, 0);

      if ((filterTxid != null && !filterTxid.contains(tid)) || (filterStatus != null && !filterStatus.contains(status)))
        continue;

      ++txCount;
      fmt.format("txid: %016x  status: %-18s  op: %-15s  locked: %-15s locking: %-15s top: %s%n", tid, status, debug, hlocks, wlocks, top);
    }
    fmt.format(" %s transactions", txCount);

    if (heldLocks.size() != 0 || waitingLocks.size() != 0) {
      fmt.format("%nThe following locks did not have an associated FATE operation%n");
      for (Entry<Long,List<String>> entry : heldLocks.entrySet())
        fmt.format("txid: %016x  locked: %s%n", entry.getKey(), entry.getValue());

      for (Entry<Long,List<String>> entry : waitingLocks.entrySet())
        fmt.format("txid: %016x  locking: %s%n", entry.getKey(), entry.getValue());
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

  public void deleteLocks(TStore<T> zs, IZooReaderWriter zk, String path, String txidStr) throws KeeperException, InterruptedException {
    // delete any locks assoc w/ fate operation
    List<String> lockedIds = zk.getChildren(path);

    for (String id : lockedIds) {
      List<String> lockNodes = zk.getChildren(path + "/" + id);
      for (String node : lockNodes) {
        String lockPath = path + "/" + id + "/" + node;
        byte[] data = zk.getData(path + "/" + id + "/" + node, null);
        String lda[] = new String(data, UTF_8).split(":");
        if (lda[1].equals(txidStr))
          zk.recursiveDelete(lockPath, NodeMissingPolicy.SKIP);
      }
    }
  }

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
