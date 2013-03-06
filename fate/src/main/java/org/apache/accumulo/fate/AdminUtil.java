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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.fate.TStore.TStatus;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.KeeperException;

/**
 * A utility to administer FATE operations
 */
public class AdminUtil<T> {
  public void print(ZooStore<T> zs, IZooReaderWriter zk, String lockPath) throws KeeperException, InterruptedException {
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
            String lda[] = new String(data).split(":");

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
        System.err.println("Failed to read locks for " + id + " continuing");
      }
    }

    List<Long> transactions = zs.list();

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
      Repo<T> repo = zs.top(tid);
      if (repo != null)
        top = repo.getDescription();

      TStatus status = null;
      status = zs.getStatus(tid);

      zs.unreserve(tid, 0);

      System.out.printf("txid: %016x  status: %-18s  op: %-15s  locked: %-15s locking: %-15s top: %s%n", tid, status, debug, hlocks, wlocks, top);
    }

    if (heldLocks.size() != 0 || waitingLocks.size() != 0) {
      System.out.println();
      System.out.println("The following locks did not have an associated FATE operation");
      System.out.println();
      for (Entry<Long,List<String>> entry : heldLocks.entrySet())
        System.out.printf("txid: %016x  locked: %s%n", entry.getKey(), entry.getValue());

      for (Entry<Long,List<String>> entry : waitingLocks.entrySet())
        System.out.printf("txid: %016x  locking: %s%n", entry.getKey(), entry.getValue());
    }
  }

  public void prepDelete(ZooStore<T> zs, IZooReaderWriter zk, String path, String txidStr) {
    checkGlobalLock(zk, path);

    long txid = Long.parseLong(txidStr, 16);
    zs.reserve(txid);
    zs.delete(txid);
    zs.unreserve(txid, 0);
  }

  public void prepFail(ZooStore<T> zs, IZooReaderWriter zk, String path, String txidStr) {
    checkGlobalLock(zk, path);

    long txid = Long.parseLong(txidStr, 16);
    zs.reserve(txid);
    zs.setStatus(txid, TStatus.FAILED_IN_PROGRESS);
    zs.unreserve(txid, 0);
  }

  public void deleteLocks(ZooStore<T> zs, IZooReaderWriter zk, String path, String txidStr) throws KeeperException, InterruptedException {
    // delete any locks assoc w/ fate operation
    List<String> lockedIds = zk.getChildren(path);

    for (String id : lockedIds) {
      List<String> lockNodes = zk.getChildren(path + "/" + id);
      for (String node : lockNodes) {
        String lockPath = path + "/" + id + "/" + node;
        byte[] data = zk.getData(path + "/" + id + "/" + node, null);
        String lda[] = new String(data).split(":");
        if (lda[1].equals(txidStr))
          zk.recursiveDelete(lockPath, NodeMissingPolicy.SKIP);
      }
    }
  }

  public void checkGlobalLock(IZooReaderWriter zk, String path) {
    try {
      if (ZooLock.getLockData(zk.getZooKeeper(), path) != null) {
        System.err.println("ERROR: Master lock is held, not running");
        System.exit(-1);
      }
    } catch (KeeperException e) {
      System.err.println("ERROR: Could not read master lock, not running " + e.getMessage());
      System.exit(-1);
    } catch (InterruptedException e) {
      System.err.println("ERROR: Could not read master lock, not running" + e.getMessage());
      System.exit(-1);
    }
  }
}
