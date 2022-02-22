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
package org.apache.accumulo.fate;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A simplified version of ZooStore to be shared across packages.
 */
public class FateZooStore {
  private final static Logger log = LoggerFactory.getLogger(FateZooStore.class);

  protected static final int RETRIES = 10;
  protected final String path;
  protected final ZooReaderWriter zk;
  protected final Set<Long> reserved;
  protected final AtomicReference<String> lastReserved = new AtomicReference<>("");
  protected final Map<Long,Long> deferred;

  protected long statusChangeEvents = 0;
  protected int reservationsWaiting = 0;

  public FateZooStore(String path, ZooReaderWriter zk)
      throws InterruptedException, KeeperException {
    this.path = path;
    this.zk = zk;
    this.reserved = new HashSet<>();
    deferred = new HashMap<>();
    zk.putPersistentData(path, new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
  }

  protected String getTXPath(long tid) {
    return String.format("%s/tx_%016x", path, tid);
  }

  protected long parseTid(String txdir) {
    return Long.parseLong(txdir.split("_")[1], 16);
  }

  @SuppressFBWarnings(value = "OBJECT_DESERIALIZATION",
      justification = "unsafe to store arbitrary serialized objects like this, but needed for now"
          + " for backwards compatibility")
  private Serializable deserialize(byte[] ser) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(ser);
      ObjectInputStream ois = new ObjectInputStream(bais);
      return (Serializable) ois.readObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected String findTop(String txpath) throws KeeperException, InterruptedException {
    List<String> ops = zk.getChildren(txpath);
    ops = new ArrayList<>(ops);
    String max = "";
    for (String child : ops)
      if (child.startsWith("repo_") && child.compareTo(max) > 0)
        max = child;

    if (max.equals(""))
      return null;

    return max;
  }

  protected void verifyReserved(long tid) {
    synchronized (this) {
      if (!reserved.contains(tid))
        throw new IllegalStateException(
            "Tried to operate on unreserved transaction " + FateTxId.formatTid(tid));
    }
  }

  public List<Long> listTransactions() {
    try {
      ArrayList<Long> l = new ArrayList<>();
      List<String> transactions = zk.getChildren(path);
      for (String txid : transactions) {
        l.add(parseTid(txid));
      }
      return l;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<Serializable> getStack(Long tid) {
    String txpath = getTXPath(tid);

    outer: while (true) {
      List<String> ops;
      try {
        ops = zk.getChildren(txpath);
      } catch (KeeperException.NoNodeException e) {
        return Collections.emptyList();
      } catch (KeeperException | InterruptedException e1) {
        throw new RuntimeException(e1);
      }

      ops = new ArrayList<>(ops);
      ops.sort(Collections.reverseOrder());

      ArrayList<Serializable> deserializedOps = new ArrayList<>();

      for (String child : ops) {
        if (child.startsWith("repo_")) {
          byte[] ser;
          try {
            ser = zk.getData(txpath + "/" + child);
            Serializable repo = deserialize(ser);
            deserializedOps.add(repo);
          } catch (KeeperException.NoNodeException e) {
            // children changed so start over
            continue outer;
          } catch (KeeperException | InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
      return deserializedOps;
    }
  }

  public void reserve(long tid) {
    synchronized (this) {
      reservationsWaiting++;
      try {
        while (reserved.contains(tid))
          try {
            this.wait(1000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }

        reserved.add(tid);
      } finally {
        reservationsWaiting--;
      }
    }
  }

  public void unreserve(long tid) {
    synchronized (this) {
      if (!reserved.remove(tid))
        throw new IllegalStateException(
            "Tried to unreserve id that was not reserved " + FateTxId.formatTid(tid));

      // do not want this unreserve to unesc wake up threads in reserve()... this leads to infinite
      // loop when tx is stuck in NEW...
      // only do this when something external has called reserve(tid)...
      if (reservationsWaiting > 0)
        this.notifyAll();
    }
  }

  public Serializable getProperty(long tid, String prop) {
    verifyReserved(tid);

    try {
      byte[] data = zk.getData(getTXPath(tid) + "/prop_" + prop);

      if (data[0] == 'O') {
        byte[] sera = new byte[data.length - 2];
        System.arraycopy(data, 2, sera, 0, sera.length);
        return deserialize(sera);
      } else if (data[0] == 'S') {
        return new String(data, 2, data.length - 2, UTF_8);
      } else {
        throw new IllegalStateException("Bad property data " + prop);
      }
    } catch (KeeperException.NoNodeException nne) {
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Object top(long tid) {
    verifyReserved(tid);

    for (int i = 0; i < RETRIES; i++) {
      String txpath = getTXPath(tid);
      try {
        String top;
        try {
          top = findTop(txpath);
          if (top == null) {
            return null;
          }
        } catch (KeeperException.NoNodeException ex) {
          throw new RuntimeException(ex);
        }

        byte[] ser = zk.getData(txpath + "/" + top);
        return deserialize(ser);
      } catch (KeeperException.NoNodeException ex) {
        log.debug("zookeeper error reading " + txpath + ": " + ex, ex);
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  public FateTransactionStatus getTStatus(long tid) {
    verifyReserved(tid);

    try {
      return FateTransactionStatus.valueOf(new String(zk.getData(getTXPath(tid)), UTF_8));
    } catch (KeeperException.NoNodeException nne) {
      return FateTransactionStatus.UNKNOWN;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public long timeCreated(long tid) {
    verifyReserved(tid);

    try {
      Stat stat = zk.getZooKeeper().exists(getTXPath(tid), false);
      return stat.getCtime();
    } catch (Exception e) {
      return 0;
    }
  }

  public void setStatus(long tid, FateTransactionStatus status) {
    verifyReserved(tid);

    try {
      zk.putPersistentData(getTXPath(tid), status.name().getBytes(UTF_8),
          ZooUtil.NodeExistsPolicy.OVERWRITE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void delete(long tid) {
    verifyReserved(tid);
    try {
      zk.recursiveDelete(getTXPath(tid), ZooUtil.NodeMissingPolicy.SKIP);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
