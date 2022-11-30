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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

//TODO use zoocache? - ACCUMULO-1297
//TODO handle zookeeper being down gracefully - ACCUMULO-1297

public class ZooStore<T> implements TStore<T> {

  private static final Logger log = LoggerFactory.getLogger(ZooStore.class);
  private String path;
  private ZooReaderWriter zk;
  private String lastReserved = "";
  private Set<Long> reserved;
  private Map<Long,Long> defered;
  private static final SecureRandom random = new SecureRandom();
  private long statusChangeEvents = 0;
  private int reservationsWaiting = 0;

  private byte[] serialize(Object o) {

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(o);
      oos.close();

      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressFBWarnings(value = "OBJECT_DESERIALIZATION",
      justification = "unsafe to store arbitrary serialized objects like this, but needed for now"
          + " for backwards compatibility")
  private Object deserialize(byte[] ser) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(ser);
      ObjectInputStream ois = new ObjectInputStream(bais);
      return ois.readObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getTXPath(long tid) {
    return FastFormat.toHexString(path + "/tx_", tid, "");
  }

  private long parseTid(String txdir) {
    return Long.parseLong(txdir.split("_")[1], 16);
  }

  public ZooStore(String path, ZooReaderWriter zk) throws KeeperException, InterruptedException {

    this.path = path;
    this.zk = zk;
    this.reserved = new HashSet<>();
    this.defered = new HashMap<>();

    zk.putPersistentData(path, new byte[0], NodeExistsPolicy.SKIP);
  }

  /**
   * For testing only
   */
  ZooStore() {}

  @Override
  public long create() {
    while (true) {
      try {
        // looking at the code for SecureRandom, it appears to be thread safe
        long tid = random.nextLong() & 0x7fffffffffffffffL;
        zk.putPersistentData(getTXPath(tid), TStatus.NEW.name().getBytes(UTF_8),
            NodeExistsPolicy.FAIL);
        return tid;
      } catch (NodeExistsException nee) {
        // exist, so just try another random #
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public long reserve() {
    try {
      while (true) {

        long events;
        synchronized (this) {
          events = statusChangeEvents;
        }

        List<String> txdirs = new ArrayList<>(zk.getChildren(path));
        Collections.sort(txdirs);

        synchronized (this) {
          if (!txdirs.isEmpty() && txdirs.get(txdirs.size() - 1).compareTo(lastReserved) <= 0) {
            lastReserved = "";
          }
        }

        for (String txdir : txdirs) {
          long tid = parseTid(txdir);

          synchronized (this) {
            // this check makes reserve pick up where it left off, so that it cycles through all as
            // it is repeatedly called.... failing to do so can lead to
            // starvation where fate ops that sort higher and hold a lock are never reserved.
            if (txdir.compareTo(lastReserved) <= 0) {
              continue;
            }

            if (defered.containsKey(tid)) {
              if (defered.get(tid) < System.currentTimeMillis()) {
                defered.remove(tid);
              } else {
                continue;
              }
            }
            if (reserved.contains(tid)) {
              continue;
            } else {
              reserved.add(tid);
              lastReserved = txdir;
            }
          }

          // have reserved id, status should not change

          try {
            TStatus status = TStatus.valueOf(new String(zk.getData(path + "/" + txdir), UTF_8));
            if (status == TStatus.SUBMITTED || status == TStatus.IN_PROGRESS
                || status == TStatus.FAILED_IN_PROGRESS) {
              return tid;
            } else {
              unreserve(tid);
            }
          } catch (NoNodeException nne) {
            // node deleted after we got the list of children, its ok
            unreserve(tid);
          } catch (Exception e) {
            unreserve(tid);
            throw e;
          }
        }

        synchronized (this) {
          // suppress lgtm alert - synchronized variable is not always true
          if (events == statusChangeEvents) { // lgtm [java/constant-comparison]
            if (defered.isEmpty()) {
              this.wait(5000);
            } else {
              Long minTime = Collections.min(defered.values());
              long waitTime = minTime - System.currentTimeMillis();
              if (waitTime > 0) {
                this.wait(Math.min(waitTime, 5000));
              }
            }
          }
        }
      }
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reserve(long tid) {
    synchronized (this) {
      reservationsWaiting++;
      try {
        while (reserved.contains(tid)) {
          try {
            this.wait(1000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }

        reserved.add(tid);
      } finally {
        reservationsWaiting--;
      }
    }
  }

  /**
   * Attempt to reserve transaction
   *
   * @param tid transaction id
   * @return true if reserved by this call, false if already reserved
   */
  @Override
  public boolean tryReserve(long tid) {
    synchronized (this) {
      if (!reserved.contains(tid)) {
        reserve(tid);
        return true;
      }
      return false;
    }
  }

  private void unreserve(long tid) {
    synchronized (this) {
      if (!reserved.remove(tid)) {
        throw new IllegalStateException(
            "Tried to unreserve id that was not reserved " + FateTxId.formatTid(tid));
      }

      // do not want this unreserve to unesc wake up threads in reserve()... this leads to infinite
      // loop when tx is stuck in NEW...
      // only do this when something external has called reserve(tid)...
      if (reservationsWaiting > 0) {
        this.notifyAll();
      }
    }
  }

  @Override
  public void unreserve(long tid, long deferTime) {

    if (deferTime < 0) {
      throw new IllegalArgumentException("deferTime < 0 : " + deferTime);
    }

    synchronized (this) {
      if (!reserved.remove(tid)) {
        throw new IllegalStateException(
            "Tried to unreserve id that was not reserved " + FateTxId.formatTid(tid));
      }

      if (deferTime > 0) {
        defered.put(tid, System.currentTimeMillis() + deferTime);
      }

      this.notifyAll();
    }

  }

  private void verifyReserved(long tid) {
    synchronized (this) {
      if (!reserved.contains(tid)) {
        throw new IllegalStateException(
            "Tried to operate on unreserved transaction " + FateTxId.formatTid(tid));
      }
    }
  }

  private static final int RETRIES = 10;

  @Override
  public Repo<T> top(long tid) {
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
        @SuppressWarnings("unchecked")
        var deserialized = (Repo<T>) deserialize(ser);
        return deserialized;
      } catch (KeeperException.NoNodeException ex) {
        log.debug("zookeeper error reading " + txpath + ": " + ex, ex);
        sleepUninterruptibly(100, MILLISECONDS);
        continue;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  private String findTop(String txpath) throws KeeperException, InterruptedException {
    List<String> ops = zk.getChildren(txpath);

    ops = new ArrayList<>(ops);

    String max = "";

    for (String child : ops) {
      if (child.startsWith("repo_") && child.compareTo(max) > 0) {
        max = child;
      }
    }

    if (max.equals("")) {
      return null;
    }

    return max;
  }

  @Override
  public void push(long tid, Repo<T> repo) throws StackOverflowException {
    verifyReserved(tid);

    String txpath = getTXPath(tid);
    try {
      String top = findTop(txpath);
      if (top != null && Long.parseLong(top.split("_")[1]) > 100) {
        throw new StackOverflowException("Repo stack size too large");
      }

      zk.putPersistentSequential(txpath + "/repo_", serialize(repo));
    } catch (StackOverflowException soe) {
      throw soe;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void pop(long tid) {
    verifyReserved(tid);

    try {
      String txpath = getTXPath(tid);
      String top = findTop(txpath);
      if (top == null) {
        throw new IllegalStateException("Tried to pop when empty " + FateTxId.formatTid(tid));
      }
      zk.recursiveDelete(txpath + "/" + top, NodeMissingPolicy.SKIP);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private TStatus _getStatus(long tid) {
    try {
      return TStatus.valueOf(new String(zk.getData(getTXPath(tid)), UTF_8));
    } catch (NoNodeException nne) {
      return TStatus.UNKNOWN;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TStatus getStatus(long tid) {
    verifyReserved(tid);
    return _getStatus(tid);
  }

  @Override
  public TStatus waitForStatusChange(long tid, EnumSet<TStatus> expected) {
    while (true) {
      long events;
      synchronized (this) {
        events = statusChangeEvents;
      }

      TStatus status = _getStatus(tid);
      if (expected.contains(status)) {
        return status;
      }

      synchronized (this) {
        // suppress lgtm alert - synchronized variable is not always true
        if (events == statusChangeEvents) { // lgtm [java/constant-comparison]
          try {
            this.wait(5000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  @Override
  public void setStatus(long tid, TStatus status) {
    verifyReserved(tid);

    try {
      zk.putPersistentData(getTXPath(tid), status.name().getBytes(UTF_8),
          NodeExistsPolicy.OVERWRITE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    synchronized (this) {
      statusChangeEvents++;
    }

  }

  @Override
  public void delete(long tid) {
    verifyReserved(tid);

    try {
      zk.recursiveDelete(getTXPath(tid), NodeMissingPolicy.SKIP);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setTransactionInfo(long tid, Fate.TxInfo txInfo, Serializable so) {
    verifyReserved(tid);

    try {
      if (so instanceof String) {
        zk.putPersistentData(getTXPath(tid) + "/" + txInfo, ("S " + so).getBytes(UTF_8),
            NodeExistsPolicy.OVERWRITE);
      } else {
        byte[] sera = serialize(so);
        byte[] data = new byte[sera.length + 2];
        System.arraycopy(sera, 0, data, 2, sera.length);
        data[0] = 'O';
        data[1] = ' ';
        zk.putPersistentData(getTXPath(tid) + "/" + txInfo, data, NodeExistsPolicy.OVERWRITE);
      }
    } catch (Exception e2) {
      throw new RuntimeException(e2);
    }
  }

  @Override
  public Serializable getTransactionInfo(long tid, Fate.TxInfo txInfo) {
    verifyReserved(tid);

    try {
      byte[] data = zk.getData(getTXPath(tid) + "/" + txInfo);

      if (data[0] == 'O') {
        byte[] sera = new byte[data.length - 2];
        System.arraycopy(data, 2, sera, 0, sera.length);
        return (Serializable) deserialize(sera);
      } else if (data[0] == 'S') {
        return new String(data, 2, data.length - 2, UTF_8);
      } else {
        throw new IllegalStateException("Bad node data " + txInfo);
      }
    } catch (NoNodeException nne) {
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Long> list() {
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

  @Override
  public long timeCreated(long tid) {
    verifyReserved(tid);

    try {
      Stat stat = zk.getZooKeeper().exists(getTXPath(tid), false);
      return stat.getCtime();
    } catch (Exception e) {
      return 0;
    }
  }

  @Override
  public List<ReadOnlyRepo<T>> getStack(long tid) {
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

      ArrayList<ReadOnlyRepo<T>> dops = new ArrayList<>();

      for (String child : ops) {
        if (child.startsWith("repo_")) {
          byte[] ser;
          try {
            ser = zk.getData(txpath + "/" + child);
            @SuppressWarnings("unchecked")
            var repo = (ReadOnlyRepo<T>) deserialize(ser);
            dops.add(repo);
          } catch (KeeperException.NoNodeException e) {
            // children changed so start over
            continue outer;
          } catch (KeeperException | InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }

      return dops;
    }
  }
}
