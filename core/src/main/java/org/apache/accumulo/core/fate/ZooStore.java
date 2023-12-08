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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

//TODO use zoocache? - ACCUMULO-1297
//TODO handle zookeeper being down gracefully - ACCUMULO-1297

public class ZooStore<T> implements FateStore<T> {

  private static final Logger log = LoggerFactory.getLogger(ZooStore.class);
  private String path;
  private ZooReaderWriter zk;
  private Set<Long> reserved;
  private Map<Long,Long> defered;

  // This is incremented each time a transaction was unreserved that was non new
  private final SignalCount unreservedNonNewCount = new SignalCount();

  // This is incremented each time a transaction is unreserved that was runnable
  private final SignalCount unreservedRunnableCount = new SignalCount();

  private byte[] serialize(Object o) {

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(o);
      oos.close();

      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
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
        long tid = RANDOM.get().nextLong() & 0x7fffffffffffffffL;
        zk.putPersistentData(getTXPath(tid), TStatus.NEW.name().getBytes(UTF_8),
            NodeExistsPolicy.FAIL);
        return tid;
      } catch (NodeExistsException nee) {
        // exist, so just try another random #
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  public FateTxStore<T> reserve(long tid) {
    synchronized (ZooStore.this) {
      while (reserved.contains(tid)) {
        try {
          ZooStore.this.wait(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }

      reserved.add(tid);
      return new FateTxStoreImpl(tid, true);
    }
  }

  /**
   * Attempt to reserve transaction
   *
   * @param tid transaction id
   * @return true if reserved by this call, false if already reserved
   */
  @Override
  public Optional<FateTxStore<T>> tryReserve(long tid) {
    synchronized (this) {
      if (!reserved.contains(tid)) {
        return Optional.of(reserve(tid));
      }
      return Optional.empty();
    }
  }

  private class FateTxStoreImpl implements FateTxStore<T> {

    private final long tid;
    private final boolean isReserved;

    private TStatus observedStatus = null;

    private FateTxStoreImpl(long tid, boolean isReserved) {
      this.tid = tid;
      this.isReserved = isReserved;
    }

    @Override
    public void unreserve(long deferTime) {

      if (deferTime < 0) {
        throw new IllegalArgumentException("deferTime < 0 : " + deferTime);
      }

      synchronized (ZooStore.this) {
        if (!reserved.remove(tid)) {
          throw new IllegalStateException(
              "Tried to unreserve id that was not reserved " + FateTxId.formatTid(tid));
        }

        // notify any threads waiting to reserve
        ZooStore.this.notifyAll();

        if (deferTime > 0) {
          defered.put(tid, System.currentTimeMillis() + deferTime);
        }
      }

      if (observedStatus != null && isRunnable(observedStatus)) {
        unreservedRunnableCount.increment();
      }

      if (observedStatus != TStatus.NEW) {
        unreservedNonNewCount.increment();
      }
    }

    private void verifyReserved(boolean isWrite) {
      if (!isReserved && isWrite) {
        throw new IllegalStateException("Attempted write on unreserved FATE transaction.");
      }

      if (isReserved) {
        synchronized (ZooStore.this) {
          if (!reserved.contains(tid)) {
            throw new IllegalStateException(
                "Tried to operate on unreserved transaction " + FateTxId.formatTid(tid));
          }
        }
      }
    }

    private static final int RETRIES = 10;

    @Override
    public Repo<T> top() {
      verifyReserved(false);

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
            throw new IllegalStateException(ex);
          }

          byte[] ser = zk.getData(txpath + "/" + top);
          @SuppressWarnings("unchecked")
          var deserialized = (Repo<T>) deserialize(ser);
          return deserialized;
        } catch (KeeperException.NoNodeException ex) {
          log.debug("zookeeper error reading " + txpath + ": " + ex, ex);
          sleepUninterruptibly(100, MILLISECONDS);
          continue;
        } catch (KeeperException | InterruptedException e) {
          throw new IllegalStateException(e);
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
    public void push(Repo<T> repo) throws StackOverflowException {
      verifyReserved(true);

      String txpath = getTXPath(tid);
      try {
        String top = findTop(txpath);
        if (top != null && Long.parseLong(top.split("_")[1]) > 100) {
          throw new StackOverflowException("Repo stack size too large");
        }

        zk.putPersistentSequential(txpath + "/repo_", serialize(repo));
      } catch (StackOverflowException soe) {
        throw soe;
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void pop() {
      verifyReserved(true);

      try {
        String txpath = getTXPath(tid);
        String top = findTop(txpath);
        if (top == null) {
          throw new IllegalStateException("Tried to pop when empty " + FateTxId.formatTid(tid));
        }
        zk.recursiveDelete(txpath + "/" + top, NodeMissingPolicy.SKIP);
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public TStatus getStatus() {
      verifyReserved(false);
      var status = _getStatus(tid);
      observedStatus = status;
      return _getStatus(tid);
    }

    @Override
    public TStatus waitForStatusChange(EnumSet<TStatus> expected) {
      Preconditions.checkState(!isReserved,
          "Attempted to wait for status change while reserved " + FateTxId.formatTid(getID()));
      while (true) {

        long countBefore = unreservedNonNewCount.getCount();

        TStatus status = _getStatus(tid);
        if (expected.contains(status)) {
          return status;
        }

        unreservedNonNewCount.waitFor(count -> count != countBefore, 1000, () -> true);
      }
    }

    @Override
    public void setStatus(TStatus status) {
      verifyReserved(true);

      try {
        zk.putPersistentData(getTXPath(tid), status.name().getBytes(UTF_8),
            NodeExistsPolicy.OVERWRITE);
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }

      observedStatus = status;
    }

    @Override
    public void delete() {
      verifyReserved(true);

      try {
        zk.recursiveDelete(getTXPath(tid), NodeMissingPolicy.SKIP);
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void setTransactionInfo(Fate.TxInfo txInfo, Serializable so) {
      verifyReserved(true);

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
      } catch (KeeperException | InterruptedException e2) {
        throw new IllegalStateException(e2);
      }
    }

    @Override
    public Serializable getTransactionInfo(Fate.TxInfo txInfo) {
      verifyReserved(false);

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
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public long timeCreated() {
      verifyReserved(false);

      try {
        Stat stat = zk.getZooKeeper().exists(getTXPath(tid), false);
        return stat.getCtime();
      } catch (Exception e) {
        return 0;
      }
    }

    @Override
    public long getID() {
      return tid;
    }

    @Override
    public List<ReadOnlyRepo<T>> getStack() {
      verifyReserved(false);
      String txpath = getTXPath(tid);

      outer: while (true) {
        List<String> ops;
        try {
          ops = zk.getChildren(txpath);
        } catch (KeeperException.NoNodeException e) {
          return Collections.emptyList();
        } catch (KeeperException | InterruptedException e1) {
          throw new IllegalStateException(e1);
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
              throw new IllegalStateException(e);
            }
          }
        }

        return dops;
      }
    }
  }

  private TStatus _getStatus(long tid) {
    try {
      return TStatus.valueOf(new String(zk.getData(getTXPath(tid)), UTF_8));
    } catch (NoNodeException nne) {
      return TStatus.UNKNOWN;
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public ReadOnlyFateTxStore<T> read(long tid) {
    return new FateTxStoreImpl(tid, false);
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
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  private boolean isRunnable(TStatus status) {
    return status == TStatus.IN_PROGRESS || status == TStatus.FAILED_IN_PROGRESS
        || status == TStatus.SUBMITTED;
  }

  @Override
  public Iterator<Long> runnable(AtomicBoolean keepWaiting) {

    while (keepWaiting.get()) {
      ArrayList<Long> runnableTids = new ArrayList<>();

      final long beforeCount = unreservedRunnableCount.getCount();

      try {

        List<String> transactions = zk.getChildren(path);
        for (String txidStr : transactions) {
          long txid = parseTid(txidStr);
          if (isRunnable(_getStatus(txid))) {
            runnableTids.add(txid);
          }
        }

        synchronized (this) {
          runnableTids.removeIf(txid -> {
            var deferedTime = defered.get(txid);
            if (deferedTime != null) {
              if (deferedTime < System.currentTimeMillis()) {
                return true;
              } else {
                defered.remove(txid);
              }
            }

            if (reserved.contains(txid)) {
              return true;
            }

            return false;
          });
        }

        if (runnableTids.isEmpty()) {
          if (beforeCount == unreservedRunnableCount.getCount()) {
            long waitTime = 5000;
            if (!defered.isEmpty()) {
              Long minTime = Collections.min(defered.values());
              waitTime = minTime - System.currentTimeMillis();
            }

            if (waitTime > 0) {
              unreservedRunnableCount.waitFor(count -> count != beforeCount, waitTime,
                  keepWaiting::get);
            }
          }
        } else {
          return runnableTids.iterator();
        }

      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    return List.<Long>of().iterator();
  }
}
