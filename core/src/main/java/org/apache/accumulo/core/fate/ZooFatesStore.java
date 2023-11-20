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
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.manager.PartitionData;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.core.util.Retry;
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

public class ZooFatesStore<T> implements FatesStore<T> {

  private static final Logger log = LoggerFactory.getLogger(ZooFatesStore.class);
  private String path;
  private ZooReaderWriter zk;

  private Map<Long,Long> defered;

  // The zookeeper lock for the process that running this store instance.
  private final ZooUtil.LockID lockID;

  // TODO this was hastily written code for serialization. Should it use json? It needs better
  // validation of data being serialized (like validate status, lock, and uuid).
  private static class NodeValue {
    final FateStatus status;
    final String lock;
    final String uuid;

    private NodeValue(byte[] serializedData) {
      var fields = new String(serializedData, UTF_8).split(":", 3);
      this.status = FateStatus.valueOf(fields[0]);
      this.lock = fields[1];
      this.uuid = fields[2];
    }

    private NodeValue(FateStatus status, String lock, String uuid) {
      if (lock.isEmpty() && !uuid.isEmpty() || uuid.isEmpty() && !lock.isEmpty()) {
        throw new IllegalArgumentException(
            "For lock and uuid expect neither is empty or both are empty. lock:'" + lock
                + "' uuid:'" + uuid + "'");
      }
      this.status = status;
      this.lock = lock;
      this.uuid = uuid;
    }

    byte[] serialize() {
      return (status.name() + ":" + lock + ":" + uuid).getBytes(UTF_8);
    }

    public boolean isReserved() {
      return !lock.isEmpty();
    }
  }

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

  public ZooFatesStore(String path, ZooReaderWriter zk, ZooUtil.LockID lockID)
      throws KeeperException, InterruptedException {

    this.path = path;
    this.zk = zk;
    this.defered = Collections.synchronizedMap(new HashMap<>());
    this.lockID = lockID;

    zk.putPersistentData(path, new byte[0], NodeExistsPolicy.SKIP);
  }

  /**
   * For testing only
   */
  ZooFatesStore() {
    lockID = null;
  }

  @Override
  public long create() {
    while (true) {
      try {
        // looking at the code for SecureRandom, it appears to be thread safe
        long tid = RANDOM.get().nextLong() & 0x7fffffffffffffffL;
        zk.putPersistentData(getTXPath(tid), new NodeValue(FateStatus.NEW, "", "").serialize(),
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
  public FateStore<T> reserve(long tid) {
    var retry =
        Retry.builder().infiniteRetries().retryAfter(25, MILLISECONDS).incrementBy(25, MILLISECONDS)
            .maxWait(30, SECONDS).backOffFactor(1.5).logInterval(3, MINUTES).createRetry();

    var resFateOp = tryReserve(tid);
    while (resFateOp.isEmpty()) {
      try {
        retry.waitForNextAttempt(log, "Attempting to reserve " + FateTxId.formatTid(tid));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalArgumentException(e);
      }
      resFateOp = tryReserve(tid);
    }

    retry.logCompletion(log, "Attempting to reserve " + FateTxId.formatTid(tid));
    return resFateOp.orElseThrow();
  }

  /**
   * Attempt to reserve transaction
   *
   * @param tid transaction id
   * @return true if reserved by this call, false if already reserved
   */
  @Override
  public Optional<FateStore<T>> tryReserve(long tid) {

    // uniquely identify this attempt to reserve the fate operation data
    var uuid = UUID.randomUUID();

    try {
      byte[] newValue = zk.mutateExisting(getTXPath(tid), currentValue -> {
        var nodeVal = new NodeValue(currentValue);
        // The uuid handle the case where there was a ZK server fault and the write for this thread
        // went through but that was not acknowledged and we are reading our own write for 2nd time.
        if (!nodeVal.isReserved() || nodeVal.uuid.equals(uuid.toString())) {
          return new NodeValue(nodeVal.status, lockID.serialize(""), uuid.toString()).serialize();
        } else {
          // returning null will not change the value AND the null will be returned
          return null;
        }
      });

      if (newValue != null) {
        // clear any deferment if it exists, it may or may not be set when its unreserved
        defered.remove(tid);
        return Optional.of(new FateStoreImpl(tid, uuid));
      } else {
        return Optional.empty();
      }
    } catch (NoNodeException e) {
      log.trace("Fate op does not exists so can not reserve", e);
      return Optional.empty();
    } catch (KeeperException | InterruptedException | AcceptableThriftTableOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private class ReadOnlyFateStoreImpl implements ReadOnlyFateStore<T> {
    private static final int RETRIES = 10;

    protected final long tid;

    protected ReadOnlyFateStoreImpl(long tid) {
      this.tid = tid;
    }

    @Override
    public Repo<T> top() {
      checkState(false);
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

    String findTop(String txpath) throws KeeperException, InterruptedException {
      checkState(false);
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

    private FateStatus _getStatus() {
      checkState(false);
      try {
        return new NodeValue(zk.getData(getTXPath(tid))).status;
      } catch (NoNodeException nne) {
        return FateStatus.UNKNOWN;
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public FateStatus getStatus() {
      checkState(false);
      return _getStatus();
    }

    @Override
    public FateStatus waitForStatusChange(EnumSet<FateStatus> expected) {
      checkState(false);
      // TODO make the max time a function of the number of concurrent callers, as the number of
      // concurrent callers increases then increase the max wait time
      // TODO could support signaling within this instance for known events
      // TODO made the maxWait low so this would be responsive... that may put a lot of load in the
      // case there are lots of things waiting...
      var retry = Retry.builder().infiniteRetries().retryAfter(25, MILLISECONDS)
          .incrementBy(25, MILLISECONDS).maxWait(1, SECONDS).backOffFactor(1.5)
          .logInterval(3, MINUTES).createRetry();

      while (true) {
        FateStatus status = _getStatus();
        if (expected.contains(status)) {
          retry.logCompletion(log, "Waiting on status change for " + FateTxId.formatTid(tid)
              + " expected:" + expected + " status:" + status);
          return status;
        }

        try {
          retry.waitForNextAttempt(log, "Waiting on status change for " + FateTxId.formatTid(tid)
              + " expected:" + expected + " status:" + status);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
    }

    @Override
    public Serializable getTransactionInfo(Fate.TxInfo txInfo) {
      checkState(false);
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
      checkState(false);
      try {
        Stat stat = zk.getZooKeeper().exists(getTXPath(tid), false);
        return stat.getCtime();
      } catch (Exception e) {
        return 0;
      }
    }

    @Override
    public long getID() {
      checkState(false);
      return tid;
    }

    @Override
    public List<ReadOnlyRepo<T>> getStack() {
      checkState(false);
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

    protected void checkState(boolean unreserving) {}

  }

  private class FateStoreImpl extends ReadOnlyFateStoreImpl implements FateStore<T> {

    private boolean reserved = true;
    private boolean deleted = false;

    private final UUID uuid;

    protected FateStoreImpl(long tid, UUID uuid) {
      super(tid);
      this.uuid = Objects.requireNonNull(uuid);
    }

    @Override
    public void push(Repo<T> repo) throws StackOverflowException {
      checkState(false);
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
      checkState(false);
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
    public void setStatus(FateStatus status) {
      checkState(false);
      try {
        zk.mutateExisting(getTXPath(tid), currentValue -> {
          var nodeVal = new NodeValue(currentValue);
          Preconditions.checkState(nodeVal.uuid.equals(uuid.toString()),
              "Tried to set status for %s and it was not reserved", FateTxId.formatTid(tid));
          return new NodeValue(status, nodeVal.lock, nodeVal.uuid).serialize();
        });
      } catch (KeeperException | InterruptedException | AcceptableThriftTableOperationException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void delete() {
      checkState(false);
      try {
        zk.recursiveDelete(getTXPath(tid), NodeMissingPolicy.SKIP);
        deleted = true;
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void setTransactionInfo(Fate.TxInfo txInfo, Serializable so) {
      checkState(false);
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

    private void unreserve() {
      checkState(true);
      try {
        if (!deleted) {
          zk.mutateExisting(getTXPath(tid), currentValue -> {
            var nodeVal = new NodeValue(currentValue);
            if (nodeVal.uuid.equals(uuid.toString())) {
              return new NodeValue(nodeVal.status, "", "").serialize();
            } else {
              // possible this is running a 2nd time in zk server fault conditions and its first
              // write went through
              return null;
            }
          });
        }
        reserved = false;
      } catch (KeeperException | InterruptedException | AcceptableThriftTableOperationException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void unreserve(long deferTime) {

      if (deferTime < 0) {
        throw new IllegalArgumentException("deferTime < 0 : " + deferTime);
      }

      if (deferTime > 0) {
        // add to defered before actually unreserving
        defered.put(tid, System.currentTimeMillis() + deferTime);
      }

      unreserve();
    }

    @Override
    protected void checkState(boolean unreserving) {
      super.checkState(unreserving);
      if (!reserved) {
        throw new IllegalStateException("Attempted to use fate store " + FateTxId.formatTid(getID())
            + " after unreserving it.");
      }

      if (!unreserving && deleted) {
        throw new IllegalStateException("Attempted to use fate store for "
            + FateTxId.formatTid(getID()) + " after deleting it.");
      }
    }
  }

  @Override
  public ReadOnlyFateStore<T> read(long tid) {
    return new ReadOnlyFateStoreImpl(tid);
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

  @Override
  public Iterator<Long> runnable(PartitionData partitionData) {
    try {
      ArrayList<Long> runnableTids = new ArrayList<>();
      List<String> transactions = zk.getChildren(path);
      for (String txid : transactions) {
        try {
          var nodeVal = new NodeValue(zk.getData(path + "/" + txid));
          var tid = parseTid(txid);
          if (!nodeVal.isReserved()
              && (nodeVal.status == FateStatus.IN_PROGRESS
                  || nodeVal.status == FateStatus.FAILED_IN_PROGRESS
                  || nodeVal.status == FateStatus.SUBMITTED)
              && tid % partitionData.getTotalInstances() == partitionData.getPartition()) {
            runnableTids.add(tid);
          }
        } catch (NoNodeException nne) {
          // expected race condition that node could be deleted after getting list of children
          log.trace("Skipping missing node {}", txid);
        }
      }

      // Anything that is not runnable in this partition should be removed from the defered set
      defered.keySet().retainAll(runnableTids);

      // Filter out any transactions that are not read to run because of deferment
      runnableTids.removeIf(runnableTid -> {
        var deferedTime = defered.get(runnableTid);
        return deferedTime != null && deferedTime < System.currentTimeMillis();
      });

      return runnableTids.iterator();
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }
}
