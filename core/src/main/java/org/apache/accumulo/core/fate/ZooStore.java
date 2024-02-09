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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;

//TODO use zoocache? - ACCUMULO-1297
//TODO handle zookeeper being down gracefully - ACCUMULO-1297

public class ZooStore<T> extends AbstractFateStore<T> {

  private static final Logger log = LoggerFactory.getLogger(ZooStore.class);
  private static final FateInstanceType fateInstanceType = FateInstanceType.META;
  private String path;
  private ZooReaderWriter zk;

  private String getTXPath(FateId fateId) {
    return path + "/tx_" + fateId.getHexTid();
  }

  public ZooStore(String path, ZooReaderWriter zk) throws KeeperException, InterruptedException {
    this(path, zk, DEFAULT_MAX_DEFERRED, DEFAULT_FATE_ID_GENERATOR);
  }

  @VisibleForTesting
  public ZooStore(String path, ZooReaderWriter zk, int maxDeferred, FateIdGenerator fateIdGenerator)
      throws KeeperException, InterruptedException {
    super(maxDeferred, fateIdGenerator);
    this.path = path;
    this.zk = zk;

    zk.putPersistentData(path, new byte[0], NodeExistsPolicy.SKIP);
  }

  /**
   * For testing only
   */
  ZooStore() {}

  @Override
  public FateId create() {
    while (true) {
      try {
        // looking at the code for SecureRandom, it appears to be thread safe
        long tid = RANDOM.get().nextLong() & 0x7fffffffffffffffL;
        FateId fateId = FateId.from(fateInstanceType, tid);
        zk.putPersistentData(getTXPath(fateId), new NodeValue(TStatus.NEW).serialize(),
            NodeExistsPolicy.FAIL);
        return fateId;
      } catch (NodeExistsException nee) {
        // exist, so just try another random #
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  protected void create(FateId fateId, FateKey key) {
    try {
      zk.putPersistentData(getTXPath(fateId), new NodeValue(TStatus.NEW, key).serialize(),
          NodeExistsPolicy.FAIL);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected Pair<TStatus,Optional<FateKey>> getStatusAndKey(FateId fateId) {
    final NodeValue node = getNode(fateId);
    return new Pair<>(node.status, node.fateKey);
  }

  private class FateTxStoreImpl extends AbstractFateTxStoreImpl<T> {

    private FateTxStoreImpl(FateId fateId, boolean isReserved) {
      super(fateId, isReserved);
    }

    private static final int RETRIES = 10;

    @Override
    public Repo<T> top() {
      verifyReserved(false);

      for (int i = 0; i < RETRIES; i++) {
        String txpath = getTXPath(fateId);
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

      String txpath = getTXPath(fateId);
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
        String txpath = getTXPath(fateId);
        String top = findTop(txpath);
        if (top == null) {
          throw new IllegalStateException("Tried to pop when empty " + fateId);
        }
        zk.recursiveDelete(txpath + "/" + top, NodeMissingPolicy.SKIP);
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void setStatus(TStatus status) {
      verifyReserved(true);

      try {
        zk.putPersistentData(getTXPath(fateId), new NodeValue(status).serialize(),
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
        zk.recursiveDelete(getTXPath(fateId), NodeMissingPolicy.SKIP);
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void setTransactionInfo(Fate.TxInfo txInfo, Serializable so) {
      verifyReserved(true);

      try {
        zk.putPersistentData(getTXPath(fateId) + "/" + txInfo, serializeTxInfo(so),
            NodeExistsPolicy.OVERWRITE);
      } catch (KeeperException | InterruptedException e2) {
        throw new IllegalStateException(e2);
      }
    }

    @Override
    public Serializable getTransactionInfo(Fate.TxInfo txInfo) {
      verifyReserved(false);

      return ZooStore.this.getTransactionInfo(txInfo, fateId);
    }

    @Override
    public long timeCreated() {
      verifyReserved(false);

      try {
        Stat stat = zk.getZooKeeper().exists(getTXPath(fateId), false);
        return stat.getCtime();
      } catch (Exception e) {
        return 0;
      }
    }

    @Override
    public List<ReadOnlyRepo<T>> getStack() {
      verifyReserved(false);
      String txpath = getTXPath(fateId);

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

  private Serializable getTransactionInfo(TxInfo txInfo, FateId fateId) {
    try {
      return deserializeTxInfo(txInfo, zk.getData(getTXPath(fateId) + "/" + txInfo));
    } catch (NoNodeException nne) {
      return null;
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected TStatus _getStatus(FateId fateId) {
    return getNode(fateId).status;
  }

  @Override
  protected Optional<FateKey> getKey(FateId fateId) {
    return getNode(fateId).fateKey;
  }

  private NodeValue getNode(FateId fateId) {
    try {
      return new NodeValue(zk.getData(getTXPath(fateId)));
    } catch (NoNodeException nne) {
      return new NodeValue(TStatus.UNKNOWN);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected FateTxStore<T> newFateTxStore(FateId fateId, boolean isReserved) {
    return new FateTxStoreImpl(fateId, isReserved);
  }

  @Override
  protected FateInstanceType getInstanceType() {
    return fateInstanceType;
  }

  @Override
  protected Stream<FateIdStatus> getTransactions() {
    try {
      return zk.getChildren(path).stream().map(strTxid -> {
        String hexTid = strTxid.split("_")[1];
        FateId fateId = FateId.from(fateInstanceType, hexTid);
        // Memoizing for two reasons. First the status may never be requested, so in that case avoid
        // the lookup. Second, if its requested multiple times the result will always be consistent.
        Supplier<TStatus> statusSupplier = Suppliers.memoize(() -> _getStatus(fateId));
        return new FateIdStatusBase(fateId) {
          @Override
          public TStatus getStatus() {
            return statusSupplier.get();
          }
        };
      });
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  protected static class NodeValue {
    final TStatus status;
    final Optional<FateKey> fateKey;

    private NodeValue(byte[] serialized) {
      try (DataInputBuffer buffer = new DataInputBuffer()) {
        buffer.reset(serialized, serialized.length);
        this.status = TStatus.valueOf(buffer.readUTF());
        this.fateKey = deserializeFateKey(buffer);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private NodeValue(TStatus status) {
      this(status, null);
    }

    private NodeValue(TStatus status, FateKey fateKey) {
      this.status = Objects.requireNonNull(status);
      this.fateKey = Optional.ofNullable(fateKey);
    }

    private Optional<FateKey> deserializeFateKey(DataInputBuffer buffer) throws IOException {
      int length = buffer.readInt();
      if (length > 0) {
        return Optional.of(FateKey.deserialize(buffer.readNBytes(length)));
      }
      return Optional.empty();
    }

    byte[] serialize() {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(baos)) {
        dos.writeUTF(status.name());
        if (fateKey.isPresent()) {
          byte[] serialized = fateKey.orElseThrow().getSerialized();
          dos.writeInt(serialized.length);
          dos.write(serialized);
        } else {
          dos.writeInt(0);
        }
        dos.close();
        return baos.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

  }
}
