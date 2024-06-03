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
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.ALL_STATUSES;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
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
// TODO 4131 noticed this class is not in the fate.zookeeper package. Should it be?
public class MetaFateStore<T> extends AbstractFateStore<T> {

  private static final Logger log = LoggerFactory.getLogger(MetaFateStore.class);
  private static final FateInstanceType fateInstanceType = FateInstanceType.META;
  private String path;
  private ZooReaderWriter zk;

  private String getTXPath(FateId fateId) {
    return path + "/tx_" + fateId.getTxUUIDStr();
  }

  public MetaFateStore(String path, ZooReaderWriter zk, ZooCache zooCache, ZooUtil.LockID lockID)
      throws KeeperException, InterruptedException {
    this(path, zk, zooCache, lockID, DEFAULT_MAX_DEFERRED, DEFAULT_FATE_ID_GENERATOR);
  }

  @VisibleForTesting
  public MetaFateStore(String path, ZooReaderWriter zk, ZooCache zooCache, ZooUtil.LockID lockID,
      int maxDeferred, FateIdGenerator fateIdGenerator)
      throws KeeperException, InterruptedException {
    super(lockID, zooCache, maxDeferred, fateIdGenerator);
    this.path = path;
    this.zk = zk;

    zk.putPersistentData(path, new byte[0], NodeExistsPolicy.SKIP);
  }

  /**
   * For testing only
   */
  MetaFateStore() {}

  @Override
  public FateId create() {
    while (true) {
      try {
        FateId fateId = FateId.from(fateInstanceType, UUID.randomUUID());
        zk.putPersistentData(getTXPath(fateId), new NodeValue(TStatus.NEW, null).serialize(),
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
      zk.putPersistentData(getTXPath(fateId), new NodeValue(TStatus.NEW, null, key).serialize(),
          NodeExistsPolicy.FAIL);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Optional<FateTxStore<T>> tryReserve(FateId fateId) {
    // return an empty option if the FateId doesn't exist
    if (_getStatus(fateId).equals(TStatus.UNKNOWN)) {
      return Optional.empty();
    }
    // uniquely identify this attempt to reserve the fate operation data
    FateReservation reservation = FateReservation.from(lockID, UUID.randomUUID());

    try {
      byte[] newSerNodeVal = zk.mutateExisting(getTXPath(fateId), currSerNodeVal -> {
        NodeValue currNodeVal = new NodeValue(currSerNodeVal);
        // The uuid handles the case where there was a ZK server fault and the write for this thread
        // went through but that was not acknowledged, and we are reading our own write for 2nd
        // time.
        if (!currNodeVal.isReserved() || (currNodeVal.isReserved()
            && currNodeVal.reservation.orElseThrow().equals(reservation))) {
          FateKey currFateKey = currNodeVal.fateKey.orElse(null);
          // Add the FateReservation to the node to reserve
          return new NodeValue(currNodeVal.status, reservation, currFateKey).serialize();
        } else {
          // This will not change the value to null but will return null
          return null;
        }
      });
      if (newSerNodeVal != null) {
        return Optional.of(new FateTxStoreImpl(fateId, reservation));
      } else {
        return Optional.empty();
      }
    } catch (InterruptedException | KeeperException | AcceptableThriftTableOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean isReserved(FateId fateId) {
    boolean isReserved;
    try {
      isReserved = getNode(fateId).isReserved();
    } catch (Exception e) {
      // Exception thrown, so node doesn't exist, so it is not reserved
      isReserved = false;
    }
    return isReserved;
  }

  @Override
  public Map<FateId,FateReservation> getActiveReservations() {
    Map<FateId,FateReservation> activeReservations = new HashMap<>();

    try {
      for (String strTxId : zk.getChildren(path)) {
        String txUUIDStr = strTxId.split("_")[1];
        FateId fateId = FateId.from(fateInstanceType, txUUIDStr);
        if (isReserved(fateId)) {
          FateReservation reservation = getNode(fateId).reservation.orElseThrow();
          activeReservations.put(fateId, reservation);
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    return activeReservations;
  }

  @Override
  public void deleteDeadReservations() {
    for (Map.Entry<FateId,FateReservation> entry : getActiveReservations().entrySet()) {
      FateId fateId = entry.getKey();
      FateReservation reservation = entry.getValue();
      try {
        zk.mutateExisting(getTXPath(fateId), currSerNodeVal -> {
          NodeValue currNodeVal = new NodeValue(currSerNodeVal);
          // Make sure the current node is still reserved and reserved with the expected reservation
          // and it is dead
          if (currNodeVal.isReserved() && currNodeVal.reservation.orElseThrow().equals(reservation)
              && isDeadReservation(currNodeVal.reservation.orElseThrow())) {
            // Delete the reservation
            return new NodeValue(currNodeVal.status, null, currNodeVal.fateKey.orElse(null))
                .serialize();
          } else {
            // No change
            return null;
          }
        });
      } catch (KeeperException | InterruptedException | AcceptableThriftTableOperationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  protected Pair<TStatus,Optional<FateKey>> getStatusAndKey(FateId fateId) {
    final NodeValue node = getNode(fateId);
    return new Pair<>(node.status, node.fateKey);
  }

  @Override
  public FateInstanceType type() {
    return fateInstanceType;
  }

  private class FateTxStoreImpl extends AbstractFateTxStoreImpl<T> {

    private FateTxStoreImpl(FateId fateId) {
      super(fateId);
    }

    private FateTxStoreImpl(FateId fateId, FateReservation reservation) {
      super(fateId, reservation);
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

      if (max.isEmpty()) {
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
        zk.mutateExisting(getTXPath(fateId), currSerializedData -> {
          NodeValue currNodeVal = new NodeValue(currSerializedData);
          FateReservation currFateReservation = currNodeVal.reservation.orElse(null);
          FateKey currFateKey = currNodeVal.fateKey.orElse(null);
          NodeValue newNodeValue = new NodeValue(status, currFateReservation, currFateKey);
          return newNodeValue.serialize();
        });
      } catch (KeeperException | InterruptedException | AcceptableThriftTableOperationException e) {
        throw new IllegalStateException(e);
      }

      observedStatus = status;
    }

    @Override
    public void delete() {
      verifyReserved(true);

      try {
        zk.recursiveDelete(getTXPath(fateId), NodeMissingPolicy.SKIP);
        this.deleted = true;
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

      return MetaFateStore.this.getTransactionInfo(txInfo, fateId);
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

    @Override
    protected void unreserve() {
      try {
        if (!this.deleted) {
          zk.mutateExisting(getTXPath(fateId), currSerNodeVal -> {
            NodeValue currNodeVal = new NodeValue(currSerNodeVal);
            FateKey currFateKey = currNodeVal.fateKey.orElse(null);
            if ((currNodeVal.isReserved()
                && currNodeVal.reservation.orElseThrow().equals(this.reservation))) {
              // Remove the FateReservation from the NodeValue to unreserve
              return new NodeValue(currNodeVal.status, null, currFateKey).serialize();
            } else {
              // possible this is running a 2nd time in zk server fault conditions and its first
              // write went through
              return null;
            }
          });
        }
        this.reservation = null;
      } catch (InterruptedException | KeeperException | AcceptableThriftTableOperationException e) {
        throw new IllegalStateException(e);
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
      return new NodeValue(TStatus.UNKNOWN, null);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected FateTxStore<T> newUnreservedFateTxStore(FateId fateId) {
    return new FateTxStoreImpl(fateId);
  }

  @Override
  protected Stream<FateIdStatus> getTransactions(Set<TStatus> statuses) {
    try {
      Stream<FateIdStatus> stream = zk.getChildren(path).stream().map(strTxid -> {
        String txUUIDStr = strTxid.split("_")[1];
        FateId fateId = FateId.from(fateInstanceType, txUUIDStr);
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

      if (!ALL_STATUSES.equals(statuses)) {
        stream = stream.filter(s -> statuses.contains(s.getStatus()));
      }

      return stream;
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Stream<FateKey> list(FateKey.FateKeyType type) {
    return getTransactions(ALL_STATUSES).flatMap(fis -> getKey(fis.getFateId()).stream())
        .filter(fateKey -> fateKey.getType() == type);
  }

  protected static class NodeValue {
    final TStatus status;
    final Optional<FateKey> fateKey;
    final Optional<FateReservation> reservation;

    private NodeValue(byte[] serializedData) {
      try (DataInputBuffer buffer = new DataInputBuffer()) {
        buffer.reset(serializedData, serializedData.length);
        this.status = TStatus.valueOf(buffer.readUTF());
        this.reservation = deserializeFateReservation(buffer);
        this.fateKey = deserializeFateKey(buffer);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private NodeValue(TStatus status, FateReservation reservation) {
      this(status, reservation, null);
    }

    private NodeValue(TStatus status, FateReservation reservation, FateKey fateKey) {
      this.status = Objects.requireNonNull(status);
      this.reservation = Optional.ofNullable(reservation);
      this.fateKey = Optional.ofNullable(fateKey);
    }

    private Optional<FateKey> deserializeFateKey(DataInputBuffer buffer) throws IOException {
      int length = buffer.readInt();
      if (length > 0) {
        return Optional.of(FateKey.deserialize(buffer.readNBytes(length)));
      }
      return Optional.empty();
    }

    private Optional<FateReservation> deserializeFateReservation(DataInputBuffer buffer)
        throws IOException {
      int length = buffer.readInt();
      if (length > 0) {
        return Optional.of(FateReservation.deserialize(buffer.readNBytes(length)));
      }
      return Optional.empty();
    }

    byte[] serialize() {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(baos)) {
        dos.writeUTF(status.name());
        if (isReserved()) {
          byte[] serializedFateReservation = reservation.orElseThrow().getSerialized();
          dos.writeInt(serializedFateReservation.length);
          dos.write(serializedFateReservation);
        } else {
          dos.writeInt(0);
        }
        if (fateKey.isPresent()) {
          byte[] serializedFateKey = fateKey.orElseThrow().getSerialized();
          dos.writeInt(serializedFateKey.length);
          dos.write(serializedFateKey);
        } else {
          dos.writeInt(0);
        }
        dos.close();
        return baos.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    public boolean isReserved() {
      return reservation.isPresent();
    }
  }
}
