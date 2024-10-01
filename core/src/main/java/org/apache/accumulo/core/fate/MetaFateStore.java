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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;

//TODO use zoocache? - ACCUMULO-1297
//TODO handle zookeeper being down gracefully - ACCUMULO-1297
public class MetaFateStore<T> extends AbstractFateStore<T> {

  private static final Logger log = LoggerFactory.getLogger(MetaFateStore.class);
  private static final FateInstanceType fateInstanceType = FateInstanceType.META;
  private String path;
  private ZooReaderWriter zk;

  private String getTXPath(FateId fateId) {
    return path + "/tx_" + fateId.getTxUUIDStr();
  }

  public MetaFateStore(String path, ZooReaderWriter zk, ZooUtil.LockID lockID,
      Predicate<ZooUtil.LockID> isLockHeld) throws KeeperException, InterruptedException {
    this(path, zk, lockID, isLockHeld, DEFAULT_MAX_DEFERRED, DEFAULT_FATE_ID_GENERATOR);
  }

  @VisibleForTesting
  public MetaFateStore(String path, ZooReaderWriter zk, ZooUtil.LockID lockID,
      Predicate<ZooUtil.LockID> isLockHeld, int maxDeferred, FateIdGenerator fateIdGenerator)
      throws KeeperException, InterruptedException {
    super(lockID, isLockHeld, maxDeferred, fateIdGenerator);
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
  public Optional<FateTxStore<T>> createAndReserve(FateKey fateKey) {
    final var reservation = FateReservation.from(lockID, UUID.randomUUID());
    final var fateId = fateIdGenerator.fromTypeAndKey(type(), fateKey);

    try {
      byte[] nodeVal = zk.mutateOrCreate(getTXPath(fateId),
          new NodeValue(TStatus.NEW, reservation, fateKey).serialize(), currSerNodeVal -> {
            // We are only returning a non-null value for the following cases:
            // 1) The existing NodeValue for fateId is exactly the same as the value set for the
            // node if it doesn't yet exist:
            // TStatus = TStatus.NEW, FateReservation = reservation, FateKey = fateKey
            // This might occur if there was a ZK server fault and the same write is running a 2nd
            // time
            // 2) The existing NodeValue for fateId has:
            // TStatus = TStatus.NEW, no FateReservation present, FateKey = fateKey
            // The fateId is NEW/unseeded and not reserved, so we can allow it to be reserved
            // Note: returning null here will not change the value to null but will return null
            NodeValue currNodeVal = new NodeValue(currSerNodeVal);
            if (currNodeVal.status == TStatus.NEW) {
              verifyFateKey(fateId, currNodeVal.fateKey, fateKey);
              if (currNodeVal.isReservedBy(reservation)) {
                return currSerNodeVal;
              } else if (!currNodeVal.isReserved()) {
                // NEW/unseeded transaction and not reserved, so we can allow it to be reserved
                return new NodeValue(TStatus.NEW, reservation, fateKey).serialize();
              } else {
                // NEW/unseeded transaction reserved under a different reservation
                return null;
              }
            } else {
              log.trace(
                  "fate id {} tstatus {} fate key {} is reserved {} "
                      + "has already been seeded with work (non-NEW status)",
                  fateId, currNodeVal.status, currNodeVal.fateKey.orElse(null),
                  currNodeVal.isReserved());
              return null;
            }
          });
      if (nodeVal != null) {
        return Optional.of(new FateTxStoreImpl(fateId, reservation));
      } else {
        return Optional.empty();
      }
    } catch (InterruptedException | KeeperException | AcceptableThriftTableOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Optional<FateTxStore<T>> tryReserve(FateId fateId) {
    // uniquely identify this attempt to reserve the fate operation data
    FateReservation reservation = FateReservation.from(lockID, UUID.randomUUID());

    try {
      byte[] newSerNodeVal = zk.mutateExisting(getTXPath(fateId), currSerNodeVal -> {
        NodeValue currNodeVal = new NodeValue(currSerNodeVal);
        // The uuid handles the case where there was a ZK server fault and the write for this thread
        // went through but that was not acknowledged, and we are reading our own write for 2nd
        // time.
        if (!currNodeVal.isReserved() || currNodeVal.isReservedBy(reservation)) {
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
    } catch (KeeperException.NoNodeException e) {
      log.trace("Tried to reserve a transaction {} that does not exist", fateId);
      return Optional.empty();
    } catch (InterruptedException | KeeperException | AcceptableThriftTableOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void deleteDeadReservations() {
    for (Map.Entry<FateId,FateReservation> entry : getActiveReservations().entrySet()) {
      FateId fateId = entry.getKey();
      FateReservation reservation = entry.getValue();
      if (isLockHeld.test(reservation.getLockID())) {
        continue;
      }
      try {
        zk.mutateExisting(getTXPath(fateId), currSerNodeVal -> {
          NodeValue currNodeVal = new NodeValue(currSerNodeVal);
          // Make sure the current node is still reserved and reserved with the expected reservation
          // and it is dead
          if (currNodeVal.isReservedBy(reservation)
              && !isLockHeld.test(currNodeVal.reservation.orElseThrow().getLockID())) {
            // Delete the reservation
            log.trace("Deleted the dead reservation {} for fate id {}", reservation, fateId);
            return new NodeValue(currNodeVal.status, null, currNodeVal.fateKey.orElse(null))
                .serialize();
          } else {
            // No change
            return null;
          }
        });
      } catch (KeeperException.NoNodeException e) {
        // the node has since been deleted. Can safely ignore
      } catch (KeeperException | InterruptedException | AcceptableThriftTableOperationException e) {
        throw new RuntimeException(e);
      }
    }
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
          String top = findTop(txpath);
          if (top == null) {
            return null;
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
      List<String> ops;
      try {
        ops = zk.getChildren(txpath);
      } catch (NoNodeException e) {
        return null;
      }

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
          // Ensure the FateId is reserved in ZK, and it is reserved with the expected reservation
          if (currNodeVal.isReservedBy(this.reservation)) {
            FateReservation currFateReservation = currNodeVal.reservation.orElseThrow();
            FateKey currFateKey = currNodeVal.fateKey.orElse(null);
            NodeValue newNodeValue = new NodeValue(status, currFateReservation, currFateKey);
            return newNodeValue.serialize();
          } else {
            throw new IllegalStateException("Either the FateId " + fateId
                + " is not reserved in ZK, or it is but the reservation in ZK: "
                + currNodeVal.reservation.orElse(null) + " differs from that in the store: "
                + this.reservation);
          }
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
            if (currNodeVal.isReservedBy(this.reservation)) {
              // Remove the FateReservation from the NodeValue to unreserve
              return new NodeValue(currNodeVal.status, null, currFateKey).serialize();
            } else {
              // possible this is running a 2nd time in zk server fault conditions and its first
              // write went through
              if (!currNodeVal.isReserved()) {
                log.trace("The FATE reservation for fate id {} does not exist in ZK", fateId);
              } else if (!currNodeVal.reservation.orElseThrow().equals(this.reservation)) {
                log.debug(
                    "The FATE reservation for fate id {} in ZK differs from that in the store",
                    fateId);
              }
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
  protected Stream<FateIdStatus> getTransactions(EnumSet<TStatus> statuses) {
    try {
      Stream<FateIdStatus> stream = zk.getChildren(path).stream().map(strTxid -> {
        String txUUIDStr = strTxid.split("_")[1];
        FateId fateId = FateId.from(fateInstanceType, txUUIDStr);
        // Memoizing for two reasons. First the status or reservation may never be requested, so
        // in that case avoid the lookup. Second, if it's requested multiple times the result will
        // always be consistent.
        Supplier<NodeValue> nodeSupplier = Suppliers.memoize(() -> getNode(fateId));
        return new FateIdStatusBase(fateId) {
          @Override
          public TStatus getStatus() {
            return nodeSupplier.get().status;
          }

          @Override
          public Optional<FateReservation> getFateReservation() {
            return nodeSupplier.get().reservation;
          }
        };
      });

      if (statuses.equals(EnumSet.allOf(TStatus.class))) {
        return stream;
      }
      return stream.filter(s -> statuses.contains(s.getStatus()));
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Stream<FateKey> list(FateKey.FateKeyType type) {
    return getTransactions(EnumSet.allOf(TStatus.class))
        .flatMap(fis -> getKey(fis.getFateId()).stream())
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
      Preconditions.checkArgument(length >= 0);
      if (length > 0) {
        return Optional.of(FateKey.deserialize(buffer.readNBytes(length)));
      }
      return Optional.empty();
    }

    private Optional<FateReservation> deserializeFateReservation(DataInputBuffer buffer)
        throws IOException {
      int length = buffer.readInt();
      Preconditions.checkArgument(length >= 0);
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

    public boolean isReservedBy(FateReservation reservation) {
      return isReserved() && this.reservation.orElseThrow().equals(reservation);
    }
  }
}
