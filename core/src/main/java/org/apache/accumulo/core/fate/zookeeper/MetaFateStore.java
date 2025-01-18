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
package org.apache.accumulo.core.fate.zookeeper;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.NEW;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUBMITTED;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.ReadOnlyRepo;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.StackOverflowException;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooSession;
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
  private ZooSession zk;
  private ZooReaderWriter zrw;

  private String getTXPath(FateId fateId) {
    return path + "/tx_" + fateId.getTxUUIDStr();
  }

  /**
   * Constructs a MetaFateStore
   *
   * @param path the path in ZK where the fate data will reside
   * @param zk the {@link ZooSession}
   * @param lockID the {@link ZooUtil.LockID} held by the process creating this store. Should be
   *        null if this store will be used as read-only (will not be used to reserve transactions)
   * @param isLockHeld the {@link Predicate} used to determine if the lockID is held or not at the
   *        time of invocation. If the store is used for a {@link Fate} which runs a dead
   *        reservation cleaner, this should be non-null, otherwise null is fine
   */
  public MetaFateStore(String path, ZooSession zk, ZooUtil.LockID lockID,
      Predicate<ZooUtil.LockID> isLockHeld) throws KeeperException, InterruptedException {
    this(path, zk, lockID, isLockHeld, DEFAULT_MAX_DEFERRED, DEFAULT_FATE_ID_GENERATOR);
  }

  @VisibleForTesting
  public MetaFateStore(String path, ZooSession zk, ZooUtil.LockID lockID,
      Predicate<ZooUtil.LockID> isLockHeld, int maxDeferred, FateIdGenerator fateIdGenerator)
      throws KeeperException, InterruptedException {
    super(lockID, isLockHeld, maxDeferred, fateIdGenerator);
    this.path = path;
    this.zk = zk;
    this.zrw = zk.asReaderWriter();

    this.zrw.putPersistentData(path, new byte[0], NodeExistsPolicy.SKIP);
  }

  @Override
  public FateId create() {
    while (true) {
      try {
        FateId fateId = fateIdGenerator.newRandomId(fateInstanceType);
        zrw.putPersistentData(getTXPath(fateId),
            new FateData<T>(TStatus.NEW, null, null, createEmptyRepoDeque(), createEmptyTxInfo())
                .serialize(),
            NodeExistsPolicy.FAIL);
        return fateId;
      } catch (NodeExistsException nee) {
        // exist, so just try another random #
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private Optional<FateTxStore<T>> createAndReserve(FateKey fateKey) {
    final var fateId = fateIdGenerator.fromTypeAndKey(type(), fateKey);
    verifyLock(lockID, fateId);
    final var reservation = FateReservation.from(lockID, UUID.randomUUID());

    try {
      byte[] newSerFateData =
          zrw.mutateOrCreate(getTXPath(fateId), new FateData<>(TStatus.NEW, reservation, fateKey,
              createEmptyRepoDeque(), createEmptyTxInfo()).serialize(), currSerFateData -> {
                // We are only returning a non-null value for the following cases:
                // 1) The existing node for fateId is exactly the same as the value set for the
                // node if it doesn't yet exist:
                // TStatus = TStatus.NEW, FateReservation = reservation, FateKey = fateKey
                // This might occur if there was a ZK server fault and the same write is running a
                // 2nd time
                // 2) The existing node for fateId has:
                // TStatus = TStatus.NEW, no FateReservation present, FateKey = fateKey
                // The fateId is NEW/unseeded and not reserved, so we can allow it to be reserved
                FateData<T> currFateData = new FateData<>(currSerFateData);
                if (currFateData.status == TStatus.NEW) {
                  verifyFateKey(fateId, currFateData.fateKey, fateKey);
                  if (currFateData.isReservedBy(reservation)) {
                    return currSerFateData;
                  } else if (!currFateData.isReserved()) {
                    // NEW/unseeded transaction and not reserved, so we can allow it to be reserved
                    return new FateData<>(TStatus.NEW, reservation, fateKey, createEmptyRepoDeque(),
                        createEmptyTxInfo()).serialize();
                  } else {
                    // NEW/unseeded transaction reserved under a different reservation
                    // This will not change the value and will return null
                    return null;
                  }
                } else {
                  log.trace(
                      "fate id {} tstatus {} fate key {} is reserved {} "
                          + "has already been seeded with work (non-NEW status)",
                      fateId, currFateData.status, currFateData.fateKey.orElse(null),
                      currFateData.isReserved());
                  return null;
                }
              });
      if (newSerFateData != null) {
        return Optional.of(new FateTxStoreImpl(fateId, reservation));
      } else {
        return Optional.empty();
      }
    } catch (InterruptedException | KeeperException | AcceptableThriftTableOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Optional<FateId> seedTransaction(Fate.FateOperation fateOp, FateKey fateKey, Repo<T> repo,
      boolean autoCleanUp) {
    return createAndReserve(fateKey).map(txStore -> {
      try {
        seedTransaction(fateOp, repo, autoCleanUp, txStore);
        return txStore.getID();
      } finally {
        txStore.unreserve(Duration.ZERO);
      }
    });
  }

  @Override
  public boolean seedTransaction(Fate.FateOperation fateOp, FateId fateId, Repo<T> repo,
      boolean autoCleanUp) {
    return tryReserve(fateId).map(txStore -> {
      try {
        if (txStore.getStatus() == NEW) {
          seedTransaction(fateOp, repo, autoCleanUp, txStore);
          return true;
        }
        return false;
      } finally {
        txStore.unreserve(Duration.ZERO);
      }
    }).orElse(false);
  }

  private void seedTransaction(Fate.FateOperation fateOp, Repo<T> repo, boolean autoCleanUp,
      FateTxStore<T> txStore) {
    if (txStore.top() == null) {
      try {
        txStore.push(repo);
      } catch (StackOverflowException e) {
        // this should not happen
        throw new IllegalStateException(e);
      }
    }

    if (autoCleanUp) {
      txStore.setTransactionInfo(TxInfo.AUTO_CLEAN, autoCleanUp);
    }
    txStore.setTransactionInfo(TxInfo.TX_NAME, fateOp);
    txStore.setStatus(SUBMITTED);
  }

  @Override
  public Optional<FateTxStore<T>> tryReserve(FateId fateId) {
    verifyLock(lockID, fateId);
    // uniquely identify this attempt to reserve the fate operation data
    FateReservation reservation = FateReservation.from(lockID, UUID.randomUUID());

    UnaryOperator<FateData<T>> fateDataOp = currFateData -> {
      // The uuid handles the case where there was a ZK server fault and the write for this thread
      // went through but that was not acknowledged, and we are reading our own write for 2nd
      // time.
      if (!currFateData.isReserved() || currFateData.isReservedBy(reservation)) {
        // Add the FateReservation to the node to reserve
        return new FateData<>(currFateData.status, reservation, currFateData.fateKey.orElse(null),
            currFateData.repoDeque, currFateData.txInfo);
      } else {
        // This will not change the value and will return null
        return null;
      }
    };

    byte[] newSerFateData;
    try {
      newSerFateData = mutate(fateId, fateDataOp);
    } catch (KeeperException.NoNodeException nne) {
      log.trace("Tried to reserve a transaction {} that does not exist", fateId);
      return Optional.empty();
    } catch (KeeperException e) {
      throw new IllegalStateException(e);
    }

    if (newSerFateData != null) {
      return Optional.of(new FateTxStoreImpl(fateId, reservation));
    } else {
      return Optional.empty();
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

      UnaryOperator<FateData<T>> fateDataOp = currFateData -> {
        // Make sure the current node is still reserved and reserved with the expected reservation
        // and it is dead
        if (currFateData.isReservedBy(reservation)
            && !isLockHeld.test(currFateData.reservation.orElseThrow().getLockID())) {
          // Delete the reservation
          log.trace("Deleted the dead reservation {} for fate id {}", reservation, fateId);
          return new FateData<>(currFateData.status, null, currFateData.fateKey.orElse(null),
              currFateData.repoDeque, currFateData.txInfo);
        } else {
          // This will not change the value and will return null
          return null;
        }
      };

      try {
        mutate(fateId, fateDataOp);
      } catch (KeeperException.NoNodeException nne) {
        // the node has since been deleted. Can safely ignore
      } catch (KeeperException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public FateInstanceType type() {
    return fateInstanceType;
  }

  private class FateTxStoreImpl extends AbstractFateTxStoreImpl {

    private FateTxStoreImpl(FateId fateId) {
      super(fateId);
    }

    private FateTxStoreImpl(FateId fateId, FateReservation reservation) {
      super(fateId, reservation);
    }

    private static final int RETRIES = 10;

    @Override
    public Repo<T> top() {
      verifyReservedAndNotDeleted(false);
      String txpath = getTXPath(fateId);

      for (int i = 0; i < RETRIES; i++) {
        FateData<T> fateData = getFateData(fateId);

        if (fateData.status == TStatus.UNKNOWN) {
          log.debug("zookeeper error reading fate data for {} at {}", fateId, txpath);
          sleepUninterruptibly(100, MILLISECONDS);
          continue;
        }

        var repoDeque = fateData.repoDeque;
        if (repoDeque.isEmpty()) {
          return null;
        } else {
          return repoDeque.peek();
        }
      }
      return null;
    }

    @Override
    public void push(Repo<T> repo) throws StackOverflowException {
      verifyReservedAndNotDeleted(true);

      UnaryOperator<FateData<T>> fateDataOp = currFateData -> {
        Preconditions.checkState(REQ_PUSH_STATUS.contains(currFateData.status),
            "Tried to push to the repo stack for %s when the transaction status is %s", fateId,
            currFateData.status);
        var repoDeque = currFateData.repoDeque;
        if (repoDeque.size() >= MAX_REPOS) {
          throw new StackOverflowException("Repo stack size too large");
        }
        repoDeque.push(repo);
        return currFateData;
      };

      try {
        mutate(fateId, fateDataOp);
      } catch (KeeperException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void pop() {
      verifyReservedAndNotDeleted(true);

      UnaryOperator<FateData<T>> fateDataOp = currFateData -> {
        Preconditions.checkState(REQ_POP_STATUS.contains(currFateData.status),
            "Tried to pop from the repo stack for %s when the transaction status is %s", fateId,
            currFateData.status);
        var repoDeque = currFateData.repoDeque;

        if (repoDeque.isEmpty()) {
          throw new IllegalStateException("Tried to pop when empty " + fateId);
        } else {
          repoDeque.pop();
        }

        return currFateData;
      };

      try {
        mutate(fateId, fateDataOp);
      } catch (KeeperException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void setStatus(TStatus status) {
      verifyReservedAndNotDeleted(true);

      UnaryOperator<FateData<T>> fateDataOp = currFateData -> {
        // Ensure the FateId is reserved in ZK, and it is reserved with the expected reservation
        if (currFateData.isReservedBy(this.reservation)) {
          return new FateData<>(status, currFateData.reservation.orElseThrow(),
              currFateData.fateKey.orElse(null), currFateData.repoDeque, currFateData.txInfo);
        } else {
          throw new IllegalStateException("Either the FateId " + fateId
              + " is not reserved in ZK, or it is but the reservation in ZK: "
              + currFateData.reservation.orElse(null) + " differs from that in the store: "
              + this.reservation);
        }
      };

      try {
        mutate(fateId, fateDataOp);
      } catch (KeeperException e) {
        throw new IllegalStateException(e);
      }

      observedStatus = status;
    }

    @Override
    public void delete() {
      _delete(REQ_DELETE_STATUS);
    }

    @Override
    public void forceDelete() {
      _delete(REQ_FORCE_DELETE_STATUS);
    }

    private void _delete(Set<TStatus> requiredStatus) {
      verifyReservedAndNotDeleted(true);

      // atomically check the txn status and delete the node
      // retry until we either atomically delete the node or the txn status is disallowed
      while (!this.deleted) {
        Stat stat = new Stat();
        FateData<T> fateData = getFateData(fateId, stat);
        Preconditions.checkState(requiredStatus.contains(fateData.status),
            "Tried to delete fate data for %s when the transaction status is %s", fateId,
            fateData.status);
        try {
          zrw.deleteStrict(getTXPath(fateId), stat.getVersion());
          this.deleted = true;
        } catch (KeeperException.BadVersionException e) {
          log.trace(
              "Deletion of ZK node fate data for {} was not able to be completed atomically... Retrying",
              fateId);
        } catch (InterruptedException | KeeperException e) {
          throw new IllegalStateException(e);
        }
      }
    }

    @Override
    public void setTransactionInfo(Fate.TxInfo txInfo, Serializable so) {
      verifyReservedAndNotDeleted(true);

      UnaryOperator<FateData<T>> fateDataOp = currFateData -> {
        currFateData.txInfo.put(txInfo, so);
        return currFateData;
      };

      try {
        mutate(fateId, fateDataOp);
      } catch (KeeperException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public Serializable getTransactionInfo(Fate.TxInfo txInfo) {
      verifyReservedAndNotDeleted(false);

      return MetaFateStore.this.getTransactionInfo(txInfo, fateId);
    }

    @Override
    public long timeCreated() {
      verifyReservedAndNotDeleted(false);

      try {
        Stat stat = zk.exists(getTXPath(fateId), null);
        return stat.getCtime();
      } catch (Exception e) {
        return 0;
      }
    }

    @Override
    public List<ReadOnlyRepo<T>> getStack() {
      verifyReservedAndNotDeleted(false);

      FateData<T> fateData = getFateData(fateId);
      return new ArrayList<>(fateData.repoDeque);
    }

    @Override
    protected void unreserve() {
      UnaryOperator<FateData<T>> fateDataOp = currFateData -> {
        if (currFateData.isReservedBy(this.reservation)) {
          // Remove the FateReservation from the node to unreserve
          return new FateData<>(currFateData.status, null, currFateData.fateKey.orElse(null),
              currFateData.repoDeque, currFateData.txInfo);
        } else {
          // possible this is running a 2nd time in zk server fault conditions and its first
          // write went through
          if (!currFateData.isReserved()) {
            log.trace("The FATE reservation for fate id {} does not exist in ZK", fateId);
          } else if (!currFateData.reservation.orElseThrow().equals(this.reservation)) {
            log.debug("The FATE reservation for fate id {} in ZK differs from that in the store",
                fateId);
          }
          // This will not change the value and will return null
          return null;
        }
      };

      if (!this.deleted) {
        try {
          mutate(fateId, fateDataOp);
        } catch (KeeperException e) {
          throw new IllegalStateException(e);
        }
      }
      this.reservation = null;
    }
  }

  private Serializable getTransactionInfo(TxInfo txInfo, FateId fateId) {
    return getFateData(fateId).txInfo.get(txInfo);
  }

  @Override
  protected TStatus _getStatus(FateId fateId) {
    return getFateData(fateId).status;
  }

  @Override
  protected Optional<FateKey> getKey(FateId fateId) {
    return getFateData(fateId).fateKey;
  }

  private FateData<T> getFateData(FateId fateId) {
    try {
      return new FateData<>(zrw.getData(getTXPath(fateId)));
    } catch (NoNodeException nne) {
      return new FateData<>(TStatus.UNKNOWN, null, null, createEmptyRepoDeque(),
          createEmptyTxInfo());
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  private FateData<T> getFateData(FateId fateId, Stat stat) {
    try {
      return new FateData<>(zrw.getData(getTXPath(fateId), stat));
    } catch (NoNodeException nne) {
      return new FateData<>(TStatus.UNKNOWN, null, null, createEmptyRepoDeque(),
          createEmptyTxInfo());
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
      Stream<FateIdStatus> stream = zrw.getChildren(path).stream().map(strTxid -> {
        String txUUIDStr = strTxid.split("_")[1];
        FateId fateId = FateId.from(fateInstanceType, txUUIDStr);
        // Memoizing for two reasons. First the status or reservation may never be requested, so
        // in that case avoid the lookup. Second, if it's requested multiple times the result will
        // always be consistent.
        Supplier<FateData<T>> nodeSupplier = Suppliers.memoize(() -> getFateData(fateId));
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

  private Deque<Repo<T>> createEmptyRepoDeque() {
    return new ArrayDeque<>();
  }

  private Map<TxInfo,Serializable> createEmptyTxInfo() {
    return new EnumMap<>(TxInfo.class);
  }

  /**
   * Mutate the existing FateData for the given fateId using the given operator.
   *
   * @param fateId the fateId for the FateData to change
   * @param fateDataOp the operation to apply to the existing FateData. Op should return null if no
   *        change is desired. Otherwise, should return the new FateData with the desired changes
   * @return the resulting serialized FateData or null if the op resulted in no change
   */
  private byte[] mutate(FateId fateId, UnaryOperator<FateData<T>> fateDataOp)
      throws KeeperException {
    try {
      return zrw.mutateExisting(getTXPath(fateId), currSerFateData -> {
        FateData<T> currFateData = new FateData<>(currSerFateData);
        FateData<T> newFateData = fateDataOp.apply(currFateData);
        if (newFateData == null) {
          // This will not change the value and will return null
          return null;
        } else {
          return newFateData.serialize();
        }
      });
    } catch (InterruptedException | AcceptableThriftTableOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  protected static class FateData<T> {
    final TStatus status;
    final Optional<FateKey> fateKey;
    final Optional<FateReservation> reservation;
    final Deque<Repo<T>> repoDeque;
    final Map<TxInfo,Serializable> txInfo;

    /**
     * Construct a FateData from a previously {@link #serialize()}ed FateData
     *
     * @param serializedData the serialized data
     */
    private FateData(byte[] serializedData) {
      try (DataInputBuffer buffer = new DataInputBuffer()) {
        buffer.reset(serializedData, serializedData.length);
        this.status = TStatus.valueOf(buffer.readUTF());
        this.reservation = deserializeFateReservation(buffer);
        this.fateKey = deserializeFateKey(buffer);
        this.repoDeque = deserializeRepoDeque(buffer);
        this.txInfo = deserializeTxInfo(buffer);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private FateData(TStatus status, FateReservation reservation, FateKey fateKey,
        Deque<Repo<T>> repoDeque, Map<TxInfo,Serializable> txInfo) {
      this.status = Objects.requireNonNull(status);
      this.reservation = Optional.ofNullable(reservation);
      this.fateKey = Optional.ofNullable(fateKey);
      this.repoDeque = Objects.requireNonNull(repoDeque);
      this.txInfo = Objects.requireNonNull(txInfo);
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

    private Deque<Repo<T>> deserializeRepoDeque(DataInputBuffer buffer) throws IOException {
      Deque<Repo<T>> deque = new ArrayDeque<>();
      int numRepos = buffer.readInt();

      for (int i = 0; i < numRepos; i++) {
        int length = buffer.readInt();
        Preconditions.checkArgument(length > 0);
        @SuppressWarnings("unchecked")
        var repo = (Repo<T>) deserialize(buffer.readNBytes(length));
        deque.add(repo);
      }

      return deque;
    }

    private Map<TxInfo,Serializable> deserializeTxInfo(DataInputBuffer buffer) throws IOException {
      Map<TxInfo,Serializable> txInfo = new EnumMap<>(TxInfo.class);
      int length = buffer.readInt();

      while (length != 0) {
        Preconditions.checkArgument(length >= 0);
        TxInfo type = TxInfo.values()[buffer.readInt()];
        txInfo.put(type, AbstractFateStore.deserializeTxInfo(type, buffer.readNBytes(length - 1)));

        // if we have reached the end of the buffer (= reached the end of the tx info data)
        if (buffer.getPosition() == buffer.getLength()) {
          break;
        }
        length = buffer.readInt();
      }

      return txInfo;
    }

    private byte[] serialize() {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(baos)) {
        // status
        dos.writeUTF(status.name());
        // reservation
        if (isReserved()) {
          byte[] serializedFateReservation = reservation.orElseThrow().getSerialized();
          dos.writeInt(serializedFateReservation.length);
          dos.write(serializedFateReservation);
        } else {
          dos.writeInt(0);
        }
        // fate key
        if (fateKey.isPresent()) {
          byte[] serializedFateKey = fateKey.orElseThrow().getSerialized();
          dos.writeInt(serializedFateKey.length);
          dos.write(serializedFateKey);
        } else {
          dos.writeInt(0);
        }
        // repo deque
        byte[] serializedRepo;
        dos.writeInt(repoDeque.size());
        // iterates from top/first/head to bottom/last/tail
        for (Repo<T> repo : repoDeque) {
          serializedRepo = AbstractFateStore.serialize(repo);
          dos.writeInt(serializedRepo.length);
          dos.write(serializedRepo);
        }
        // tx info
        if (!txInfo.isEmpty()) {
          for (var elt : txInfo.entrySet()) {
            byte[] serTxInfo = serializeTxInfo(elt.getValue());
            dos.writeInt(1 + serTxInfo.length);
            dos.writeInt(elt.getKey().ordinal());
            dos.write(serTxInfo);
          }
        } else {
          dos.writeInt(0);
        }
        // done
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
