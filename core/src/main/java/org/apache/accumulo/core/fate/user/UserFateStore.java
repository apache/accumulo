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
package org.apache.accumulo.core.fate.user;

import java.io.IOException;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.Fate.FateOperation;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.FateKey.FateKeyType;
import org.apache.accumulo.core.fate.ReadOnlyRepo;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.StackOverflowException;
import org.apache.accumulo.core.fate.user.FateMutator.Status;
import org.apache.accumulo.core.fate.user.schema.FateSchema.RepoColumnFamily;
import org.apache.accumulo.core.fate.user.schema.FateSchema.TxColumnFamily;
import org.apache.accumulo.core.fate.user.schema.FateSchema.TxInfoColumnFamily;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class UserFateStore<T> extends AbstractFateStore<T> {

  private static final Logger log = LoggerFactory.getLogger(UserFateStore.class);

  private final ClientContext context;
  private final String tableName;

  private static final FateInstanceType fateInstanceType = FateInstanceType.USER;
  private static final com.google.common.collect.Range<Integer> REPO_RANGE =
      com.google.common.collect.Range.closed(1, MAX_REPOS);

  /**
   * Constructs a UserFateStore
   *
   * @param context the {@link ClientContext}
   * @param tableName the name of the table which will store the Fate data
   * @param lockID the {@link org.apache.accumulo.core.fate.zookeeper.ZooUtil.LockID} held by the
   *        process creating this store. Should be null if this store will be used as read-only
   *        (will not be used to reserve transactions)
   * @param isLockHeld the {@link Predicate} used to determine if the lockID is held or not at the
   *        time of invocation. If the store is used for a {@link Fate} which runs a dead
   *        reservation cleaner, this should be non-null, otherwise null is fine
   */
  public UserFateStore(ClientContext context, String tableName, ZooUtil.LockID lockID,
      Predicate<ZooUtil.LockID> isLockHeld) {
    this(context, tableName, lockID, isLockHeld, DEFAULT_MAX_DEFERRED, DEFAULT_FATE_ID_GENERATOR);
  }

  @VisibleForTesting
  public UserFateStore(ClientContext context, String tableName, ZooUtil.LockID lockID,
      Predicate<ZooUtil.LockID> isLockHeld, int maxDeferred, FateIdGenerator fateIdGenerator) {
    super(lockID, isLockHeld, maxDeferred, fateIdGenerator);
    this.context = Objects.requireNonNull(context);
    this.tableName = Objects.requireNonNull(tableName);
  }

  @Override
  public FateId create() {

    int attempt = 0;
    while (true) {

      FateId fateId = getFateId();

      if (attempt >= 1) {
        log.debug("Failed to create new id: {}, trying again", fateId);
        UtilWaitThread.sleep(100);
      }

      var status = newMutator(fateId).requireAbsent().putStatus(TStatus.NEW)
          .putCreateTime(System.currentTimeMillis()).tryMutate();

      switch (status) {
        case ACCEPTED:
          return fateId;
        case UNKNOWN:
        case REJECTED:
          attempt++;
          continue;
        default:
          throw new IllegalStateException("Unknown status " + status);
      }
    }
  }

  public FateId getFateId() {
    return fateIdGenerator.newRandomId(type());
  }

  @Override
  public Seeder<T> beginSeeding() {
    return new BatchSeeder();
  }

  private FateMutator<T> seedTransaction(Fate.FateOperation fateOp, FateKey fateKey, FateId fateId,
      Repo<T> repo, boolean autoCleanUp) {
    Supplier<FateMutator<T>> mutatorFactory = () -> newMutator(fateId).requireAbsent()
        .putKey(fateKey).putCreateTime(System.currentTimeMillis());
    return buildMutator(mutatorFactory, fateOp, repo, autoCleanUp);
  }

  @Override
  public boolean seedTransaction(Fate.FateOperation fateOp, FateId fateId, Repo<T> repo,
      boolean autoCleanUp) {
    Supplier<FateMutator<T>> mutatorFactory =
        () -> newMutator(fateId).requireStatus(TStatus.NEW).requireUnreserved().requireAbsentKey();
    return seedTransaction(mutatorFactory, fateId.canonical(), fateOp, repo, autoCleanUp);
  }

  private FateMutator<T> buildMutator(Supplier<FateMutator<T>> mutatorFactory,
      Fate.FateOperation fateOp, Repo<T> repo, boolean autoCleanUp) {
    var mutator = mutatorFactory.get();
    mutator =
        mutator.putFateOp(serializeTxInfo(fateOp)).putRepo(1, repo).putStatus(TStatus.SUBMITTED);
    if (autoCleanUp) {
      mutator = mutator.putAutoClean(serializeTxInfo(autoCleanUp));
    }
    return mutator;
  }

  private boolean seedTransaction(Supplier<FateMutator<T>> mutatorFactory, String logId,
      Fate.FateOperation fateOp, Repo<T> repo, boolean autoCleanUp) {
    var mutator = buildMutator(mutatorFactory, fateOp, repo, autoCleanUp);
    int maxAttempts = 5;
    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      var status = mutator.tryMutate();
      if (status == FateMutator.Status.ACCEPTED) {
        // signal to the super class that a new fate transaction was seeded and is ready to run
        seededTx();
        log.trace("Attempt to seed {} returned {}", logId, status);
        return true;
      } else if (status == FateMutator.Status.REJECTED) {
        log.debug("Attempt to seed {} returned {}", logId, status);
        return false;
      } else if (status == FateMutator.Status.UNKNOWN) {
        // At this point can not reliably determine if the conditional mutation was successful or
        // not because no reservation was acquired. For example since no reservation was acquired it
        // is possible that seeding was a success and something immediately picked it up and started
        // operating on it and changing it. If scanning after that point can not conclude success or
        // failure. Another situation is that maybe the fateId already existed in a seeded form
        // prior to getting this unknown.
        log.debug("Attempt to seed {} returned {} status, retrying", logId, status);
        UtilWaitThread.sleep(250);
      }
    }

    log.warn("Repeatedly received unknown status when attempting to seed {}", logId);
    return false;
  }

  @Override
  public Optional<FateTxStore<T>> tryReserve(FateId fateId) {
    verifyLock(lockID, fateId);
    // Create a unique FateReservation for this reservation attempt
    FateReservation reservation = FateReservation.from(lockID, UUID.randomUUID());

    // requiring any status prevents creating an entry if the fate id doesn't exist
    FateMutator.Status status =
        newMutator(fateId).requireStatus(TStatus.values()).putReservedTx(reservation).tryMutate();
    if (status.equals(FateMutator.Status.ACCEPTED)) {
      return Optional.of(new FateTxStoreImpl(fateId, reservation));
    } else if (status.equals(FateMutator.Status.UNKNOWN)) {
      // If the status is UNKNOWN, this means an error occurred after the mutation was
      // sent to the TabletServer, and it is unknown if the mutation was written. We
      // need to check if the mutation was written and if it was written by this
      // attempt at reservation. If it was written by this reservation attempt,
      // we can return the FateTxStore since it was successfully reserved in this
      // attempt, otherwise we return empty (was written by another reservation
      // attempt or was not written at all).
      try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(getRow(fateId));
        scanner.fetchColumn(TxColumnFamily.RESERVATION_COLUMN.getColumnFamily(),
            TxColumnFamily.RESERVATION_COLUMN.getColumnQualifier());
        FateReservation persistedRes =
            scanner.stream().map(entry -> FateReservation.deserialize(entry.getValue().get()))
                .findFirst().orElse(null);
        if (persistedRes != null && persistedRes.equals(reservation)) {
          return Optional.of(new FateTxStoreImpl(fateId, reservation));
        }
      } catch (TableNotFoundException e) {
        throw new IllegalStateException(tableName + " not found!", e);
      }
    }
    return Optional.empty();
  }

  @Override
  public void deleteDeadReservations() {
    for (Entry<FateId,FateReservation> activeRes : getActiveReservations().entrySet()) {
      FateId fateId = activeRes.getKey();
      FateReservation reservation = activeRes.getValue();
      if (!isLockHeld.test(reservation.getLockID())) {
        var status = newMutator(fateId).putUnreserveTx(reservation).tryMutate();
        if (status == FateMutator.Status.ACCEPTED) {
          // Technically, this should also be logged for the case where the mutation status
          // is UNKNOWN, but the mutation was actually written (fate id was unreserved)
          // but there is no way to tell if it was unreserved from this mutation or another
          // thread simply unreserving the transaction
          log.trace("Deleted the dead reservation {} for fate id {}", reservation, fateId);
        }
        // No need to verify the status... If it is ACCEPTED, we have successfully unreserved
        // the dead transaction. If it is REJECTED, the reservation has changed (i.e.,
        // has been unreserved so no need to do anything, or has been unreserved and reserved
        // again in which case we don't want to change it). If it is UNKNOWN, the mutation
        // may or may not have been written. If it was written, we have successfully unreserved
        // the dead transaction. If it was not written, the next cycle/call to
        // deleteDeadReservations() will try again.
      }
    }
  }

  @Override
  protected Stream<FateIdStatus> getTransactions(EnumSet<TStatus> statuses) {
    try {
      Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY);
      scanner.setRange(new Range());
      RowFateStatusFilter.configureScanner(scanner, statuses);
      TxColumnFamily.STATUS_COLUMN.fetch(scanner);
      TxColumnFamily.RESERVATION_COLUMN.fetch(scanner);
      TxInfoColumnFamily.FATE_OP_COLUMN.fetch(scanner);
      return scanner.stream().onClose(scanner::close).map(e -> {
        String txUUIDStr = e.getKey().getRow().toString();
        FateId fateId = FateId.from(fateInstanceType, txUUIDStr);
        SortedMap<Key,Value> rowMap;
        TStatus status = TStatus.UNKNOWN;
        FateReservation reservation = null;
        Fate.FateOperation fateOp = null;
        try {
          rowMap = WholeRowIterator.decodeRow(e.getKey(), e.getValue());
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
        // Always expect a status, optionally expect a fate operation (present if seeded)
        // and optionally expect a fate reservation (present if currently reserved)
        Preconditions.checkState(rowMap.size() >= 1 && rowMap.size() <= 3,
            "Invalid row seen: %s. Expected to see tx status and optionally a fate op and "
                + "optionally a fate reservation",
            rowMap);
        for (Entry<Key,Value> entry : rowMap.entrySet()) {
          Text colf = entry.getKey().getColumnFamily();
          Text colq = entry.getKey().getColumnQualifier();
          Value val = entry.getValue();
          switch (colq.toString()) {
            case TxColumnFamily.STATUS:
              status = TStatus.valueOf(val.toString());
              break;
            case TxColumnFamily.RESERVATION:
              reservation = FateReservation.deserialize(val.get());
              break;
            case TxInfoColumnFamily.FATE_OP:
              fateOp = (Fate.FateOperation) deserializeTxInfo(TxInfo.FATE_OP, val.get());
              break;
            default:
              throw new IllegalStateException("Unexpected column seen: " + colf + ":" + colq);
          }
        }
        final TStatus finalStatus = status;
        final Optional<FateReservation> finalReservation = Optional.ofNullable(reservation);
        final Optional<Fate.FateOperation> finalFateOp = Optional.ofNullable(fateOp);
        return new FateIdStatusBase(fateId) {
          @Override
          public TStatus getStatus() {
            return finalStatus;
          }

          @Override
          public Optional<FateReservation> getFateReservation() {
            return finalReservation;
          }

          @Override
          public Optional<Fate.FateOperation> getFateOperation() {
            return finalFateOp;
          }
        };
      });
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(tableName + " not found!", e);
    }
  }

  @Override
  public Stream<FateKey> list(FateKey.FateKeyType type) {
    try {
      Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY);
      scanner.setRange(new Range());
      TxColumnFamily.TX_KEY_COLUMN.fetch(scanner);
      FateKeyFilter.configureScanner(scanner, type);
      return scanner.stream().onClose(scanner::close)
          .map(e -> FateKey.deserialize(e.getValue().get()));
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(tableName + " not found!", e);
    }
  }

  @Override
  protected TStatus _getStatus(FateId fateId) {
    return scanTx(scanner -> {
      scanner.setRange(getRow(fateId));
      TxColumnFamily.STATUS_COLUMN.fetch(scanner);
      return scanner.stream().map(e -> TStatus.valueOf(e.getValue().toString())).findFirst()
          .orElse(TStatus.UNKNOWN);
    });
  }

  @Override
  protected Optional<FateKey> getKey(FateId fateId) {
    return scanTx(scanner -> {
      scanner.setRange(getRow(fateId));
      TxColumnFamily.TX_KEY_COLUMN.fetch(scanner);
      return scanner.stream().map(e -> FateKey.deserialize(e.getValue().get())).findFirst();
    });
  }

  @Override
  protected FateTxStore<T> newUnreservedFateTxStore(FateId fateId) {
    return new FateTxStoreImpl(fateId);
  }

  static Range getRow(FateId fateId) {
    return new Range(getRowId(fateId));
  }

  public static String getRowId(FateId fateId) {
    return fateId.getTxUUIDStr();
  }

  private FateMutatorImpl<T> newMutator(FateId fateId) {
    return new FateMutatorImpl<>(context, tableName, fateId);
  }

  private <R> R scanTx(Function<Scanner,R> func) {
    try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
      return func.apply(scanner);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(tableName + " not found!", e);
    }
  }

  @Override
  public FateInstanceType type() {
    return fateInstanceType;
  }

  private class BatchSeeder implements Seeder<T> {
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Map<FateId,Pair<FateMutator<T>,CompletableFuture<Optional<FateId>>>> pending =
        new HashMap<>();

    @Override
    public CompletableFuture<Optional<FateId>> attemptToSeedTransaction(FateOperation fateOp,
        FateKey fateKey, Repo<T> repo, boolean autoCleanUp) {
      Preconditions.checkState(!closed.get(), "Can't attempt to seed with a closed seeder.");

      final var fateId = fateIdGenerator.fromTypeAndKey(type(), fateKey);
      // If not already submitted, add to the pending list and return the future
      // or the existing future if duplicate. The pending map will store the mutator
      // to be processed on close in a one batch.
      return pending.computeIfAbsent(fateId, id -> {
        FateMutator<T> mutator = seedTransaction(fateOp, fateKey, fateId, repo, autoCleanUp);
        CompletableFuture<Optional<FateId>> future = new CompletableFuture<>();
        return new Pair<>(mutator, future);
      }).getSecond();
    }

    @Override
    public void close() {
      closed.set(true);

      int maxAttempts = 5;

      // This loop will submit all the pending mutations as one batch
      // to a conditional writer and any known results will be removed
      // from the pending map. Unknown results will be re-attempted up
      // to the maxAttempts count
      for (int attempt = 0; attempt < maxAttempts && !pending.isEmpty(); attempt++) {
        var currentResults = tryMutateBatch();
        for (Entry<FateId,ConditionalWriter.Status> result : currentResults.entrySet()) {
          var fateId = result.getKey();
          var status = result.getValue();
          var future = pending.get(fateId).getSecond();
          switch (result.getValue()) {
            case ACCEPTED:
              seededTx();
              log.trace("Attempt to seed {} returned {}", fateId.canonical(), status);
              // Complete the future with the fatId and remove from pending
              future.complete(Optional.of(fateId));
              pending.remove(fateId);
              break;
            case REJECTED:
              log.debug("Attempt to seed {} returned {}", fateId.canonical(), status);
              // Rejected so complete with an empty optional and remove from pending
              future.complete(Optional.empty());
              pending.remove(fateId);
              break;
            case UNKNOWN:
              log.debug("Attempt to seed {} returned {} status, retrying", fateId.canonical(),
                  status);
              // unknown, so don't remove from map so that we try again if still under
              // max attempts
              break;
            default:
              // do not expect other statuses
              throw new IllegalStateException("Unhandled status for mutation " + status);
          }
        }

        if (!pending.isEmpty()) {
          // At this point can not reliably determine if the unknown pending mutations were
          // successful or not because no reservation was acquired. For example since no
          // reservation was acquired it is possible that seeding was a success and something
          // immediately picked it up and started operating on it and changing it.
          // If scanning after that point can not conclude success or failure. Another situation
          // is that maybe the fateId already existed in a seeded form prior to getting this
          // unknown.
          UtilWaitThread.sleep(250);
        }
      }

      // Any remaining will be UNKNOWN status, so complete the futures with an optional empty
      pending.forEach((fateId, pair) -> {
        pair.getSecond().complete(Optional.empty());
        log.warn("Repeatedly received unknown status when attempting to seed {}",
            fateId.canonical());
      });
    }

    // Submit all the pending mutations to a single conditional writer
    // as one batch and return the results for each mutation
    private Map<FateId,ConditionalWriter.Status> tryMutateBatch() {
      if (pending.isEmpty()) {
        return Map.of();
      }

      final Map<FateId,ConditionalWriter.Status> resultsMap = new HashMap<>();
      try (ConditionalWriter writer = context.createConditionalWriter(tableName)) {
        Iterator<ConditionalWriter.Result> results = writer
            .write(pending.values().stream().map(pair -> pair.getFirst().getMutation()).iterator());
        while (results.hasNext()) {
          var result = results.next();
          var row = new Text(result.getMutation().getRow());
          resultsMap.put(FateId.from(FateInstanceType.USER, row.toString()), result.getStatus());
        }
      } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
        throw new IllegalStateException(e);
      }
      return resultsMap;
    }
  }

  private class FateTxStoreImpl extends AbstractFateTxStoreImpl {

    private FateTxStoreImpl(FateId fateId) {
      super(fateId);
    }

    private FateTxStoreImpl(FateId fateId, FateReservation reservation) {
      super(fateId, reservation);
    }

    @Override
    public Repo<T> top() {
      verifyReservedAndNotDeleted(false);

      return scanTx(scanner -> {
        scanner.setRange(getRow(fateId));
        scanner.setBatchSize(1);
        scanner.fetchColumnFamily(RepoColumnFamily.NAME);
        return scanner.stream().map(e -> {
          @SuppressWarnings("unchecked")
          var repo = (Repo<T>) deserialize(e.getValue().get());
          return repo;
        }).findFirst().orElse(null);
      });
    }

    @Override
    public List<ReadOnlyRepo<T>> getStack() {
      verifyReservedAndNotDeleted(false);

      return scanTx(scanner -> {
        scanner.setRange(getRow(fateId));
        scanner.fetchColumnFamily(RepoColumnFamily.NAME);
        return scanner.stream().map(e -> {
          @SuppressWarnings("unchecked")
          var repo = (ReadOnlyRepo<T>) deserialize(e.getValue().get());
          return repo;
        }).collect(Collectors.toList());
      });
    }

    @Override
    public Serializable getTransactionInfo(TxInfo txInfo) {
      verifyReservedAndNotDeleted(false);

      try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(getRow(fateId));

        final ColumnFQ cq;
        switch (txInfo) {
          case FATE_OP:
            cq = TxInfoColumnFamily.FATE_OP_COLUMN;
            break;
          case AUTO_CLEAN:
            cq = TxInfoColumnFamily.AUTO_CLEAN_COLUMN;
            break;
          case EXCEPTION:
            cq = TxInfoColumnFamily.EXCEPTION_COLUMN;
            break;
          case RETURN_VALUE:
            cq = TxInfoColumnFamily.RETURN_VALUE_COLUMN;
            break;
          case TX_AGEOFF:
            cq = TxInfoColumnFamily.TX_AGEOFF_COLUMN;
            break;
          default:
            throw new IllegalArgumentException("Unexpected TxInfo type " + txInfo);
        }
        scanner.fetchColumn(cq.getColumnFamily(), cq.getColumnQualifier());

        return scanner.stream().map(e -> deserializeTxInfo(txInfo, e.getValue().get())).findFirst()
            .orElse(null);
      } catch (TableNotFoundException e) {
        throw new IllegalStateException(tableName + " not found!", e);
      }
    }

    @Override
    public long timeCreated() {
      verifyReservedAndNotDeleted(false);

      return scanTx(scanner -> {
        scanner.setRange(getRow(fateId));
        TxColumnFamily.CREATE_TIME_COLUMN.fetch(scanner);
        return scanner.stream().map(e -> Long.parseLong(e.getValue().toString())).findFirst()
            .orElse(0L);
      });
    }

    @Override
    public void push(Repo<T> repo) throws StackOverflowException {
      verifyReservedAndNotDeleted(true);

      Optional<Integer> top = findTop();

      if (top.filter(t -> t >= MAX_REPOS).isPresent()) {
        throw new StackOverflowException("Repo stack size too large");
      }

      FateMutator<T> fateMutator =
          newMutator(fateId).requireStatus(REQ_PUSH_STATUS.toArray(TStatus[]::new));
      fateMutator.putRepo(top.map(t -> t + 1).orElse(1), repo).mutate();
    }

    @Override
    public void pop() {
      verifyReservedAndNotDeleted(true);

      Optional<Integer> top = findTop();
      top.ifPresent(t -> newMutator(fateId).requireStatus(REQ_POP_STATUS.toArray(TStatus[]::new))
          .deleteRepo(t).mutate());
    }

    @Override
    public void setStatus(TStatus status) {
      verifyReservedAndNotDeleted(true);

      newMutator(fateId).putStatus(status).mutate();
      observedStatus = status;
    }

    @Override
    public void setTransactionInfo(TxInfo txInfo, Serializable so) {
      verifyReservedAndNotDeleted(true);

      final byte[] serialized = serializeTxInfo(so);

      newMutator(fateId).putTxInfo(txInfo, serialized).mutate();
    }

    @Override
    public void delete() {
      verifyReservedAndNotDeleted(true);

      var mutator = newMutator(fateId);
      mutator.requireStatus(REQ_DELETE_STATUS.toArray(TStatus[]::new));
      mutator.delete().mutate();
      this.deleted = true;
    }

    @Override
    public void forceDelete() {
      verifyReservedAndNotDeleted(true);

      var mutator = newMutator(fateId);
      mutator.requireStatus(REQ_FORCE_DELETE_STATUS.toArray(TStatus[]::new));
      mutator.delete().mutate();
      this.deleted = true;
    }

    private Optional<Integer> findTop() {
      return scanTx(scanner -> {
        scanner.setRange(getRow(fateId));
        scanner.setBatchSize(1);
        scanner.fetchColumnFamily(RepoColumnFamily.NAME);
        return scanner.stream().map(e -> restoreRepo(e.getKey().getColumnQualifier())).findFirst();
      });
    }

    @Override
    protected void unreserve() {
      if (!deleted) {
        FateMutator.Status status;
        do {
          status = newMutator(fateId).putUnreserveTx(reservation).tryMutate();
        } while (status.equals(FateMutator.Status.UNKNOWN));
      }
      reservation = null;
    }
  }

  static Text invertRepo(int position) {
    Preconditions.checkArgument(REPO_RANGE.contains(position),
        "Position %s is not in the valid range of [0,%s]", position, MAX_REPOS);
    return new Text(String.format("%02d", MAX_REPOS - position));
  }

  static Integer restoreRepo(Text invertedPosition) {
    int position = MAX_REPOS - Integer.parseInt(invertedPosition.toString());
    Preconditions.checkArgument(REPO_RANGE.contains(position),
        "Position %s is not in the valid range of [0,%s]", position, MAX_REPOS);
    return position;
  }
}
