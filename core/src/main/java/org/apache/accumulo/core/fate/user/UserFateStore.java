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
import java.time.Duration;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.ReadOnlyRepo;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.StackOverflowException;
import org.apache.accumulo.core.fate.user.schema.FateSchema.RepoColumnFamily;
import org.apache.accumulo.core.fate.user.schema.FateSchema.TxColumnFamily;
import org.apache.accumulo.core.fate.user.schema.FateSchema.TxInfoColumnFamily;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
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
  private static final int maxRepos = 100;
  private static final com.google.common.collect.Range<Integer> REPO_RANGE =
      com.google.common.collect.Range.closed(1, maxRepos);

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

  public UserFateStore(ClientContext context, ZooUtil.LockID lockID,
      Predicate<ZooUtil.LockID> isLockHeld) {
    this(context, AccumuloTable.FATE.tableName(), lockID, isLockHeld);
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

      var status = newMutator(fateId).requireStatus().putStatus(TStatus.NEW)
          .putCreateTime(System.currentTimeMillis()).putInitReservationVal().tryMutate();

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
    return FateId.from(fateInstanceType, UUID.randomUUID());
  }

  @Override
  public Optional<FateTxStore<T>> createAndReserve(FateKey fateKey) {
    final var reservation = FateReservation.from(lockID, UUID.randomUUID());
    final var fateId = fateIdGenerator.fromTypeAndKey(type(), fateKey);
    Optional<FateTxStore<T>> txStore = Optional.empty();
    int maxAttempts = 5;
    FateMutator.Status status = null;

    // Only need to retry if it is UNKNOWN
    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      status = newMutator(fateId).requireStatus().putStatus(TStatus.NEW).putKey(fateKey)
          .putReservedTxOnCreation(reservation).putCreateTime(System.currentTimeMillis())
          .tryMutate();
      if (status != FateMutator.Status.UNKNOWN) {
        break;
      }
      UtilWaitThread.sleep(100);
    }

    switch (status) {
      case ACCEPTED:
        txStore = Optional.of(new FateTxStoreImpl(fateId, reservation));
        break;
      case REJECTED:
        // If the status is REJECTED, we need to check what about the mutation was REJECTED:
        // 1) Possible something like the following occurred:
        // the first attempt was UNKNOWN but written, the next attempt would be rejected
        // We return the FateTxStore in this case.
        // 2) If there is a collision with existing fate id, throw error
        // 3) If the fate id is already reserved, return an empty optional
        // 4) If the fate id is still NEW/unseeded and unreserved, we can try to reserve it
        try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
          scanner.setRange(getRow(fateId));
          scanner.fetchColumn(TxColumnFamily.STATUS_COLUMN.getColumnFamily(),
              TxColumnFamily.STATUS_COLUMN.getColumnQualifier());
          scanner.fetchColumn(TxColumnFamily.TX_KEY_COLUMN.getColumnFamily(),
              TxColumnFamily.TX_KEY_COLUMN.getColumnQualifier());
          scanner.fetchColumn(TxColumnFamily.RESERVATION_COLUMN.getColumnFamily(),
              TxColumnFamily.RESERVATION_COLUMN.getColumnQualifier());
          TStatus statusSeen = TStatus.UNKNOWN;
          Optional<FateKey> fateKeySeen = Optional.empty();
          Optional<FateReservation> reservationSeen = Optional.empty();

          for (Entry<Key,Value> entry : scanner) {
            Text colf = entry.getKey().getColumnFamily();
            Text colq = entry.getKey().getColumnQualifier();
            Value val = entry.getValue();

            switch (colq.toString()) {
              case TxColumnFamily.STATUS:
                statusSeen = TStatus.valueOf(val.toString());
                break;
              case TxColumnFamily.TX_KEY:
                fateKeySeen = Optional.of(FateKey.deserialize(val.get()));
                break;
              case TxColumnFamily.RESERVATION:
                if (FateReservation.isFateReservation(val.get())) {
                  reservationSeen = Optional.of(FateReservation.deserialize(val.get()));
                }
                break;
              default:
                throw new IllegalStateException("Unexpected column seen: " + colf + ":" + colq);
            }
          }

          if (statusSeen == TStatus.NEW) {
            verifyFateKey(fateId, fateKeySeen, fateKey);
            // This will be the case if the mutation status is REJECTED but the mutation was written
            if (reservationSeen.isPresent() && reservationSeen.orElseThrow().equals(reservation)) {
              txStore = Optional.of(new FateTxStoreImpl(fateId, reservation));
            } else if (reservationSeen.isEmpty()) {
              // NEW/unseeded transaction and not reserved, so we can allow it to be reserved
              // we tryReserve() since another thread may have reserved it since the scan
              txStore = tryReserve(fateId);
              // the status was known before reserving to be NEW,
              // however it could change so check after reserving to avoid race conditions.
              var statusAfterReserve =
                  txStore.map(ReadOnlyFateTxStore::getStatus).orElse(TStatus.UNKNOWN);
              if (statusAfterReserve != TStatus.NEW) {
                txStore.ifPresent(txs -> txs.unreserve(Duration.ZERO));
                txStore = Optional.empty();
              }
            }
          } else {
            log.trace(
                "fate id {} tstatus {} fate key {} is reserved {} "
                    + "has already been seeded with work (non-NEW status)",
                fateId, statusSeen, fateKeySeen.orElse(null), reservationSeen.isPresent());
          }
        } catch (TableNotFoundException e) {
          throw new IllegalStateException(tableName + " not found!", e);
        }
        break;
      default:
        throw new IllegalStateException("Unknown or unexpected status " + status);
    }

    return txStore;
  }

  @Override
  public Optional<FateTxStore<T>> tryReserve(FateId fateId) {
    // Create a unique FateReservation for this reservation attempt
    FateReservation reservation = FateReservation.from(lockID, UUID.randomUUID());

    FateMutator.Status status = newMutator(fateId).putReservedTx(reservation).tryMutate();
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
        FateReservation persistedRes = scanner.stream()
            .filter(entry -> FateReservation.isFateReservation(entry.getValue().get()))
            .map(entry -> FateReservation.deserialize(entry.getValue().get())).findFirst()
            .orElse(null);
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
  protected Stream<FateIdStatus> getTransactions(Set<TStatus> statuses) {
    try {
      Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY);
      scanner.setRange(new Range());
      RowFateStatusFilter.configureScanner(scanner, statuses);
      TxColumnFamily.STATUS_COLUMN.fetch(scanner);
      TxColumnFamily.RESERVATION_COLUMN.fetch(scanner);
      return scanner.stream().onClose(scanner::close).map(e -> {
        String txUUIDStr = e.getKey().getRow().toString();
        FateId fateId = FateId.from(fateInstanceType, txUUIDStr);
        SortedMap<Key,Value> rowMap;
        TStatus status = TStatus.UNKNOWN;
        FateReservation reservation = null;
        try {
          rowMap = WholeRowIterator.decodeRow(e.getKey(), e.getValue());
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
        // expect status and optionally reservation
        Preconditions.checkState(rowMap.size() == 1 || rowMap.size() == 2,
            "Invalid row seen: %s. Expected to see one entry for the status and optionally an "
                + "entry for the fate reservation",
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
              if (FateReservation.isFateReservation(val.get())) {
                reservation = FateReservation.deserialize(val.get());
              }
              break;
            default:
              throw new IllegalStateException("Unexpected column seen: " + colf + ":" + colq);
          }
        }
        final TStatus finalStatus = status;
        final Optional<FateReservation> finalReservation = Optional.ofNullable(reservation);
        return new FateIdStatusBase(fateId) {
          @Override
          public TStatus getStatus() {
            return finalStatus;
          }

          @Override
          public Optional<FateReservation> getFateReservation() {
            return finalReservation;
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

  private class FateTxStoreImpl extends AbstractFateTxStoreImpl {

    private FateTxStoreImpl(FateId fateId) {
      super(fateId);
    }

    private FateTxStoreImpl(FateId fateId, FateReservation reservation) {
      super(fateId, reservation);
    }

    @Override
    public Repo<T> top() {
      verifyReserved(false);

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
      verifyReserved(false);

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
      verifyReserved(false);

      try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(getRow(fateId));

        final ColumnFQ cq;
        switch (txInfo) {
          case TX_NAME:
            cq = TxInfoColumnFamily.TX_NAME_COLUMN;
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
      verifyReserved(false);

      return scanTx(scanner -> {
        scanner.setRange(getRow(fateId));
        TxColumnFamily.CREATE_TIME_COLUMN.fetch(scanner);
        return scanner.stream().map(e -> Long.parseLong(e.getValue().toString())).findFirst()
            .orElse(0L);
      });
    }

    @Override
    public void push(Repo<T> repo) throws StackOverflowException {
      verifyReserved(true);

      Optional<Integer> top = findTop();

      if (top.filter(t -> t >= maxRepos).isPresent()) {
        throw new StackOverflowException("Repo stack size too large");
      }

      FateMutator<T> fateMutator =
          newMutator(fateId).requireStatus(TStatus.IN_PROGRESS, TStatus.NEW);
      fateMutator.putRepo(top.map(t -> t + 1).orElse(1), repo).mutate();
    }

    @Override
    public void pop() {
      verifyReserved(true);

      Optional<Integer> top = findTop();
      top.ifPresent(t -> newMutator(fateId)
          .requireStatus(TStatus.FAILED_IN_PROGRESS, TStatus.SUCCESSFUL).deleteRepo(t).mutate());
    }

    @Override
    public void setStatus(TStatus status) {
      verifyReserved(true);

      newMutator(fateId).putStatus(status).mutate();
      observedStatus = status;
    }

    @Override
    public void setTransactionInfo(TxInfo txInfo, Serializable so) {
      verifyReserved(true);

      final byte[] serialized = serializeTxInfo(so);

      newMutator(fateId).putTxInfo(txInfo, serialized).mutate();
    }

    @Override
    public void delete() {
      verifyReserved(true);

      var mutator = newMutator(fateId);
      mutator.requireStatus(TStatus.NEW, TStatus.SUBMITTED, TStatus.SUCCESSFUL, TStatus.FAILED);
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
        "Position %s is not in the valid range of [0,%s]", position, maxRepos);
    return new Text(String.format("%02d", maxRepos - position));
  }

  static Integer restoreRepo(Text invertedPosition) {
    int position = maxRepos - Integer.parseInt(invertedPosition.toString());
    Preconditions.checkArgument(REPO_RANGE.contains(position),
        "Position %s is not in the valid range of [0,%s]", position, maxRepos);
    return position;
  }
}
