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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import org.apache.accumulo.core.metadata.AccumuloTable;
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
    final int maxAttempts = 5;

    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      FateId fateId = getFateId();

      if (attempt >= 1) {
        log.debug("Failed to create new id: {}, trying again", fateId);
        UtilWaitThread.sleep(100);
      }

      var status = newMutator(fateId).requireStatus().putStatus(TStatus.NEW)
          .putCreateTime(System.currentTimeMillis()).putInitReserveColVal().tryMutate();

      switch (status) {
        case ACCEPTED:
          return fateId;
        case UNKNOWN:
        case REJECTED:
          continue;
        default:
          throw new IllegalStateException("Unknown status " + status);
      }
    }

    throw new IllegalStateException("Failed to create new id after " + maxAttempts + " attempts");
  }

  public FateId getFateId() {
    return FateId.from(fateInstanceType, UUID.randomUUID());
  }

  @Override
  protected void create(FateId fateId, FateKey fateKey) {
    final int maxAttempts = 5;

    for (int attempt = 0; attempt < maxAttempts; attempt++) {

      if (attempt >= 1) {
        log.debug("Failed to create transaction with fateId {} and fateKey {}, trying again",
            fateId, fateKey);
        UtilWaitThread.sleep(100);
      }

      var status = newMutator(fateId).requireStatus().putStatus(TStatus.NEW).putKey(fateKey)
          .putCreateTime(System.currentTimeMillis()).putInitReserveColVal().tryMutate();

      switch (status) {
        case ACCEPTED:
          return;
        case UNKNOWN:
          continue;
        case REJECTED:
          throw new IllegalStateException("Attempt to create transaction with fateId " + fateId
              + " and fateKey " + fateKey + " was rejected");
        default:
          throw new IllegalStateException("Unknown status " + status);
      }
    }

    throw new IllegalStateException("Failed to create transaction with fateId " + fateId
        + " and fateKey " + fateKey + " after " + maxAttempts + " attempts");
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
            .filter(entry -> FateReservation.isFateReservation(entry.getValue().toString()))
            .map(entry -> FateReservation.from(entry.getValue().toString())).findFirst()
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
  public boolean isReserved(FateId fateId) {
    try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setRange(getRow(fateId));
      scanner.fetchColumn(TxColumnFamily.RESERVATION_COLUMN.getColumnFamily(),
          TxColumnFamily.RESERVATION_COLUMN.getColumnQualifier());
      return scanner.stream()
          .map(entry -> FateReservation.isFateReservation(entry.getValue().toString())).findFirst()
          .orElse(false);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(tableName + " not found!", e);
    }
  }

  @Override
  public Map<FateId,FateReservation> getActiveReservations() {
    Map<FateId,FateReservation> activeReservations = new HashMap<>();

    try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setRange(new Range());
      scanner.fetchColumn(TxColumnFamily.RESERVATION_COLUMN.getColumnFamily(),
          TxColumnFamily.RESERVATION_COLUMN.getColumnQualifier());
      scanner.stream()
          .filter(entry -> FateReservation.isFateReservation(entry.getValue().toString()))
          .forEach(entry -> {
            String reservationColVal = entry.getValue().toString();
            FateId fateId = FateId.from(fateInstanceType, entry.getKey().getRow().toString());
            FateReservation reservation = FateReservation.from(reservationColVal);
            activeReservations.put(fateId, reservation);
          });
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(tableName + " not found!", e);
    }

    return activeReservations;
  }

  @Override
  public void deleteDeadReservations() {
    for (Entry<FateId,FateReservation> entry : getActiveReservations().entrySet()) {
      FateId fateId = entry.getKey();
      FateReservation reservation = entry.getValue();
      if (!isLockHeld.test(reservation.getLockID())) {
        newMutator(fateId).putUnreserveTx(reservation).tryMutate();
        // No need to check the status... If it is ACCEPTED, we have successfully unreserved
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
      FateStatusFilter.configureScanner(scanner, statuses);
      TxColumnFamily.STATUS_COLUMN.fetch(scanner);
      return scanner.stream().onClose(scanner::close).map(e -> {
        String txUUIDStr = e.getKey().getRow().toString();
        FateId fateId = FateId.from(fateInstanceType, txUUIDStr);
        return new FateIdStatusBase(fateId) {
          @Override
          public TStatus getStatus() {
            return TStatus.valueOf(e.getValue().toString());
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
  protected Pair<TStatus,Optional<FateKey>> getStatusAndKey(FateId fateId) {
    return scanTx(scanner -> {
      scanner.setRange(getRow(fateId));
      TxColumnFamily.STATUS_COLUMN.fetch(scanner);
      TxColumnFamily.TX_KEY_COLUMN.fetch(scanner);

      TStatus status = null;
      FateKey key = null;

      for (Entry<Key,Value> entry : scanner) {
        final String qual = entry.getKey().getColumnQualifierData().toString();
        switch (qual) {
          case TxColumnFamily.STATUS:
            status = TStatus.valueOf(entry.getValue().toString());
            break;
          case TxColumnFamily.TX_KEY:
            key = FateKey.deserialize(entry.getValue().get());
            break;
          default:
            throw new IllegalStateException("Unexpected column qualifier: " + qual);
        }
      }

      return new Pair<>(Optional.ofNullable(status).orElse(TStatus.UNKNOWN),
          Optional.ofNullable(key));
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

  private class FateTxStoreImpl extends AbstractFateTxStoreImpl<T> {

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
