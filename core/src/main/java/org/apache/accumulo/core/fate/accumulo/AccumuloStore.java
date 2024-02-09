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
package org.apache.accumulo.core.fate.accumulo;

import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.io.Serializable;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
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
import org.apache.accumulo.core.fate.accumulo.schema.FateSchema.RepoColumnFamily;
import org.apache.accumulo.core.fate.accumulo.schema.FateSchema.TxColumnFamily;
import org.apache.accumulo.core.fate.accumulo.schema.FateSchema.TxInfoColumnFamily;
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

public class AccumuloStore<T> extends AbstractFateStore<T> {

  private static final Logger log = LoggerFactory.getLogger(AccumuloStore.class);

  private final ClientContext context;
  private final String tableName;

  private static final FateInstanceType fateInstanceType = FateInstanceType.USER;
  private static final int maxRepos = 100;
  private static final com.google.common.collect.Range<Integer> REPO_RANGE =
      com.google.common.collect.Range.closed(1, maxRepos);

  public AccumuloStore(ClientContext context, String tableName) {
    this(context, tableName, DEFAULT_MAX_DEFERRED, DEFAULT_FATE_ID_GENERATOR);
  }

  @VisibleForTesting
  public AccumuloStore(ClientContext context, String tableName, int maxDeferred,
      FateIdGenerator fateIdGenerator) {
    super(maxDeferred, fateIdGenerator);
    this.context = Objects.requireNonNull(context);
    this.tableName = Objects.requireNonNull(tableName);
  }

  public AccumuloStore(ClientContext context) {
    this(context, AccumuloTable.FATE.tableName());
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
          .putCreateTime(System.currentTimeMillis()).tryMutate();

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
    long tid = RANDOM.get().nextLong() & 0x7fffffffffffffffL;
    return FateId.from(fateInstanceType, tid);
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
          .putCreateTime(System.currentTimeMillis()).tryMutate();

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
  protected Stream<FateIdStatus> getTransactions() {
    try {
      Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY);
      scanner.setRange(new Range());
      TxColumnFamily.STATUS_COLUMN.fetch(scanner);
      return scanner.stream().onClose(scanner::close).map(e -> {
        String hexTid = e.getKey().getRow().toString().split("_")[1];
        FateId fateId = FateId.from(fateInstanceType, hexTid);
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
  protected FateTxStore<T> newFateTxStore(FateId fateId, boolean isReserved) {
    return new FateTxStoreImpl(fateId, isReserved);
  }

  @Override
  protected FateInstanceType getInstanceType() {
    return fateInstanceType;
  }

  static Range getRow(FateId fateId) {
    return new Range(getRowId(fateId));
  }

  public static String getRowId(FateId fateId) {
    return "tx_" + fateId.getHexTid();
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

  private class FateTxStoreImpl extends AbstractFateTxStoreImpl<T> {

    private FateTxStoreImpl(FateId fateId, boolean isReserved) {
      super(fateId, isReserved);
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

      FateMutator<T> fateMutator = newMutator(fateId);
      fateMutator.putRepo(top.map(t -> t + 1).orElse(1), repo).mutate();
    }

    @Override
    public void pop() {
      verifyReserved(true);

      Optional<Integer> top = findTop();
      top.ifPresent(t -> newMutator(fateId).deleteRepo(t).mutate());
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

      newMutator(fateId).delete().mutate();
    }

    private Optional<Integer> findTop() {
      return scanTx(scanner -> {
        scanner.setRange(getRow(fateId));
        scanner.setBatchSize(1);
        scanner.fetchColumnFamily(RepoColumnFamily.NAME);
        return scanner.stream().map(e -> restoreRepo(e.getKey().getColumnQualifier())).findFirst();
      });
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
