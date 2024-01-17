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
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.ReadOnlyRepo;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.StackOverflowException;
import org.apache.accumulo.core.fate.accumulo.schema.FateSchema.RepoColumnFamily;
import org.apache.accumulo.core.fate.accumulo.schema.FateSchema.TxColumnFamily;
import org.apache.accumulo.core.fate.accumulo.schema.FateSchema.TxInfoColumnFamily;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class AccumuloStore<T> extends AbstractFateStore<T> {

  private final ClientContext context;
  private final String tableName;

  private static final int maxRepos = 100;
  private static final com.google.common.collect.Range<Integer> REPO_RANGE =
      com.google.common.collect.Range.closed(1, maxRepos);

  public AccumuloStore(ClientContext context, String tableName) {
    this.context = Objects.requireNonNull(context);
    this.tableName = Objects.requireNonNull(tableName);
  }

  public AccumuloStore(ClientContext context) {
    this(context, AccumuloTable.FATE.tableName());
  }

  @Override
  public long create() {
    long tid = RANDOM.get().nextLong() & 0x7fffffffffffffffL;

    newMutator(tid).putStatus(TStatus.NEW).putCreateTime(System.currentTimeMillis()).mutate();

    return tid;
  }

  @Override
  protected Stream<FateIdStatus> getTransactions() {
    try {
      Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY);
      scanner.setRange(new Range());
      TxColumnFamily.STATUS_COLUMN.fetch(scanner);
      return scanner.stream().onClose(scanner::close).map(e -> {
        return new FateIdStatusBase(parseTid(e.getKey().getRow().toString())) {
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
  protected TStatus _getStatus(long tid) {
    return scanTx(scanner -> {
      scanner.setRange(getRow(tid));
      TxColumnFamily.STATUS_COLUMN.fetch(scanner);
      return scanner.stream().map(e -> TStatus.valueOf(e.getValue().toString())).findFirst()
          .orElse(TStatus.UNKNOWN);
    });
  }

  @Override
  protected FateTxStore<T> newFateTxStore(long tid, boolean isReserved) {
    return new FateTxStoreImpl(tid, isReserved);
  }

  static Range getRow(long tid) {
    return new Range("tx_" + FastFormat.toHexString(tid));
  }

  private FateMutatorImpl<T> newMutator(long tid) {
    return new FateMutatorImpl<>(context, tableName, tid);
  }

  private <R> R scanTx(Function<Scanner,R> func) {
    try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
      return func.apply(scanner);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(tableName + " not found!", e);
    }
  }

  private class FateTxStoreImpl extends AbstractFateTxStoreImpl<T> {

    private FateTxStoreImpl(long tid, boolean isReserved) {
      super(tid, isReserved);
    }

    @Override
    public Repo<T> top() {
      verifyReserved(false);

      return scanTx(scanner -> {
        scanner.setRange(getRow(tid));
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
        scanner.setRange(getRow(tid));
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
        scanner.setRange(getRow(tid));

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
        scanner.setRange(getRow(tid));
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

      FateMutator<T> fateMutator = newMutator(tid);
      fateMutator.putRepo(top.map(t -> t + 1).orElse(1), repo).mutate();
    }

    @Override
    public void pop() {
      verifyReserved(true);

      Optional<Integer> top = findTop();
      top.ifPresent(t -> newMutator(tid).deleteRepo(t).mutate());
    }

    @Override
    public void setStatus(TStatus status) {
      verifyReserved(true);

      newMutator(tid).putStatus(status).mutate();
      observedStatus = status;
    }

    @Override
    public void setTransactionInfo(TxInfo txInfo, Serializable so) {
      verifyReserved(true);

      FateMutator<T> fateMutator = newMutator(tid);
      final byte[] serialized = serializeTxInfo(so);

      switch (txInfo) {
        case TX_NAME:
          fateMutator.putName(serialized);
          break;
        case AUTO_CLEAN:
          fateMutator.putAutoClean(serialized);
          break;
        case EXCEPTION:
          fateMutator.putException(serialized);
          break;
        case RETURN_VALUE:
          fateMutator.putReturnValue(serialized);
          break;
        default:
          throw new IllegalArgumentException("Unexpected TxInfo type " + txInfo);
      }

      fateMutator.mutate();
    }

    @Override
    public void delete() {
      verifyReserved(true);

      newMutator(tid).delete().mutate();
    }

    private Optional<Integer> findTop() {
      return scanTx(scanner -> {
        scanner.setRange(getRow(tid));
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
