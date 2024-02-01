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

import static org.apache.accumulo.core.fate.AbstractFateStore.serialize;
import static org.apache.accumulo.core.fate.accumulo.AccumuloStore.getRow;
import static org.apache.accumulo.core.fate.accumulo.AccumuloStore.invertRepo;

import java.util.Objects;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.accumulo.schema.FateSchema.RepoColumnFamily;
import org.apache.accumulo.core.fate.accumulo.schema.FateSchema.TxColumnFamily;
import org.apache.accumulo.core.fate.accumulo.schema.FateSchema.TxInfoColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

public class FateMutatorImpl<T> implements FateMutator<T> {

  private final ClientContext context;
  private final String tableName;
  private final FateId fateId;
  private final ConditionalMutation mutation;

  public FateMutatorImpl(ClientContext context, String tableName, FateId fateId) {
    this.context = Objects.requireNonNull(context);
    this.tableName = Objects.requireNonNull(tableName);
    this.fateId = fateId;
    this.mutation = new ConditionalMutation(new Text("tx_" + fateId.getHexTid()));
  }

  @Override
  public FateMutator<T> putStatus(TStatus status) {
    TxColumnFamily.STATUS_COLUMN.put(mutation, new Value(status.name()));
    return this;
  }

  @Override
  public FateMutator<T> putCreateTime(long ctime) {
    TxColumnFamily.CREATE_TIME_COLUMN.put(mutation, new Value(Long.toString(ctime)));
    return this;
  }

  @Override
  public FateMutator<T> putName(byte[] data) {
    TxInfoColumnFamily.TX_NAME_COLUMN.put(mutation, new Value(data));
    return this;
  }

  @Override
  public FateMutator<T> putAutoClean(byte[] data) {
    TxInfoColumnFamily.AUTO_CLEAN_COLUMN.put(mutation, new Value(data));
    return this;
  }

  @Override
  public FateMutator<T> putException(byte[] data) {
    TxInfoColumnFamily.EXCEPTION_COLUMN.put(mutation, new Value(data));
    return this;
  }

  @Override
  public FateMutator<T> putReturnValue(byte[] data) {
    TxInfoColumnFamily.RETURN_VALUE_COLUMN.put(mutation, new Value(data));
    return this;
  }

  @Override
  public FateMutator<T> putAgeOff(byte[] data) {
    TxInfoColumnFamily.TX_AGEOFF_COLUMN.put(mutation, new Value(data));
    return this;
  }

  @Override
  public FateMutator<T> putTxInfo(TxInfo txInfo, byte[] data) {
    switch (txInfo) {
      case TX_NAME:
        putName(data);
        break;
      case AUTO_CLEAN:
        putAutoClean(data);
        break;
      case EXCEPTION:
        putException(data);
        break;
      case RETURN_VALUE:
        putReturnValue(data);
        break;
      case TX_AGEOFF:
        putAgeOff(data);
        break;
      default:
        throw new IllegalArgumentException("Unexpected TxInfo type: " + txInfo);
    }
    return this;
  }

  @Override
  public FateMutator<T> putRepo(int position, Repo<T> repo) {
    final Text cq = invertRepo(position);
    // ensure this repo is not already set
    mutation.addCondition(new Condition(RepoColumnFamily.NAME, cq));
    mutation.put(RepoColumnFamily.NAME, cq, new Value(serialize(repo)));
    return this;
  }

  @Override
  public FateMutator<T> deleteRepo(int position) {
    mutation.putDelete(RepoColumnFamily.NAME, invertRepo(position));
    return this;
  }

  public FateMutator<T> delete() {
    try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setRange(getRow(fateId));
      scanner.forEach(
          (key, value) -> mutation.putDelete(key.getColumnFamily(), key.getColumnQualifier()));
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(tableName + " not found!", e);
    }
    return this;
  }

  /**
   * Require that the transaction status is one of the given statuses. If no statuses are provided,
   * require that the status column is absent.
   *
   * @param statuses The statuses to check against.
   */
  public FateMutator<T> requireStatus(TStatus... statuses) {
    Condition condition = StatusMappingIterator.createCondition(statuses);
    mutation.addCondition(condition);
    return this;
  }

  @Override
  public void mutate() {
    var status = tryMutate();
    if (status != Status.ACCEPTED) {
      throw new IllegalStateException("Failed to write mutation " + status + " " + mutation);
    }
  }

  @Override
  public Status tryMutate() {
    try {
      // if there are no conditions attached, then we can use a batch writer
      if (mutation.getConditions().isEmpty()) {
        try (BatchWriter writer = context.createBatchWriter(tableName)) {
          writer.addMutation(mutation);
        } catch (MutationsRejectedException e) {
          throw new RuntimeException(e);
        }

        return Status.ACCEPTED;
      } else {
        try (ConditionalWriter writer = context.createConditionalWriter(tableName)) {
          ConditionalWriter.Result result = writer.write(mutation);

          switch (result.getStatus()) {
            case ACCEPTED:
              return Status.ACCEPTED;
            case REJECTED:
              return Status.REJECTED;
            case UNKNOWN:
              return Status.UNKNOWN;
            default:
              // do not expect other statuses
              throw new IllegalStateException(
                  "Unhandled status for mutation " + result.getStatus());
          }

        } catch (AccumuloException | AccumuloSecurityException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
