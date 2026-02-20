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

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.Fate.FateOperation;
import org.apache.accumulo.core.util.Pair;

/**
 * Transient in memory store for transactions.
 */
public class TestStore implements FateStore<String> {

  private final Map<FateId,Pair<TStatus,Optional<FateKey>>> statuses = new HashMap<>();
  private final Map<FateId,Map<Fate.TxInfo,Serializable>> txInfos = new HashMap<>();
  private final Set<FateId> reserved = new HashSet<>();
  private static final FateInstanceType fateInstanceType = FateInstanceType.USER;

  @Override
  public FateId create() {
    FateId fateId = FateId.from(fateInstanceType, UUID.randomUUID());
    statuses.put(fateId, new Pair<>(TStatus.NEW, Optional.empty()));
    return fateId;
  }

  @Override
  public Seeder<String> beginSeeding() {
    return new Seeder<>() {
      @Override
      public CompletableFuture<Optional<FateId>> attemptToSeedTransaction(FateOperation fateOp,
          FateKey fateKey, Repo<String> repo, boolean autoCleanUp) {
        return CompletableFuture.completedFuture(Optional.empty());
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public boolean seedTransaction(Fate.FateOperation fateOp, FateId fateId, Repo<String> repo,
      boolean autoCleanUp) {
    return false;
  }

  @Override
  public FateTxStore<String> reserve(FateId fateId) {
    if (reserved.contains(fateId)) {
      // other fate stores would wait, but do not expect test to reserve
      throw new IllegalStateException();
    }
    // twice... if test change, then change this
    reserved.add(fateId);
    return new TestFateTxStore(fateId);
  }

  @Override
  public Optional<FateTxStore<String>> tryReserve(FateId fateId) {
    synchronized (this) {
      if (!reserved.contains(fateId)) {
        reserve(fateId);
        return Optional.of(new TestFateTxStore(fateId));
      }
      return Optional.empty();
    }
  }

  @Override
  public Map<FateId,FateReservation> getActiveReservations() {
    // This method only makes sense for the FateStores that don't store their reservations in memory
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteDeadReservations() {
    // This method only makes sense for the FateStores that don't store their reservations in memory
    throw new UnsupportedOperationException();
  }

  private class TestFateTxStore implements FateTxStore<String> {

    private final FateId fateId;

    TestFateTxStore(FateId fateId) {
      this.fateId = fateId;
    }

    @Override
    public Repo<String> top() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<ReadOnlyRepo<String>> getStack() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TStatus getStatus() {
      if (!reserved.contains(fateId)) {
        throw new IllegalStateException();
      }

      Pair<TStatus,Optional<FateKey>> status = statuses.get(fateId);
      if (status == null) {
        return TStatus.UNKNOWN;
      }
      return status.getFirst();
    }

    @Override
    public Optional<FateKey> getKey() {
      if (!reserved.contains(fateId)) {
        throw new IllegalStateException();
      }

      Pair<TStatus,Optional<FateKey>> status = statuses.get(fateId);
      if (status == null) {
        return Optional.empty();
      }
      return status.getSecond();
    }

    @Override
    public TStatus waitForStatusChange(EnumSet<TStatus> expected) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Serializable getTransactionInfo(Fate.TxInfo txInfo) {
      var submap = txInfos.get(fateId);
      if (submap == null) {
        return null;
      }

      return submap.get(txInfo);
    }

    @Override
    public long timeCreated() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FateId getID() {
      return fateId;
    }

    @Override
    public void push(Repo<String> repo) throws StackOverflowException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void pop() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setStatus(TStatus status) {
      if (!reserved.contains(fateId)) {
        throw new IllegalStateException();
      }
      Pair<TStatus,Optional<FateKey>> currentStatus = statuses.get(fateId);
      if (currentStatus == null) {
        throw new IllegalStateException();
      }
      statuses.put(fateId, new Pair<>(status, currentStatus.getSecond()));
    }

    @Override
    public void setTransactionInfo(Fate.TxInfo txInfo, Serializable val) {
      if (!reserved.contains(fateId)) {
        throw new IllegalStateException();
      }

      txInfos.computeIfAbsent(fateId, t -> new HashMap<>()).put(txInfo, val);
    }

    @Override
    public void delete() {
      if (!reserved.contains(fateId)) {
        throw new IllegalStateException();
      }
      statuses.remove(fateId);
    }

    @Override
    public void forceDelete() {
      throw new UnsupportedOperationException(
          this.getClass().getSimpleName() + " should not be calling forceDelete()");
    }

    @Override
    public void unreserve(Duration deferTime) {
      if (!reserved.remove(fateId)) {
        throw new IllegalStateException();
      }
    }
  }

  @Override
  public ReadOnlyFateTxStore<String> read(FateId fateId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Stream<FateIdStatus> list() {
    return new ArrayList<>(statuses.entrySet()).stream().map(e -> new FateIdStatus() {
      @Override
      public FateId getFateId() {
        return e.getKey();
      }

      @Override
      public TStatus getStatus() {
        return e.getValue().getFirst();
      }

      @Override
      public Optional<FateReservation> getFateReservation() {
        throw new UnsupportedOperationException(
            "Only the 'reserved' set should be used for reservations in the test store");
      }

      @Override
      public Optional<Fate.FateOperation> getFateOperation() {
        throw new UnsupportedOperationException("Test not configured or expected to be calling "
            + "this method. Functionality can be added if needed.");
      }
    });
  }

  @Override
  public Stream<FateIdStatus> list(EnumSet<TStatus> statuses) {
    return list().filter(fis -> statuses.contains(fis.getStatus()));
  }

  @Override
  public Stream<FateKey> list(FateKey.FateKeyType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void runnable(BooleanSupplier keepWaiting, Consumer<FateIdStatus> idConsumer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getDeferredCount() {
    return 0;
  }

  @Override
  public FateInstanceType type() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDeferredOverflow() {
    return false;
  }

  @Override
  public void close() {
    // no-op
  }
}
