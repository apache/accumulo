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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.FateIdStatus;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.ReadOnlyFateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.util.Pair;

/**
 * Transient in memory store for transactions.
 */
public class TestStore implements FateStore<String> {

  private long nextId = 1;
  private final Map<FateId,Pair<TStatus,Optional<FateKey>>> statuses = new HashMap<>();
  private final Map<FateId,Map<Fate.TxInfo,Serializable>> txInfos = new HashMap<>();
  private final Set<FateId> reserved = new HashSet<>();
  private static final FateInstanceType fateInstanceType = FateInstanceType.USER;

  @Override
  public FateId create() {
    FateId fateId = FateId.from(fateInstanceType, nextId++);
    statuses.put(fateId, new Pair<>(TStatus.NEW, Optional.empty()));
    return fateId;
  }

  @Override
  public Optional<FateTxStore<String>> createAndReserve(FateKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FateTxStore<String> reserve(FateId fateId) {
    if (reserved.contains(fateId)) {
      throw new IllegalStateException(); // zoo store would wait, but do not expect test to reserve
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
      return getStatusAndKey().getFirst();
    }

    @Override
    public Optional<FateKey> getKey() {
      return getStatusAndKey().getSecond();
    }

    @Override
    public Pair<TStatus,Optional<FateKey>> getStatusAndKey() {
      if (!reserved.contains(fateId)) {
        throw new IllegalStateException();
      }

      Pair<TStatus,Optional<FateKey>> status = statuses.get(fateId);
      if (status == null) {
        return new Pair<>(TStatus.UNKNOWN, Optional.empty());
      }

      return status;
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
    public void unreserve(long deferTime, TimeUnit timeUnit) {
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
    });
  }

  @Override
  public void runnable(AtomicBoolean keepWaiting, Consumer<FateId> idConsumer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getDeferredCount() {
    return 0;
  }

  @Override
  public boolean isDeferredOverflow() {
    return false;
  }
}
