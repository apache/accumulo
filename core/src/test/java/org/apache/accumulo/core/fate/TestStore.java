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
import java.util.function.LongConsumer;
import java.util.stream.Stream;

/**
 * Transient in memory store for transactions.
 */
public class TestStore implements FateStore<String> {

  private long nextId = 1;
  private Map<Long,TStatus> statuses = new HashMap<>();
  private Map<Long,Map<Fate.TxInfo,Serializable>> txInfos = new HashMap<>();
  private Set<Long> reserved = new HashSet<>();

  @Override
  public long create() {
    statuses.put(nextId, TStatus.NEW);
    return nextId++;
  }

  @Override
  public FateTxStore<String> reserve(long tid) {
    if (reserved.contains(tid)) {
      throw new IllegalStateException(); // zoo store would wait, but do not expect test to reserve
    }
    // twice... if test change, then change this
    reserved.add(tid);
    return new TestFateTxStore(tid);
  }

  @Override
  public Optional<FateTxStore<String>> tryReserve(long tid) {
    synchronized (this) {
      if (!reserved.contains(tid)) {
        reserve(tid);
        return Optional.of(new TestFateTxStore(tid));
      }
      return Optional.empty();
    }
  }

  private class TestFateTxStore implements FateTxStore<String> {

    private final long tid;

    TestFateTxStore(long tid) {
      this.tid = tid;
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
      if (!reserved.contains(tid)) {
        throw new IllegalStateException();
      }

      TStatus status = statuses.get(tid);
      if (status == null) {
        return TStatus.UNKNOWN;
      }
      return status;
    }

    @Override
    public TStatus waitForStatusChange(EnumSet<TStatus> expected) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Serializable getTransactionInfo(Fate.TxInfo txInfo) {
      var submap = txInfos.get(tid);
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
    public long getID() {
      return tid;
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
      if (!reserved.contains(tid)) {
        throw new IllegalStateException();
      }
      if (!statuses.containsKey(tid)) {
        throw new IllegalStateException();
      }
      statuses.put(tid, status);
    }

    @Override
    public void setTransactionInfo(Fate.TxInfo txInfo, Serializable val) {
      if (!reserved.contains(tid)) {
        throw new IllegalStateException();
      }

      txInfos.computeIfAbsent(tid, t -> new HashMap<>()).put(txInfo, val);
    }

    @Override
    public void delete() {
      if (!reserved.contains(tid)) {
        throw new IllegalStateException();
      }
      statuses.remove(tid);
    }

    @Override
    public void unreserve(long deferTime, TimeUnit timeUnit) {
      if (!reserved.remove(tid)) {
        throw new IllegalStateException();
      }
    }
  }

  @Override
  public ReadOnlyFateTxStore<String> read(long tid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Stream<FateIdStatus> list() {
    return new ArrayList<>(statuses.entrySet()).stream().map(e -> new FateIdStatus() {
      @Override
      public long getTxid() {
        return e.getKey();
      }

      @Override
      public TStatus getStatus() {
        return e.getValue();
      }
    });
  }

  @Override
  public void runnable(AtomicBoolean keepWaiting, LongConsumer idConsumer) {
    throw new UnsupportedOperationException();
  }

}
