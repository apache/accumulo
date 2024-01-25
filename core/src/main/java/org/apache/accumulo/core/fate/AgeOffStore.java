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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This store removes Repos, in the store it wraps, that are in a finished or new state for more
 * than a configurable time period.
 *
 * No external time source is used. It starts tracking idle time when its created.
 */
public class AgeOffStore<T> implements FateStore<T> {

  public interface TimeSource {
    long currentTimeMillis();
  }

  private static final Logger log = LoggerFactory.getLogger(AgeOffStore.class);

  private final FateStore<T> store;
  private Map<FateId,Long> candidates;
  private long ageOffTime;
  private long minTime;
  private TimeSource timeSource;

  private synchronized void updateMinTime() {
    minTime = Long.MAX_VALUE;

    for (Long time : candidates.values()) {
      if (time < minTime) {
        minTime = time;
      }
    }
  }

  private synchronized void addCandidate(FateId fateId) {
    long time = timeSource.currentTimeMillis();
    candidates.put(fateId, time);
    if (time < minTime) {
      minTime = time;
    }
  }

  private synchronized void removeCandidate(FateId fateId) {
    Long time = candidates.remove(fateId);
    if (time != null && time <= minTime) {
      updateMinTime();
    }
  }

  public void ageOff() {
    HashSet<FateId> oldTxs = new HashSet<>();

    synchronized (this) {
      long time = timeSource.currentTimeMillis();
      if (minTime < time && time - minTime >= ageOffTime) {
        for (Entry<FateId,Long> entry : candidates.entrySet()) {
          if (time - entry.getValue() >= ageOffTime) {
            oldTxs.add(entry.getKey());
          }
        }

        candidates.keySet().removeAll(oldTxs);
        updateMinTime();
      }
    }

    for (FateId oldTx : oldTxs) {
      try {
        FateTxStore<T> txStore = store.reserve(oldTx);
        try {
          switch (txStore.getStatus()) {
            case NEW:
            case FAILED:
            case SUCCESSFUL:
              txStore.delete();
              log.debug("Aged off FATE tx {}", oldTx);
              break;
            default:
              break;
          }

        } finally {
          txStore.unreserve(0, TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        log.warn("Failed to age off FATE tx " + oldTx, e);
      }
    }
  }

  public AgeOffStore(FateStore<T> store, long ageOffTime, TimeSource timeSource) {
    this.store = store;
    this.ageOffTime = ageOffTime;
    this.timeSource = timeSource;
    candidates = new HashMap<>();

    minTime = Long.MAX_VALUE;

    // ELASTICITY_TODO need to rework how this class works so that it does not buffer everything in
    // memory.
    List<FateId> fateIds = store.list().collect(Collectors.toList());
    for (FateId fateId : fateIds) {
      FateTxStore<T> txStore = store.reserve(fateId);
      try {
        switch (txStore.getStatus()) {
          case NEW:
          case FAILED:
          case SUCCESSFUL:
            addCandidate(fateId);
            break;
          default:
            break;
        }
      } finally {
        txStore.unreserve(0, TimeUnit.MILLISECONDS);
      }
    }
  }

  @Override
  public FateId create() {
    FateId fateId = store.create();
    addCandidate(fateId);
    return fateId;
  }

  @Override
  public FateTxStore<T> reserve(FateId fateId) {
    return new AgeOffFateTxStore(store.reserve(fateId));
  }

  @Override
  public Optional<FateTxStore<T>> tryReserve(FateId fateId) {
    return store.tryReserve(fateId).map(AgeOffFateTxStore::new);
  }

  private class AgeOffFateTxStore extends WrappedFateTxStore<T> {

    private AgeOffFateTxStore(FateTxStore<T> wrapped) {
      super(wrapped);
    }

    @Override
    public void setStatus(FateStore.TStatus status) {
      super.setStatus(status);

      switch (status) {
        case SUBMITTED:
        case IN_PROGRESS:
        case FAILED_IN_PROGRESS:
          removeCandidate(getID());
          break;
        case FAILED:
        case SUCCESSFUL:
          addCandidate(getID());
          break;
        default:
          break;
      }
    }

    @Override
    public void delete() {
      super.delete();
      removeCandidate(getID());
    }
  }

  @Override
  public ReadOnlyFateTxStore<T> read(FateId fateId) {
    return store.read(fateId);
  }

  @Override
  public Stream<FateId> list() {
    return store.list();
  }

  @Override
  public void runnable(AtomicBoolean keepWaiting, Consumer<FateId> idConsumer) {
    store.runnable(keepWaiting, idConsumer);
  }
}
