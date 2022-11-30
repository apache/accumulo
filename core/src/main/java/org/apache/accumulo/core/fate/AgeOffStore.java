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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This store removes Repos, in the store it wraps, that are in a finished or new state for more
 * than a configurable time period.
 *
 * No external time source is used. It starts tracking idle time when its created.
 */
public class AgeOffStore<T> implements TStore<T> {

  public interface TimeSource {
    long currentTimeMillis();
  }

  private static final Logger log = LoggerFactory.getLogger(AgeOffStore.class);

  private final ZooStore<T> store;
  private Map<Long,Long> candidates;
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

  private synchronized void addCandidate(long txid) {
    long time = timeSource.currentTimeMillis();
    candidates.put(txid, time);
    if (time < minTime) {
      minTime = time;
    }
  }

  private synchronized void removeCandidate(long txid) {
    Long time = candidates.remove(txid);
    if (time != null && time <= minTime) {
      updateMinTime();
    }
  }

  public void ageOff() {
    HashSet<Long> oldTxs = new HashSet<>();

    synchronized (this) {
      long time = timeSource.currentTimeMillis();
      if (minTime < time && time - minTime >= ageOffTime) {
        for (Entry<Long,Long> entry : candidates.entrySet()) {
          if (time - entry.getValue() >= ageOffTime) {
            oldTxs.add(entry.getKey());
          }
        }

        candidates.keySet().removeAll(oldTxs);
        updateMinTime();
      }
    }

    for (Long txid : oldTxs) {
      try {
        store.reserve(txid);
        try {
          switch (store.getStatus(txid)) {
            case NEW:
            case FAILED:
            case SUCCESSFUL:
              store.delete(txid);
              log.debug("Aged off FATE tx {}", FateTxId.formatTid(txid));
              break;
            default:
              break;
          }

        } finally {
          store.unreserve(txid, 0);
        }
      } catch (Exception e) {
        log.warn("Failed to age off FATE tx " + FateTxId.formatTid(txid), e);
      }
    }
  }

  public AgeOffStore(ZooStore<T> store, long ageOffTime, TimeSource timeSource) {
    this.store = store;
    this.ageOffTime = ageOffTime;
    this.timeSource = timeSource;
    candidates = new HashMap<>();

    minTime = Long.MAX_VALUE;

    List<Long> txids = store.list();
    for (Long txid : txids) {
      store.reserve(txid);
      try {
        switch (store.getStatus(txid)) {
          case NEW:
          case FAILED:
          case SUCCESSFUL:
            addCandidate(txid);
            break;
          default:
            break;
        }
      } finally {
        store.unreserve(txid, 0);
      }
    }
  }

  @Override
  public long create() {
    long txid = store.create();
    addCandidate(txid);
    return txid;
  }

  @Override
  public long reserve() {
    return store.reserve();
  }

  @Override
  public void reserve(long tid) {
    store.reserve(tid);
  }

  @Override
  public boolean tryReserve(long tid) {
    return store.tryReserve(tid);
  }

  @Override
  public void unreserve(long tid, long deferTime) {
    store.unreserve(tid, deferTime);
  }

  @Override
  public Repo<T> top(long tid) {
    return store.top(tid);
  }

  @Override
  public void push(long tid, Repo<T> repo) throws StackOverflowException {
    store.push(tid, repo);
  }

  @Override
  public void pop(long tid) {
    store.pop(tid);
  }

  @Override
  public org.apache.accumulo.core.fate.TStore.TStatus getStatus(long tid) {
    return store.getStatus(tid);
  }

  @Override
  public void setStatus(long tid, org.apache.accumulo.core.fate.TStore.TStatus status) {
    store.setStatus(tid, status);

    switch (status) {
      case SUBMITTED:
      case IN_PROGRESS:
      case FAILED_IN_PROGRESS:
        removeCandidate(tid);
        break;
      case FAILED:
      case SUCCESSFUL:
        addCandidate(tid);
        break;
      default:
        break;
    }
  }

  @Override
  public org.apache.accumulo.core.fate.TStore.TStatus waitForStatusChange(long tid,
      EnumSet<org.apache.accumulo.core.fate.TStore.TStatus> expected) {
    return store.waitForStatusChange(tid, expected);
  }

  @Override
  public void setTransactionInfo(long tid, Fate.TxInfo txInfo, Serializable val) {
    store.setTransactionInfo(tid, txInfo, val);
  }

  @Override
  public Serializable getTransactionInfo(long tid, Fate.TxInfo txInfo) {
    return store.getTransactionInfo(tid, txInfo);
  }

  @Override
  public void delete(long tid) {
    store.delete(tid);
    removeCandidate(tid);
  }

  @Override
  public List<Long> list() {
    return store.list();
  }

  @Override
  public long timeCreated(long tid) {
    return store.timeCreated(tid);
  }

  @Override
  public List<ReadOnlyRepo<T>> getStack(long tid) {
    return store.getStack(tid);
  }
}
