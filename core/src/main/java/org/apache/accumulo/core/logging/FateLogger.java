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
package org.apache.accumulo.core.logging;

import static org.apache.accumulo.core.fate.FateTxId.formatTid;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FatesStore;
import org.apache.accumulo.core.fate.ReadOnlyFatesStore;
import org.apache.accumulo.core.fate.ReadOnlyRepo;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.StackOverflowException;
import org.apache.accumulo.core.manager.PartitionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FateLogger {
  private static final String PREFIX = Logging.PREFIX + "fate.";

  // Logs all mutations to FATEs persistent storage. Enabling this logger could help debug
  // reproducible problems with FATE transactions.
  private static final Logger storeLog = LoggerFactory.getLogger(PREFIX + "store");

  private static class LoggingFateStore<T> implements FatesStore.FateStore<T> {

    private final FatesStore.FateStore<T> wrapped;
    private final Function<Repo<T>,String> toLogString;

    private LoggingFateStore(FatesStore.FateStore<T> wrapped,
        Function<Repo<T>,String> toLogString) {
      this.wrapped = wrapped;
      this.toLogString = toLogString;
    }

    @Override
    public List<ReadOnlyRepo<T>> getStack() {
      return wrapped.getStack();
    }

    @Override
    public ReadOnlyFatesStore.FateStatus getStatus() {
      return wrapped.getStatus();
    }

    @Override
    public ReadOnlyFatesStore.FateStatus
        waitForStatusChange(EnumSet<ReadOnlyFatesStore.FateStatus> expected) {
      return wrapped.waitForStatusChange(expected);
    }

    @Override
    public Serializable getTransactionInfo(Fate.TxInfo txInfo) {
      return wrapped.getTransactionInfo(txInfo);
    }

    @Override
    public long timeCreated() {
      return wrapped.timeCreated();
    }

    @Override
    public long getID() {
      return wrapped.getID();
    }

    @Override
    public Repo<T> top() {
      return wrapped.top();
    }

    @Override
    public void push(Repo<T> repo) throws StackOverflowException {
      wrapped.push(repo);
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} pushed {}", formatTid(wrapped.getID()), toLogString.apply(repo));
      }
    }

    @Override
    public void pop() {
      wrapped.pop();
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} popped", formatTid(wrapped.getID()));
      }
    }

    @Override
    public void setStatus(ReadOnlyFatesStore.FateStatus status) {
      wrapped.setStatus(status);
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} setStatus to {}", formatTid(wrapped.getID()), status);
      }
    }

    @Override
    public void setTransactionInfo(Fate.TxInfo txInfo, Serializable val) {
      wrapped.setTransactionInfo(txInfo, val);
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} setting {} to {}", formatTid(wrapped.getID()), txInfo, val);
      }
    }

    @Override
    public void delete() {
      wrapped.delete();
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} deleted fate transaction", formatTid(wrapped.getID()));
      }
    }

    @Override
    public void unreserve(long deferTime) {
      wrapped.unreserve(deferTime);
    }
  }

  public static <T> FatesStore<T> wrap(FatesStore<T> store, Function<Repo<T>,String> toLogString) {

    // only logging operations that change the persisted data, not operations that only read data
    return new FatesStore<>() {
      @Override
      public FateStore<T> reserve(long tid) {
        return new LoggingFateStore<>(store.reserve(tid), toLogString);
      }

      @Override
      public Optional<FateStore<T>> tryReserve(long tid) {
        return store.tryReserve(tid).map(fos -> new LoggingFateStore<>(fos, toLogString));
      }

      @Override
      public ReadOnlyFateStore<T> read(long tid) {
        return store.read(tid);
      }

      @Override
      public List<Long> list() {
        return store.list();
      }

      @Override
      public Iterator<Long> runnable(PartitionData partitionData) {
        return store.runnable(partitionData);
      }

      @Override
      public long create() {
        long tid = store.create();
        if (storeLog.isTraceEnabled()) {
          storeLog.trace("{} created fate transaction", formatTid(tid));
        }
        return tid;
      }
    };
  }
}
