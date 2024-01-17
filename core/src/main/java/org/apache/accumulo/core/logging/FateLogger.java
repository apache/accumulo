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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.StackOverflowException;
import org.apache.accumulo.core.fate.WrappedFateTxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FateLogger {
  private static final String PREFIX = Logging.PREFIX + "fate.";

  // Logs all mutations to FATEs persistent storage. Enabling this logger could help debug
  // reproducible problems with FATE transactions.
  private static final Logger storeLog = LoggerFactory.getLogger(PREFIX + "store");

  private static class LoggingFateTxStore<T> extends WrappedFateTxStore<T> {

    private final Function<Repo<T>,String> toLogString;

    private LoggingFateTxStore(FateTxStore<T> wrapped, Function<Repo<T>,String> toLogString) {
      super(wrapped);
      this.toLogString = toLogString;
    }

    @Override
    public void push(Repo<T> repo) throws StackOverflowException {
      super.push(repo);
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} pushed {}", formatTid(getID()), toLogString.apply(repo));
      }
    }

    @Override
    public void pop() {
      super.pop();
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} popped", formatTid(getID()));
      }
    }

    @Override
    public void setStatus(ReadOnlyFateStore.TStatus status) {
      super.setStatus(status);
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} setStatus to {}", formatTid(getID()), status);
      }
    }

    @Override
    public void setTransactionInfo(Fate.TxInfo txInfo, Serializable val) {
      super.setTransactionInfo(txInfo, val);
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} setting {} to {}", formatTid(getID()), txInfo, val);
      }
    }

    @Override
    public void delete() {
      super.delete();
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} deleted fate transaction", formatTid(getID()));
      }
    }
  }

  public static <T> FateStore<T> wrap(FateStore<T> store, Function<Repo<T>,String> toLogString) {

    // only logging operations that change the persisted data, not operations that only read data
    return new FateStore<>() {

      @Override
      public FateTxStore<T> reserve(long tid) {
        return new LoggingFateTxStore<>(store.reserve(tid), toLogString);
      }

      @Override
      public Optional<FateTxStore<T>> tryReserve(long tid) {
        return store.tryReserve(tid).map(ftxs -> new LoggingFateTxStore<>(ftxs, toLogString));
      }

      @Override
      public ReadOnlyFateTxStore<T> read(long tid) {
        return store.read(tid);
      }

      @Override
      public Stream<FateIdStatus> list() {
        return store.list();
      }

      @Override
      public void runnable(AtomicBoolean keepWaiting, LongConsumer idConsumer) {
        store.runnable(keepWaiting, idConsumer);
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
