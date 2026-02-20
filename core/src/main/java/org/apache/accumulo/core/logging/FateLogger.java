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

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.Fate.FateOperation;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.FateStore.Seeder;
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

    private LoggingFateTxStore(FateTxStore<T> wrapped, Function<Repo<T>,String> toLogString,
        boolean allowForceDel) {
      super(wrapped, allowForceDel);
      this.toLogString = toLogString;
    }

    @Override
    public void push(Repo<T> repo) throws StackOverflowException {
      super.push(repo);
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} pushed {}", getID(), toLogString.apply(repo));
      }
    }

    @Override
    public void pop() {
      super.pop();
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} popped", getID());
      }
    }

    @Override
    public void setStatus(ReadOnlyFateStore.TStatus status) {
      super.setStatus(status);
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} setStatus to {}", getID(), status);
      }
    }

    @Override
    public void setTransactionInfo(Fate.TxInfo txInfo, Serializable val) {
      super.setTransactionInfo(txInfo, val);
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} setting {} to {}", getID(), txInfo, val);
      }
    }

    @Override
    public void delete() {
      super.delete();
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("{} deleted fate transaction", getID());
      }
    }
  }

  public static <T> FateStore<T> wrap(FateStore<T> store, Function<Repo<T>,String> toLogString,
      boolean allowForceDel) {

    // only logging operations that change the persisted data, not operations that only read data
    return new FateStore<>() {

      @Override
      public FateTxStore<T> reserve(FateId fateId) {
        return new LoggingFateTxStore<>(store.reserve(fateId), toLogString, allowForceDel);
      }

      @Override
      public Optional<FateTxStore<T>> tryReserve(FateId fateId) {
        return store.tryReserve(fateId)
            .map(ftxs -> new LoggingFateTxStore<>(ftxs, toLogString, allowForceDel));
      }

      @Override
      public ReadOnlyFateTxStore<T> read(FateId fateId) {
        return store.read(fateId);
      }

      @Override
      public Stream<FateIdStatus> list() {
        return store.list();
      }

      @Override
      public Stream<FateIdStatus> list(EnumSet<TStatus> statuses) {
        return store.list(statuses);
      }

      @Override
      public Stream<FateKey> list(FateKey.FateKeyType type) {
        return store.list(type);
      }

      @Override
      public void runnable(BooleanSupplier keepWaiting, Consumer<FateIdStatus> idConsumer) {
        store.runnable(keepWaiting, idConsumer);
      }

      @Override
      public FateId create() {
        FateId fateId = store.create();
        if (storeLog.isTraceEnabled()) {
          storeLog.trace("{} created fate transaction", fateId);
        }
        return fateId;
      }

      @Override
      public Seeder<T> beginSeeding() {
        return new SeederLogger<>(store, toLogString);
      }

      @Override
      public boolean seedTransaction(Fate.FateOperation fateOp, FateId fateId, Repo<T> repo,
          boolean autoCleanUp) {
        boolean seeded = store.seedTransaction(fateOp, fateId, repo, autoCleanUp);
        if (storeLog.isTraceEnabled()) {
          storeLog.trace("{} {} {} {}", fateId, seeded ? "seeded" : "unable to seed",
              toLogString.apply(repo), autoCleanUp);
        }
        return seeded;
      }

      @Override
      public int getDeferredCount() {
        return store.getDeferredCount();
      }

      @Override
      public FateInstanceType type() {
        return store.type();
      }

      @Override
      public boolean isDeferredOverflow() {
        return store.isDeferredOverflow();
      }

      @Override
      public Map<FateId,FateReservation> getActiveReservations() {
        return store.getActiveReservations();
      }

      @Override
      public void deleteDeadReservations() {
        store.deleteDeadReservations();
      }

      @Override
      public void close() {
        store.close();
      }
    };
  }

  public static class SeederLogger<T> implements Seeder<T> {
    private final FateStore<T> store;
    private final Seeder<T> seeder;
    private final Function<Repo<T>,String> toLogString;

    public SeederLogger(FateStore<T> store, Function<Repo<T>,String> toLogString) {
      this.store = Objects.requireNonNull(store);
      this.seeder = store.beginSeeding();
      this.toLogString = Objects.requireNonNull(toLogString);
    }

    @Override
    public CompletableFuture<Optional<FateId>> attemptToSeedTransaction(FateOperation fateOp,
        FateKey fateKey, Repo<T> repo, boolean autoCleanUp) {
      var future = this.seeder.attemptToSeedTransaction(fateOp, fateKey, repo, autoCleanUp);
      return future.whenComplete((optional, throwable) -> {
        if (storeLog.isTraceEnabled()) {
          optional.ifPresentOrElse(fateId -> {
            storeLog.trace("{} seeded {} {} {}", fateId, fateKey, toLogString.apply(repo),
                autoCleanUp);
          }, () -> {
            storeLog.trace("Possibly unable to seed {} {} {}", fateKey, toLogString.apply(repo),
                autoCleanUp);
          });
        }
      });
    }

    @Override
    public void close() {
      seeder.close();
      if (storeLog.isTraceEnabled()) {
        storeLog.trace("attempted to close seeder for {}", store.type());
      }
    }
  }
}
