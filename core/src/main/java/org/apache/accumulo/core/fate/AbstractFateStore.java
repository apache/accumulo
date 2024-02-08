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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class AbstractFateStore<T> implements FateStore<T> {

  private static final Logger log = LoggerFactory.getLogger(AbstractFateStore.class);

  // Default maximum size of 100,000 transactions before deferral is stopped and
  // all existing transactions are processed immediately again
  protected static final int DEFAULT_MAX_DEFERRED = 100_000;

  protected final Set<FateId> reserved;
  protected final Map<FateId,Long> deferred;
  private final int maxDeferred;
  private final AtomicBoolean deferredOverflow = new AtomicBoolean();

  // This is incremented each time a transaction was unreserved that was non new
  protected final SignalCount unreservedNonNewCount = new SignalCount();

  // This is incremented each time a transaction is unreserved that was runnable
  protected final SignalCount unreservedRunnableCount = new SignalCount();

  public AbstractFateStore() {
    this(DEFAULT_MAX_DEFERRED);
  }

  public AbstractFateStore(int maxDeferred) {
    this.maxDeferred = maxDeferred;
    this.reserved = new HashSet<>();
    this.deferred = new HashMap<>();
  }

  public static byte[] serialize(Object o) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(o);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressFBWarnings(value = "OBJECT_DESERIALIZATION",
      justification = "unsafe to store arbitrary serialized objects like this, but needed for now"
          + " for backwards compatibility")
  public static Object deserialize(byte[] ser) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(ser);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      return ois.readObject();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Attempt to reserve the fate transaction.
   *
   * @param fateId The FateId
   * @return An Optional containing the FateTxStore if the transaction was successfully reserved, or
   *         an empty Optional if the transaction was already reserved.
   */
  @Override
  public Optional<FateTxStore<T>> tryReserve(FateId fateId) {
    synchronized (this) {
      if (!reserved.contains(fateId)) {
        return Optional.of(reserve(fateId));
      }
      return Optional.empty();
    }
  }

  @Override
  public FateTxStore<T> reserve(FateId fateId) {
    synchronized (AbstractFateStore.this) {
      while (reserved.contains(fateId)) {
        try {
          AbstractFateStore.this.wait(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }

      reserved.add(fateId);
      return newFateTxStore(fateId, true);
    }
  }

  @Override
  public void runnable(AtomicBoolean keepWaiting, Consumer<FateId> idConsumer) {

    AtomicLong seen = new AtomicLong(0);

    while (keepWaiting.get() && seen.get() == 0) {
      final long beforeCount = unreservedRunnableCount.getCount();
      final boolean beforeDeferredOverflow = deferredOverflow.get();

      try (Stream<FateIdStatus> transactions = getTransactions()) {
        transactions.filter(fateIdStatus -> isRunnable(fateIdStatus.getStatus()))
            .map(FateIdStatus::getFateId).filter(fateId -> {
              synchronized (AbstractFateStore.this) {
                var deferredTime = deferred.get(fateId);
                if (deferredTime != null) {
                  if ((deferredTime - System.nanoTime()) >= 0) {
                    return false;
                  } else {
                    deferred.remove(fateId);
                  }
                }
                return !reserved.contains(fateId);
              }
            }).forEach(fateId -> {
              seen.incrementAndGet();
              idConsumer.accept(fateId);
            });
      }

      // If deferredOverflow was previously marked true then the deferred map
      // would have been cleared and seen.get() should be greater than 0 as there would
      // be a lot of transactions to process in the previous run, so we won't be sleeping here
      if (seen.get() == 0) {
        if (beforeCount == unreservedRunnableCount.getCount()) {
          long waitTime = 5000;
          synchronized (AbstractFateStore.this) {
            if (!deferred.isEmpty()) {
              long currTime = System.nanoTime();
              long minWait =
                  deferred.values().stream().mapToLong(l -> l - currTime).min().getAsLong();
              waitTime = TimeUnit.MILLISECONDS.convert(minWait, TimeUnit.NANOSECONDS);
            }
          }

          if (waitTime > 0) {
            unreservedRunnableCount.waitFor(count -> count != beforeCount, waitTime,
                keepWaiting::get);
          }
        }
      }

      // Reset if the current state only if it matches the state before the execution.
      // This is to avoid a race condition where the flag was set during the run.
      // We should ensure at least one of the FATE executors will run through the
      // entire transaction list first before clearing the flag and allowing more
      // deferred entries into the map again. In other words, if the before state
      // was false and during the execution at some point it was marked true this would
      // not reset until after the next run
      deferredOverflow.compareAndSet(beforeDeferredOverflow, false);
    }
  }

  @Override
  public Stream<FateIdStatus> list() {
    return getTransactions();
  }

  @Override
  public ReadOnlyFateTxStore<T> read(FateId fateId) {
    return newFateTxStore(fateId, false);
  }

  protected boolean isRunnable(TStatus status) {
    return status == TStatus.IN_PROGRESS || status == TStatus.FAILED_IN_PROGRESS
        || status == TStatus.SUBMITTED;
  }

  public static abstract class FateIdStatusBase implements FateIdStatus {
    private final FateId fateId;

    public FateIdStatusBase(FateId fateId) {
      this.fateId = fateId;
    }

    @Override
    public FateId getFateId() {
      return fateId;
    }
  }

  @Override
  public boolean isDeferredOverflow() {
    return deferredOverflow.get();
  }

  @Override
  public int getDeferredCount() {
    // This method is primarily used right now for unit testing but
    // if this synchronization becomes an issue we could add an atomic
    // counter instead to track it separately so we don't need to lock
    synchronized (AbstractFateStore.this) {
      return deferred.size();
    }
  }

  protected abstract Stream<FateIdStatus> getTransactions();

  protected abstract TStatus _getStatus(FateId fateId);

  protected abstract FateTxStore<T> newFateTxStore(FateId fateId, boolean isReserved);

  protected abstract class AbstractFateTxStoreImpl<T> implements FateTxStore<T> {
    protected final FateId fateId;
    protected final boolean isReserved;

    protected TStatus observedStatus = null;

    protected AbstractFateTxStoreImpl(FateId fateId, boolean isReserved) {
      this.fateId = fateId;
      this.isReserved = isReserved;
    }

    @Override
    public TStatus waitForStatusChange(EnumSet<TStatus> expected) {
      Preconditions.checkState(!isReserved,
          "Attempted to wait for status change while reserved " + fateId);
      while (true) {

        long countBefore = unreservedNonNewCount.getCount();

        TStatus status = _getStatus(fateId);
        if (expected.contains(status)) {
          return status;
        }

        unreservedNonNewCount.waitFor(count -> count != countBefore, 1000, () -> true);
      }
    }

    @Override
    public void unreserve(long deferTime, TimeUnit timeUnit) {
      deferTime = TimeUnit.NANOSECONDS.convert(deferTime, timeUnit);

      if (deferTime < 0) {
        throw new IllegalArgumentException("deferTime < 0 : " + deferTime);
      }

      synchronized (AbstractFateStore.this) {
        if (!reserved.remove(fateId)) {
          throw new IllegalStateException("Tried to unreserve id that was not reserved " + fateId);
        }

        // notify any threads waiting to reserve
        AbstractFateStore.this.notifyAll();

        // If deferred map has overflowed then skip adding to the deferred map
        // and clear the map and set the flag. This will cause the next execution
        // of runnable to process all the transactions and to not defer as we
        // have a large backlog and want to make progress
        if (deferTime > 0 && !deferredOverflow.get()) {
          if (deferred.size() >= maxDeferred) {
            log.info(
                "Deferred map overflowed with size {}, clearing and setting deferredOverflow to true",
                deferred.size());
            deferredOverflow.set(true);
            deferred.clear();
          } else {
            deferred.put(fateId, System.nanoTime() + deferTime);
          }
        }
      }

      if (observedStatus != null && isRunnable(observedStatus)) {
        unreservedRunnableCount.increment();
      }

      if (observedStatus != TStatus.NEW) {
        unreservedNonNewCount.increment();
      }
    }

    protected void verifyReserved(boolean isWrite) {
      if (!isReserved && isWrite) {
        throw new IllegalStateException("Attempted write on unreserved FATE transaction.");
      }

      if (isReserved) {
        synchronized (AbstractFateStore.this) {
          if (!reserved.contains(fateId)) {
            throw new IllegalStateException("Tried to operate on unreserved transaction " + fateId);
          }
        }
      }
    }

    @Override
    public TStatus getStatus() {
      verifyReserved(false);
      var status = _getStatus(fateId);
      observedStatus = status;
      return status;
    }

    @Override
    public FateId getID() {
      return fateId;
    }

    protected byte[] serializeTxInfo(Serializable so) {
      if (so instanceof String) {
        return ("S " + so).getBytes(UTF_8);
      } else {
        byte[] sera = serialize(so);
        byte[] data = new byte[sera.length + 2];
        System.arraycopy(sera, 0, data, 2, sera.length);
        data[0] = 'O';
        data[1] = ' ';
        return data;
      }
    }

    protected Serializable deserializeTxInfo(TxInfo txInfo, byte[] data) {
      if (data[0] == 'O') {
        byte[] sera = new byte[data.length - 2];
        System.arraycopy(data, 2, sera, 0, sera.length);
        return (Serializable) deserialize(sera);
      } else if (data[0] == 'S') {
        return new String(data, 2, data.length - 2, UTF_8);
      } else {
        throw new IllegalStateException("Bad node data " + txInfo);
      }
    }
  }
}
