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
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.util.CountDownTimer;
import org.apache.accumulo.core.util.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class AbstractFateStore<T> implements FateStore<T> {

  private static final Logger log = LoggerFactory.getLogger(AbstractFateStore.class);

  // Default maximum size of 100,000 transactions before deferral is stopped and
  // all existing transactions are processed immediately again
  public static final int DEFAULT_MAX_DEFERRED = 100_000;

  public static final FateIdGenerator DEFAULT_FATE_ID_GENERATOR = new FateIdGenerator() {
    @Override
    public FateId fromTypeAndKey(FateInstanceType instanceType, FateKey fateKey) {
      UUID txUUID = UUID.nameUUIDFromBytes(fateKey.getSerialized());
      return FateId.from(instanceType, txUUID);
    }
  };

  // The ZooKeeper lock for the process that's running this store instance
  protected final ZooUtil.LockID lockID;
  protected final Predicate<ZooUtil.LockID> isLockHeld;
  protected final Map<FateId,CountDownTimer> deferred;
  protected final FateIdGenerator fateIdGenerator;
  private final int maxDeferred;
  private final AtomicBoolean deferredOverflow = new AtomicBoolean();

  // This is incremented each time a transaction is unreserved that was runnable
  private final SignalCount unreservedRunnableCount = new SignalCount();

  // Keeps track of the number of concurrent callers to waitForStatusChange()
  private final AtomicInteger concurrentStatusChangeCallers = new AtomicInteger(0);

  public AbstractFateStore() {
    this(createDummyLockID(), null, DEFAULT_MAX_DEFERRED, DEFAULT_FATE_ID_GENERATOR);
  }

  public AbstractFateStore(ZooUtil.LockID lockID, Predicate<ZooUtil.LockID> isLockHeld) {
    this(lockID, isLockHeld, DEFAULT_MAX_DEFERRED, DEFAULT_FATE_ID_GENERATOR);
  }

  public AbstractFateStore(ZooUtil.LockID lockID, Predicate<ZooUtil.LockID> isLockHeld,
      int maxDeferred, FateIdGenerator fateIdGenerator) {
    this.maxDeferred = maxDeferred;
    this.fateIdGenerator = Objects.requireNonNull(fateIdGenerator);
    this.deferred = Collections.synchronizedMap(new HashMap<>());
    this.lockID = Objects.requireNonNull(lockID);
    // If the store is used for a Fate which runs a dead reservation cleaner,
    // this should be non-null, otherwise null is fine
    this.isLockHeld = isLockHeld;
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

  @Override
  public FateTxStore<T> reserve(FateId fateId) {
    var retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(25))
        .incrementBy(Duration.ofMillis(25)).maxWait(Duration.ofSeconds(30)).backOffFactor(1.5)
        .logInterval(Duration.ofMinutes(3)).createRetry();
    Optional<FateTxStore<T>> reserveAttempt = tryReserve(fateId);
    while (reserveAttempt.isEmpty()) {
      Preconditions.checkState(!_getStatus(fateId).equals(TStatus.UNKNOWN),
          "Attempted to reserve a tx that does not exist: " + fateId);
      try {
        retry.waitForNextAttempt(log, "Attempting to reserve " + fateId);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalArgumentException(e);
      }
      reserveAttempt = tryReserve(fateId);
    }
    retry.logCompletion(log, "Attempting to reserve " + fateId);

    return reserveAttempt.orElseThrow();
  }

  private static final Set<TStatus> IN_PROGRESS_SET = Set.of(TStatus.IN_PROGRESS);
  private static final Set<TStatus> OTHER_RUNNABLE_SET =
      Set.of(TStatus.SUBMITTED, TStatus.FAILED_IN_PROGRESS);

  @Override
  public void runnable(AtomicBoolean keepWaiting, Consumer<FateId> idConsumer) {

    AtomicLong seen = new AtomicLong(0);

    while (keepWaiting.get() && seen.get() == 0) {
      final long beforeCount = unreservedRunnableCount.getCount();
      final boolean beforeDeferredOverflow = deferredOverflow.get();

      try (Stream<FateIdStatus> inProgress = getTransactions(IN_PROGRESS_SET);
          Stream<FateIdStatus> other = getTransactions(OTHER_RUNNABLE_SET)) {
        // read the in progress transaction first and then everything else in order to process those
        // first
        var transactions = Stream.concat(inProgress, other);
        transactions.filter(fateIdStatus -> isRunnable(fateIdStatus.getStatus()))
            .filter(fateIdStatus -> {
              var fateId = fateIdStatus.getFateId();
              var deferredTime = deferred.get(fateId);
              if (deferredTime != null) {
                if (deferredTime.isExpired()) {
                  deferred.remove(fateId);
                } else {
                  return false;
                }
              }
              return fateIdStatus.getFateReservation().isEmpty();
            }).forEach(fateIdStatus -> {
              seen.incrementAndGet();
              idConsumer.accept(fateIdStatus.getFateId());
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
              waitTime = deferred.values().stream()
                  .mapToLong(countDownTimer -> countDownTimer.timeLeft(TimeUnit.MILLISECONDS)).min()
                  .getAsLong();
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
    return getTransactions(TStatus.ALL_STATUSES);
  }

  @Override
  public Stream<FateIdStatus> list(Set<TStatus> statuses) {
    return getTransactions(statuses);
  }

  @Override
  public ReadOnlyFateTxStore<T> read(FateId fateId) {
    return newUnreservedFateTxStore(fateId);
  }

  @Override
  public Map<FateId,FateReservation> getActiveReservations() {
    return list().filter(entry -> entry.getFateReservation().isPresent()).collect(Collectors
        .toMap(FateIdStatus::getFateId, entry -> entry.getFateReservation().orElseThrow()));
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
    return deferred.size();
  }

  protected void verifyFateKey(FateId fateId, Optional<FateKey> fateKeySeen,
      FateKey fateKeyExpected) {
    Preconditions.checkState(fateKeySeen.isPresent(), "fate key is missing from fate id " + fateId);
    Preconditions.checkState(fateKeySeen.orElseThrow().equals(fateKeyExpected),
        "Collision detected for fate id " + fateId);
  }

  protected abstract Stream<FateIdStatus> getTransactions(Set<TStatus> statuses);

  protected abstract TStatus _getStatus(FateId fateId);

  protected abstract Optional<FateKey> getKey(FateId fateId);

  protected abstract FateTxStore<T> newUnreservedFateTxStore(FateId fateId);

  protected abstract class AbstractFateTxStoreImpl<T> implements FateTxStore<T> {
    protected final FateId fateId;
    protected boolean deleted;
    protected FateReservation reservation;

    protected TStatus observedStatus = null;

    protected AbstractFateTxStoreImpl(FateId fateId) {
      this.fateId = fateId;
      this.deleted = false;
      this.reservation = null;
    }

    protected AbstractFateTxStoreImpl(FateId fateId, FateReservation reservation) {
      this.fateId = fateId;
      this.deleted = false;
      this.reservation = Objects.requireNonNull(reservation);
    }

    protected boolean isReserved() {
      return this.reservation != null;
    }

    @Override
    public TStatus waitForStatusChange(EnumSet<TStatus> expected) {
      Preconditions.checkState(!isReserved(),
          "Attempted to wait for status change while reserved: " + fateId);
      verifyReserved(false);

      int currNumCallers = concurrentStatusChangeCallers.incrementAndGet();

      try {
        var retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(25))
            .incrementBy(Duration.ofMillis(25)).maxWait(Duration.ofSeconds(currNumCallers))
            .backOffFactor(1.5).logInterval(Duration.ofMinutes(3)).createRetry();

        while (true) {

          TStatus status = _getStatus(fateId);
          if (expected.contains(status)) {
            retry.logCompletion(log, "Waiting on status change for " + fateId + " expected:"
                + expected + " status:" + status);
            return status;
          }

          try {
            retry.waitForNextAttempt(log, "Waiting on status change for " + fateId + " expected:"
                + expected + " status:" + status);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
        }
      } finally {
        concurrentStatusChangeCallers.decrementAndGet();
      }
    }

    @Override
    public void unreserve(Duration deferTime) {
      Preconditions.checkState(isReserved(),
          "Attempted to unreserve a transaction that was not reserved: " + fateId);

      if (deferTime.isNegative()) {
        throw new IllegalArgumentException("deferTime < 0 : " + deferTime);
      }

      // If deferred map has overflowed then skip adding to the deferred map
      // and clear the map and set the flag. This will cause the next execution
      // of runnable to process all the transactions and to not defer as we
      // have a large backlog and want to make progress
      if (deferTime.compareTo(Duration.ZERO) > 0 && !isDeferredOverflow()) {
        if (deferred.size() >= maxDeferred) {
          log.info(
              "Deferred map overflowed with size {}, clearing and setting deferredOverflow to true",
              deferred.size());
          deferredOverflow.set(true);
          deferred.clear();
        } else {
          deferred.put(fateId, CountDownTimer.startNew(deferTime));
        }
      }

      unreserve();

      if (observedStatus != null && isRunnable(observedStatus)) {
        unreservedRunnableCount.increment();
      }
    }

    protected abstract void unreserve();

    protected void verifyReserved(boolean isWrite) {
      if (!isReserved() && isWrite) {
        throw new IllegalStateException(
            "Attempted write on unreserved FATE transaction: " + fateId);
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
    public Optional<FateKey> getKey() {
      verifyReserved(false);
      return AbstractFateStore.this.getKey(fateId);
    }

    @Override
    public FateId getID() {
      return fateId;
    }
  }

  public interface FateIdGenerator {
    FateId fromTypeAndKey(FateInstanceType instanceType, FateKey fateKey);
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

  /**
   * this is a temporary method used to create a dummy lock when using a FateStore outside the
   * context of a Manager (one example is testing) so reservations can still be made.
   *
   * @return a dummy {@link ZooUtil.LockID}
   */
  public static ZooUtil.LockID createDummyLockID() {
    return new ZooUtil.LockID("/path", "node", 123);
  }
}
