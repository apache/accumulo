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
import java.util.function.LongConsumer;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class AbstractFateStore<T> implements FateStore<T> {

  private static final Logger log = LoggerFactory.getLogger(AbstractFateStore.class);

  protected final Set<Long> reserved;
  protected final Map<Long,Long> deferred;

  // This is incremented each time a transaction was unreserved that was non new
  protected final SignalCount unreservedNonNewCount = new SignalCount();

  // This is incremented each time a transaction is unreserved that was runnable
  protected final SignalCount unreservedRunnableCount = new SignalCount();

  public AbstractFateStore() {
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
   * Attempt to reserve transaction
   *
   * @param tid transaction id
   * @return An Optional containing the FateTxStore if the transaction was successfully reserved, or
   *         an empty Optional if the transaction was already reserved.
   */
  @Override
  public Optional<FateTxStore<T>> tryReserve(long tid) {
    synchronized (this) {
      if (!reserved.contains(tid)) {
        return Optional.of(reserve(tid));
      }
      return Optional.empty();
    }
  }

  @Override
  public FateTxStore<T> reserve(long tid) {
    synchronized (AbstractFateStore.this) {
      while (reserved.contains(tid)) {
        try {
          AbstractFateStore.this.wait(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }

      reserved.add(tid);
      return newFateTxStore(tid, true);
    }
  }

  @Override
  public void runnable(AtomicBoolean keepWaiting, LongConsumer idConsumer) {

    AtomicLong seen = new AtomicLong(0);

    while (keepWaiting.get() && seen.get() == 0) {
      final long beforeCount = unreservedRunnableCount.getCount();

      try (Stream<FateIdStatus> transactions = getTransactions()) {
        transactions.filter(fateIdStatus -> isRunnable(fateIdStatus.getStatus()))
            .mapToLong(FateIdStatus::getTxid).filter(txid -> {
              synchronized (AbstractFateStore.this) {
                var deferredTime = deferred.get(txid);
                if (deferredTime != null) {
                  if ((deferredTime - System.nanoTime()) >= 0) {
                    return false;
                  } else {
                    deferred.remove(txid);
                  }
                }
                return !reserved.contains(txid);
              }
            }).forEach(txid -> {
              seen.incrementAndGet();
              idConsumer.accept(txid);
            });
      }

      if (seen.get() == 0) {
        if (beforeCount == unreservedRunnableCount.getCount()) {
          long waitTime = 5000;
          if (!deferred.isEmpty()) {
            long currTime = System.nanoTime();
            long minWait =
                deferred.values().stream().mapToLong(l -> l - currTime).min().getAsLong();
            waitTime = TimeUnit.MILLISECONDS.convert(minWait, TimeUnit.NANOSECONDS);
          }

          if (waitTime > 0) {
            unreservedRunnableCount.waitFor(count -> count != beforeCount, waitTime,
                keepWaiting::get);
          }
        }
      }
    }
  }

  @Override
  public Stream<Long> list() {
    return getTransactions().map(fateIdStatus -> fateIdStatus.txid);
  }

  @Override
  public ReadOnlyFateTxStore<T> read(long tid) {
    return newFateTxStore(tid, false);
  }

  protected boolean isRunnable(TStatus status) {
    return status == TStatus.IN_PROGRESS || status == TStatus.FAILED_IN_PROGRESS
        || status == TStatus.SUBMITTED;
  }

  protected long parseTid(String txdir) {
    return Long.parseLong(txdir.split("_")[1], 16);
  }

  public static abstract class FateIdStatus {
    private final long txid;

    public FateIdStatus(long txid) {
      this.txid = txid;
    }

    public long getTxid() {
      return txid;
    }

    public abstract TStatus getStatus();
  }

  protected abstract Stream<FateIdStatus> getTransactions();

  protected abstract TStatus _getStatus(long tid);

  protected abstract FateTxStore<T> newFateTxStore(long tid, boolean isReserved);

  protected abstract class AbstractFateTxStoreImpl<T> implements FateTxStore<T> {
    protected final long tid;
    protected final boolean isReserved;

    protected TStatus observedStatus = null;

    protected AbstractFateTxStoreImpl(long tid, boolean isReserved) {
      this.tid = tid;
      this.isReserved = isReserved;
    }

    @Override
    public TStatus waitForStatusChange(EnumSet<TStatus> expected) {
      Preconditions.checkState(!isReserved,
          "Attempted to wait for status change while reserved " + FateTxId.formatTid(getID()));
      while (true) {

        long countBefore = unreservedNonNewCount.getCount();

        TStatus status = _getStatus(tid);
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
        if (!reserved.remove(tid)) {
          throw new IllegalStateException(
              "Tried to unreserve id that was not reserved " + FateTxId.formatTid(tid));
        }

        // notify any threads waiting to reserve
        AbstractFateStore.this.notifyAll();

        if (deferTime > 0) {
          deferred.put(tid, System.nanoTime() + deferTime);
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
          if (!reserved.contains(tid)) {
            throw new IllegalStateException(
                "Tried to operate on unreserved transaction " + FateTxId.formatTid(tid));
          }
        }
      }
    }

    @Override
    public TStatus getStatus() {
      verifyReserved(false);
      var status = _getStatus(tid);
      observedStatus = status;
      return status;
    }

    @Override
    public long getID() {
      return tid;
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
