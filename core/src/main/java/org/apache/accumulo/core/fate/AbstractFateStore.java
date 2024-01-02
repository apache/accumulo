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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class AbstractFateStore<T> implements FateStore<T> {

  private static final Logger log = LoggerFactory.getLogger(AbstractFateStore.class);

  protected final Set<Long> reserved;
  protected final Map<Long,Long> defered;

  // This is incremented each time a transaction was unreserved that was non new
  protected final SignalCount unreservedNonNewCount = new SignalCount();

  // This is incremented each time a transaction is unreserved that was runnable
  protected final SignalCount unreservedRunnableCount = new SignalCount();

  public AbstractFateStore() {
    this.reserved = new HashSet<>();
    this.defered = new HashMap<>();
  }

  public static byte[] serialize(Object o) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(o);
      oos.close();

      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressFBWarnings(value = "OBJECT_DESERIALIZATION",
      justification = "unsafe to store arbitrary serialized objects like this, but needed for now"
          + " for backwards compatibility")
  public static Object deserialize(byte[] ser) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(ser);
      ObjectInputStream ois = new ObjectInputStream(bais);
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
   * @return true if reserved by this call, false if already reserved
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
  public Iterator<Long> runnable(AtomicBoolean keepWaiting) {

    while (keepWaiting.get()) {
      ArrayList<Long> runnableTids = new ArrayList<>();

      final long beforeCount = unreservedRunnableCount.getCount();

      List<String> transactions = getTransactions();
      for (String txidStr : transactions) {
        long txid = parseTid(txidStr);
        if (isRunnable(_getStatus(txid))) {
          runnableTids.add(txid);
        }
      }

      synchronized (this) {
        runnableTids.removeIf(txid -> {
          var deferedTime = defered.get(txid);
          if (deferedTime != null) {
            if (deferedTime >= System.currentTimeMillis()) {
              return true;
            } else {
              defered.remove(txid);
            }
          }

          if (reserved.contains(txid)) {
            return true;
          }

          return false;
        });
      }

      if (runnableTids.isEmpty()) {
        if (beforeCount == unreservedRunnableCount.getCount()) {
          long waitTime = 5000;
          if (!defered.isEmpty()) {
            Long minTime = Collections.min(defered.values());
            waitTime = minTime - System.currentTimeMillis();
          }

          if (waitTime > 0) {
            unreservedRunnableCount.waitFor(count -> count != beforeCount, waitTime,
                keepWaiting::get);
          }
        }
      } else {
        return runnableTids.iterator();
      }

    }

    return List.<Long>of().iterator();
  }

  @Override
  public List<Long> list() {
    ArrayList<Long> l = new ArrayList<>();
    List<String> transactions = getTransactions();
    for (String txid : transactions) {
      l.add(parseTid(txid));
    }
    return l;
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

  protected abstract List<String> getTransactions();

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
    public void unreserve(long deferTime) {

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
          defered.put(tid, System.currentTimeMillis() + deferTime);
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
