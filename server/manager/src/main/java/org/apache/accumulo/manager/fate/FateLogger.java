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
package org.apache.accumulo.manager.fate;

import static org.apache.accumulo.fate.FateTxId.formatTid;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;

import org.apache.accumulo.fate.Fate;
import org.apache.accumulo.core.logging.Logging;
import org.apache.accumulo.fate.FateTransactionStatus;
import org.apache.accumulo.fate.StackOverflowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log operations that change persisted data. Operations that only read data will not be logged.
 */
public class FateLogger {
  private static final String PREFIX = Logging.PREFIX + "fate.";

  // Logs all mutations to FATEs persistent storage. Enabling this logger could help debug
  // reproducible problems with FATE transactions.
  private static final Logger storeLog = LoggerFactory.getLogger(PREFIX + "store");

  /**
   * Wrap the given TStore in a generic TStore that only logs. It will only log operations that
   * change the persisted data. Operations that only read data will not be logged.
   */
  public static TStore wrap(TStore store, Function<Repo,String> toLogString) {

    // only logging operations that change the persisted data, not operations that only read data
    return new TStore() {

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
      public List<ReadOnlyRepo> getStack(long tid) {
        return store.getStack(tid);
      }

      @Override
      public FateTransactionStatus getStatus(long tid) {
        return store.getStatus(tid);
      }

      @Override
      public FateTransactionStatus waitForStatusChange(long tid,
          EnumSet<FateTransactionStatus> expected) {
        return store.waitForStatusChange(tid, expected);
      }

      @Override
      public Serializable getTransactionInfo(long tid, Fate.TxInfo txInfo) {
        return store.getTransactionInfo(tid, txInfo);
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
      public long create() {
        long tid = store.create();
        if (storeLog.isTraceEnabled())
          storeLog.trace("{} created fate transaction", formatTid(tid));
        return tid;
      }

      @Override
      public Repo top(long tid) {
        return store.top(tid);
      }

      @Override
      public void push(long tid, Repo repo) throws StackOverflowException {
        store.push(tid, repo);
        if (storeLog.isTraceEnabled())
          storeLog.trace("{} pushed {}", formatTid(tid), toLogString.apply(repo));
      }

      @Override
      public void pop(long tid) {
        store.pop(tid);
        if (storeLog.isTraceEnabled())
          storeLog.trace("{} popped", formatTid(tid));
      }

      @Override
      public void setStatus(long tid, FateTransactionStatus status) {
        store.setStatus(tid, status);
        if (storeLog.isTraceEnabled())
          storeLog.trace("{} setStatus to {}", formatTid(tid), status);
      }

      @Override
      public void setTransactionInfo(long tid, Fate.TxInfo txInfo, Serializable val) {
        store.setTransactionInfo(tid, txInfo, val);
        if (storeLog.isTraceEnabled())
          storeLog.trace("{} setting {} to {}", formatTid(tid), txInfo, val);
      }

      @Override
      public void delete(long tid) {
        store.delete(tid);
        if (storeLog.isTraceEnabled())
          storeLog.trace("{} deleted fate transaction", formatTid(tid));
      }
    };
  }
}
