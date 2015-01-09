/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.fate.zookeeper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

public class TransactionWatcher {

  public interface Arbitrator {
    boolean transactionAlive(String type, long tid) throws Exception;

    boolean transactionComplete(String type, long tid) throws Exception;
  }

  private static final Logger log = Logger.getLogger(TransactionWatcher.class);
  final private Map<Long,AtomicInteger> counts = new HashMap<Long,AtomicInteger>();
  final private Arbitrator arbitrator;

  public TransactionWatcher(Arbitrator arbitrator) {
    this.arbitrator = arbitrator;
  }

  public <T> T run(String ztxBulk, long tid, Callable<T> callable) throws Exception {
    synchronized (counts) {
      if (!arbitrator.transactionAlive(ztxBulk, tid)) {
        throw new Exception("Transaction " + tid + " of type " + ztxBulk + " is no longer active");
      }
      AtomicInteger count = counts.get(tid);
      if (count == null)
        counts.put(tid, count = new AtomicInteger());
      count.incrementAndGet();
    }
    try {
      return callable.call();
    } finally {
      synchronized (counts) {
        AtomicInteger count = counts.get(tid);
        if (count == null) {
          log.error("unexpected missing count for transaction" + tid);
        } else {
          if (count.decrementAndGet() == 0)
            counts.remove(tid);
        }
      }
    }
  }

  public boolean isActive(long tid) {
    synchronized (counts) {
      log.debug("Transactions in progress " + counts);
      AtomicInteger count = counts.get(tid);
      return count != null && count.get() > 0;
    }
  }

}
