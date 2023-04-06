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
package org.apache.accumulo.server.zookeeper;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class TransactionWatcherTest {

  static class SimpleArbitrator implements TransactionWatcher.Arbitrator {
    Map<String,List<Long>> started = new HashMap<>();
    Map<String,List<Long>> cleanedUp = new HashMap<>();

    public synchronized void start(String txType, Long txid) throws Exception {
      List<Long> txids = started.get(txType);
      if (txids == null) {
        txids = new ArrayList<>();
      }
      if (txids.contains(txid)) {
        throw new Exception("transaction already started");
      }
      txids.add(txid);
      started.put(txType, txids);

      txids = cleanedUp.get(txType);
      if (txids == null) {
        txids = new ArrayList<>();
      }
      if (txids.contains(txid)) {
        throw new IllegalStateException("transaction was started but not cleaned up");
      }
      txids.add(txid);
      cleanedUp.put(txType, txids);
    }

    public synchronized void stop(String txType, Long txid) throws Exception {
      List<Long> txids = started.get(txType);
      if (txids != null && txids.contains(txid)) {
        txids.remove(txids.indexOf(txid));
        return;
      }
      throw new Exception("transaction does not exist");
    }

    public synchronized void cleanup(String txType, Long txid) throws Exception {
      List<Long> txids = cleanedUp.get(txType);
      if (txids != null && txids.contains(txid)) {
        txids.remove(txids.indexOf(txid));
        return;
      }
      throw new Exception("transaction does not exist");
    }

    @Override
    public synchronized boolean transactionAlive(String txType, long tid) {
      List<Long> txids = started.get(txType);
      if (txids == null) {
        return false;
      }
      return txids.contains(tid);
    }

    @Override
    public boolean transactionComplete(String txType, long tid) {
      List<Long> txids = cleanedUp.get(txType);
      if (txids == null) {
        return true;
      }
      return !txids.contains(tid);
    }

  }

  @Test
  public void testTransactionWatcher() throws Exception {
    final String txType = "someName";
    final long txid = 7;
    final SimpleArbitrator sa = new SimpleArbitrator();
    final TransactionWatcher txw = new TransactionWatcher(sa);
    sa.start(txType, txid);
    assertThrows(Exception.class, () -> sa.start(txType, txid),
        "simple arbitrator did not throw an exception");
    txw.isActive(txid);
    assertFalse(txw.isActive(txid));
    txw.run(txType, txid, () -> {
      assertTrue(txw.isActive(txid));
      return null;
    });
    assertFalse(txw.isActive(txid));
    assertFalse(sa.transactionComplete(txType, txid));
    sa.stop(txType, txid);
    assertFalse(sa.transactionAlive(txType, txid));
    assertFalse(sa.transactionComplete(txType, txid));
    sa.cleanup(txType, txid);
    assertTrue(sa.transactionComplete(txType, txid));
    assertThrows(Exception.class, () -> txw.run(txType, txid, () -> null),
        "Should not be able to start a new work on a discontinued transaction");
    final long txid2 = 9;
    sa.start(txType, txid2);
    txw.run(txType, txid2, () -> {
      assertTrue(txw.isActive(txid2));
      sa.stop(txType, txid2);
      assertThrows(Exception.class, () -> txw.run(txType, txid2, () -> null),
          "Should not be able to start a new work on a discontinued transaction");
      assertTrue(txw.isActive(txid2));
      return null;
    });

  }

}
