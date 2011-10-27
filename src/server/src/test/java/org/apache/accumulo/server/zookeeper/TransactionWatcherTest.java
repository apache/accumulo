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
package org.apache.accumulo.server.zookeeper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import junit.framework.Assert;

import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.junit.Test;

public class TransactionWatcherTest {
  
  static class SimpleArbitrator implements TransactionWatcher.Arbitrator {
    Map<String,List<Long>> map = new HashMap<String,List<Long>>();
    
    public synchronized void start(String txType, Long txid) throws Exception {
      List<Long> txids = map.get(txType);
      if (txids == null)
        txids = new ArrayList<Long>();
      if (txids.contains(txid))
        throw new Exception("transaction already started");
      txids.add(txid);
      map.put(txType, txids);
    }
    
    public synchronized void stop(String txType, Long txid) throws Exception {
      List<Long> txids = map.get(txType);
      if (txids != null && txids.contains(txid)) {
        txids.remove(txids.indexOf(txid));
        return;
      }
      throw new Exception("transaction does not exist");
    }
    
    @Override
    synchronized public boolean transactionAlive(String txType, long tid) throws Exception {
      List<Long> txids = map.get(txType);
      if (txids == null)
        return false;
      return txids.contains(tid);
    }
    
  }
  
  @Test
  public void testTransactionWatcher() throws Exception {
    final String txType = "someName";
    final long txid = 7;
    final SimpleArbitrator sa = new SimpleArbitrator();
    final TransactionWatcher txw = new TransactionWatcher(sa);
    sa.start(txType, txid);
    try {
      sa.start(txType, txid);
      Assert.fail("simple arbitrator did not throw an exception");
    } catch (Exception ex) {
      // expected
    }
    txw.isActive(txid);
    Assert.assertFalse(txw.isActive(txid));
    txw.run(txType, txid, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        Assert.assertTrue(txw.isActive(txid));
        return null;
      }
    });
    Assert.assertFalse(txw.isActive(txid));
    sa.stop(txType, txid);
    Assert.assertFalse(sa.transactionAlive(txType, txid));
    try {
      txw.run(txType, txid, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          Assert.fail("Should not be able to start a new work on a discontinued transaction");
          return null;
        }
      });
      Assert.fail("work against stopped transaction should fail");
    } catch (Exception ex) {
      ;
    }
    final long txid2 = 9;
    sa.start(txType, txid2);
    txw.run(txType, txid2, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        Assert.assertTrue(txw.isActive(txid2));
        sa.stop(txType, txid2);
        try {
          txw.run(txType, txid2, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              Assert.fail("Should not be able to start a new work on a discontinued transaction");
              return null;
            }
          });
          Assert.fail("work against a stopped transaction should fail");
        } catch (Exception ex) {
          // expected
        }
        Assert.assertTrue(txw.isActive(txid2));
        return null;
      }
    });
    
  }
  
}
