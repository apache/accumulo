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
package org.apache.accumulo.server.test.randomwalk.bulk;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class Verify extends Test {
  
  static byte[] zero = "0".getBytes();
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    ThreadPoolExecutor threadPool = Setup.getThreadPool(state);
    threadPool.shutdown();
    int lastSize = 0;
    while (!threadPool.isTerminated()) {
      int size = threadPool.getQueue().size() + threadPool.getActiveCount();
      log.info("Waiting for " + size + " nodes to complete");
      if (size != lastSize)
        makingProgress();
      lastSize = size;
      threadPool.awaitTermination(10, TimeUnit.SECONDS);
    }
    
    String user = state.getConnector().whoami();
    Authorizations auths = state.getConnector().securityOperations().getUserAuthorizations(user);
    Scanner scanner = state.getConnector().createScanner(Setup.getTableName(), auths);
    scanner.fetchColumnFamily(BulkPlusOne.CHECK_COLUMN_FAMILY);
    for (Entry<Key,Value> entry : scanner) {
      byte[] value = entry.getValue().get();
      if (!Arrays.equals(value, zero)) {
        throw new Exception("Bad key at " + entry);
      }
    }
    
    scanner.clearColumns();
    scanner.fetchColumnFamily(BulkPlusOne.MARKER_CF);
    RowIterator rowIter = new RowIterator(scanner);
    
    while (rowIter.hasNext()) {
      Iterator<Entry<Key,Value>> row = rowIter.next();
      long prev = 0;
      Text rowText = null;
      while (row.hasNext()) {
        Entry<Key,Value> entry = row.next();
        
        if (rowText == null)
          rowText = entry.getKey().getRow();

        long curr = Long.valueOf(entry.getKey().getColumnQualifier().toString());

        if (curr - 1 != prev)
          throw new Exception("Bad marker count " + entry.getKey() + " " + entry.getValue() + " " + prev);
        
        if (!entry.getValue().toString().equals("1"))
          throw new Exception("Bad marker value " + entry.getKey() + " " + entry.getValue());
        
        prev = curr;
      }
      
      if (BulkPlusOne.counter.get() != prev) {
        throw new Exception("Row " + rowText + " does not have all markers " + BulkPlusOne.counter.get() + " " + prev);
      }
    }

    log.info("Test successful on table " + Setup.getTableName());
    state.getConnector().tableOperations().delete(Setup.getTableName());
  }
    
  public static void main(String args[]) throws Exception {
    int i = 0;
    String instance = args[i++];
    String keepers = args[i++];
    String username = args[i++];
    String passwd = args[i++];
    String tablename = args[i++];
    int timeout = (int) DefaultConfiguration.getInstance().getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    Instance inst = new ZooKeeperInstance(instance, keepers, timeout);
    Connector conn = inst.getConnector(username, passwd.getBytes());
    Scanner scanner = conn.createScanner(tablename, conn.securityOperations().getUserAuthorizations(username));
    scanner.fetchColumnFamily(BulkPlusOne.CHECK_COLUMN_FAMILY);
    Text startBadRow = null;
    Text lastBadRow = null;
    Value currentBadValue = null;
    for (Entry<Key,Value> entry : scanner) {
      // System.out.println("Entry: " + entry);
      byte[] value = entry.getValue().get();
      if (!Arrays.equals(value, zero)) {
        if (currentBadValue == null || entry.getValue().equals(currentBadValue)) {
          // same value, keep skipping ahead
          lastBadRow = new Text(entry.getKey().getRow());
          if (startBadRow == null)
            startBadRow = lastBadRow;
        } else {
          // new bad value, report
          report(startBadRow, lastBadRow, currentBadValue);
          startBadRow = lastBadRow = new Text(entry.getKey().getRow());
        }
        currentBadValue = new Value(entry.getValue());
      } else {
        // end of bad range, report
        if (startBadRow != null) {
          report(startBadRow, lastBadRow, currentBadValue);
        }
        startBadRow = lastBadRow = null;
        currentBadValue = null;
      }
    }
    if (startBadRow != null) {
      report(startBadRow, lastBadRow, currentBadValue);
    }
  }
  
  /**
   * @param startBadEntry
   * @param lastBadEntry
   */
  private static void report(Text startBadRow, Text lastBadRow, Value value) {
    System.out.println("Bad value " + new String(value.get()));
    System.out.println(" Range [" + startBadRow + " -> " + lastBadRow + "]");
  }
  
}
