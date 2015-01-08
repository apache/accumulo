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
package org.apache.accumulo.test.randomwalk.bulk;

import static com.google.common.base.Charsets.UTF_8;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class Verify extends Test {

  static byte[] zero = new byte[] {'0'};

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
    if (!"true".equals(state.get("bulkImportSuccess"))) {
      log.info("Not verifying bulk import test due to import failures");
      return;
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
    ClientOnRequiredTable opts = new ClientOnRequiredTable();
    opts.parseArgs(Verify.class.getName(), args);
    Scanner scanner = opts.getConnector().createScanner(opts.tableName, opts.auths);
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

  private static void report(Text startBadRow, Text lastBadRow, Value value) {
    System.out.println("Bad value " + new String(value.get(), UTF_8));
    System.out.println(" Range [" + startBadRow + " -> " + lastBadRow + "]");
  }

}
