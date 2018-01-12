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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.Test;

/**
 *
 */
public class TimeoutIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 75;
  }

  @Test
  public void run() throws Exception {
    Connector conn = getConnector();
    String[] tableNames = getUniqueNames(2);
    testBatchWriterTimeout(conn, tableNames[0]);
    testBatchScannerTimeout(conn, tableNames[1]);
  }

  public void testBatchWriterTimeout(Connector conn, String tableName) throws Exception {
    conn.tableOperations().create(tableName);
    conn.tableOperations().addConstraint(tableName, SlowConstraint.class.getName());

    // give constraint time to propagate through zookeeper
    sleepUninterruptibly(1, TimeUnit.SECONDS);

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig().setTimeout(3, TimeUnit.SECONDS));

    Mutation mut = new Mutation("r1");
    mut.put("cf1", "cq1", "v1");

    bw.addMutation(mut);
    try {
      bw.close();
      fail("batch writer did not timeout");
    } catch (MutationsRejectedException mre) {
      if (mre.getCause() instanceof TimedOutException)
        return;
      throw mre;
    }
  }

  public void testBatchScannerTimeout(Connector conn, String tableName) throws Exception {
    getConnector().tableOperations().create(tableName);

    BatchWriter bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());

    Mutation m = new Mutation("r1");
    m.put("cf1", "cq1", "v1");
    m.put("cf1", "cq2", "v2");
    m.put("cf1", "cq3", "v3");
    m.put("cf1", "cq4", "v4");

    bw.addMutation(m);
    bw.close();

    try (BatchScanner bs = getConnector().createBatchScanner(tableName, Authorizations.EMPTY, 2)) {
      bs.setRanges(Collections.singletonList(new Range()));

      // should not timeout
      for (Entry<Key,Value> entry : bs) {
        entry.getKey();
      }

      bs.setTimeout(5, TimeUnit.SECONDS);
      IteratorSetting iterSetting = new IteratorSetting(100, SlowIterator.class);
      iterSetting.addOption("sleepTime", 2000 + "");
      bs.addScanIterator(iterSetting);

      try {
        for (Entry<Key,Value> entry : bs) {
          entry.getKey();
        }
        fail("batch scanner did not time out");
      } catch (TimedOutException toe) {
        // toe.printStackTrace();
      }
    }
  }

}
