/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(75)
public class TimeoutIT extends AccumuloClusterHarness {

  @Test
  public void run() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String[] tableNames = getUniqueNames(2);
      testBatchWriterTimeout(client, tableNames[0]);
      testBatchScannerTimeout(client, tableNames[1]);
    }
  }

  public void testBatchWriterTimeout(AccumuloClient client, String tableName) throws Exception {
    client.tableOperations().create(tableName);
    client.tableOperations().addConstraint(tableName, SlowConstraint.class.getName());

    // give constraint time to propagate through zookeeper
    sleepUninterruptibly(1, TimeUnit.SECONDS);

    BatchWriter bw = client.createBatchWriter(tableName,
        new BatchWriterConfig().setTimeout(3, TimeUnit.SECONDS));

    Mutation mut = new Mutation("r1");
    mut.put("cf1", "cq1", "v1");

    bw.addMutation(mut);
    var mre =
        assertThrows("batch writer did not timeout", MutationsRejectedException.class, bw::close);
    if (mre.getCause() instanceof TimedOutException)
      return;
    throw mre;
  }

  public void testBatchScannerTimeout(AccumuloClient client, String tableName) throws Exception {
    client.tableOperations().create(tableName);

    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      Mutation m = new Mutation("r1");
      m.put("cf1", "cq1", "v1");
      m.put("cf1", "cq2", "v2");
      m.put("cf1", "cq3", "v3");
      m.put("cf1", "cq4", "v4");
      bw.addMutation(m);
    }

    try (BatchScanner bs = client.createBatchScanner(tableName)) {
      bs.setRanges(Collections.singletonList(new Range()));

      // should not timeout
      for (Entry<Key,Value> entry : bs) {
        entry.getKey();
      }

      bs.setTimeout(5, TimeUnit.SECONDS);
      IteratorSetting iterSetting = new IteratorSetting(100, SlowIterator.class);
      iterSetting.addOption("sleepTime", 2000 + "");
      bs.addScanIterator(iterSetting);

      assertThrows("batch scanner did not time out", TimedOutException.class,
          () -> bs.iterator().next());
    }
  }

}
