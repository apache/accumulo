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
package org.apache.accumulo.test.functional;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.jupiter.api.Test;

public class TimeoutIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(75);
  }

  @Test
  public void run() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String[] tableNames = getUniqueNames(3);
      testBatchWriterTimeout(client, tableNames[0]);
      testBatchScannerTimeout(client, tableNames[1]);
      testScannerTimeout(client, tableNames[2]);
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
        assertThrows(MutationsRejectedException.class, bw::close, "batch writer did not timeout");
    if (mre.getCause() instanceof TimedOutException) {
      return;
    }
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
      bs.forEach((k, v) -> {});

      bs.setTimeout(5, TimeUnit.SECONDS);
      IteratorSetting iterSetting = new IteratorSetting(100, SlowIterator.class);
      iterSetting.addOption("sleepTime", 2000 + "");
      bs.addScanIterator(iterSetting);

      assertThrows(TimedOutException.class, () -> bs.iterator().next(),
          "batch scanner did not time out");
    }
  }

  public void testScannerTimeout(AccumuloClient client, String tableName) throws Exception {
    client.tableOperations().create(tableName);

    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      Mutation m = new Mutation("r1");
      m.put("cf1", "cq1", "v1");
      m.put("cf1", "cq2", "v2");
      m.put("cf1", "cq3", "v3");
      m.put("cf1", "cq4", "v4");
      bw.addMutation(m);
    }

    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setRange(new Range());

      scanner.setTimeout(500, TimeUnit.MILLISECONDS);
      IteratorSetting iterSetting = new IteratorSetting(100, SlowIterator.class);
      iterSetting.addOption("sleepTime", 100 + "");
      iterSetting.addOption("seekSleepTime", 100 + "");

      scanner.addScanIterator(iterSetting);

      final AtomicInteger count = new AtomicInteger(0);
      // should not timeout
      scanner.forEach((k, v) -> count.incrementAndGet());
      assertEquals(4, count.get());
    }

    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setRange(new Range());

      scanner.setTimeout(500, TimeUnit.MILLISECONDS);
      IteratorSetting iterSetting = new IteratorSetting(100, SlowIterator.class);
      iterSetting.addOption("sleepTime", 5000 + "");
      iterSetting.addOption("seekSleepTime", 5000 + "");

      scanner.addScanIterator(iterSetting);

      assertThrows(RuntimeException.class, () -> scanner.iterator().next(),
          "scanner did not time out");
    }
  }
}
