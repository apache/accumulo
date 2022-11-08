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
package org.apache.accumulo.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.YieldingIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ACCUMULO-4643
public class YieldScannersIT extends AccumuloClusterHarness {
  Logger log = LoggerFactory.getLogger(YieldScannersIT.class);
  private static final char START_ROW = 'a';

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  @Test
  public void testScan() throws Exception {
    // make a table
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(tableName);
      final BatchWriter writer = client.createBatchWriter(tableName);
      for (int i = 0; i < 10; i++) {
        byte[] row = {(byte) (START_ROW + i)};
        Mutation m = new Mutation(new Text(row));
        m.put("", "", "");
        writer.addMutation(m);
      }
      writer.flush();
      writer.close();

      log.info("Creating scanner");
      // make a scanner for a table with 10 keys
      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        final IteratorSetting cfg = new IteratorSetting(100, YieldingIterator.class);
        scanner.addScanIterator(cfg);

        log.info("iterating");
        Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
        int keyCount = 0;
        int yieldNextCount = 0;
        int yieldSeekCount = 0;
        while (it.hasNext()) {
          Map.Entry<Key,Value> next = it.next();
          log.info(keyCount + ": Got key " + next.getKey() + " with value " + next.getValue());

          // verify we got the expected key
          char expected = (char) (START_ROW + keyCount);
          assertEquals(Character.toString(expected), next.getKey().getRow().toString(),
              "Unexpected row");

          // determine whether we yielded on a next and seek
          if ((keyCount & 1) != 0) {
            yieldNextCount++;
            yieldSeekCount++;
          }
          String[] value = next.getValue().toString().split(",");
          assertEquals(Integer.toString(yieldNextCount), value[0], "Unexpected yield next count");
          assertEquals(Integer.toString(yieldSeekCount), value[1], "Unexpected yield seek count");
          assertEquals(Integer.toString(yieldNextCount + yieldSeekCount), value[2],
              "Unexpected rebuild count");

          keyCount++;
        }
        assertEquals(10, keyCount, "Did not get the expected number of results");
      }
    }
  }

  @Test
  public void testBatchScan() throws Exception {
    // make a table
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(tableName);
      final BatchWriter writer = client.createBatchWriter(tableName);
      for (int i = 0; i < 10; i++) {
        byte[] row = {(byte) (START_ROW + i)};
        Mutation m = new Mutation(new Text(row));
        m.put("", "", "");
        writer.addMutation(m);
      }
      writer.flush();
      writer.close();

      log.info("Creating batch scanner");
      // make a scanner for a table with 10 keys
      try (BatchScanner scanner = client.createBatchScanner(tableName)) {
        final IteratorSetting cfg = new IteratorSetting(100, YieldingIterator.class);
        scanner.addScanIterator(cfg);
        scanner.setRanges(Collections.singleton(new Range()));

        log.info("iterating");
        Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
        int keyCount = 0;
        int yieldNextCount = 0;
        int yieldSeekCount = 0;
        while (it.hasNext()) {
          Map.Entry<Key,Value> next = it.next();
          log.info(keyCount + ": Got key " + next.getKey() + " with value " + next.getValue());

          // verify we got the expected key
          char expected = (char) (START_ROW + keyCount);
          assertEquals(Character.toString(expected), next.getKey().getRow().toString(),
              "Unexpected row");

          // determine whether we yielded on a next and seek
          if ((keyCount & 1) != 0) {
            yieldNextCount++;
            yieldSeekCount++;
          }
          String[] value = next.getValue().toString().split(",");
          assertEquals(Integer.toString(yieldNextCount), value[0], "Unexpected yield next count");
          assertEquals(Integer.toString(yieldSeekCount), value[1], "Unexpected yield seek count");
          assertEquals(Integer.toString(yieldNextCount + yieldSeekCount), value[2],
              "Unexpected rebuild count");

          keyCount++;
        }
        assertEquals(10, keyCount, "Did not get the expected number of results");
      }
    }
  }

  @Test
  public void testBatchScanWithSplits() throws Exception {
    // make a table
    final String tableName = getUniqueNames(1)[0];
    TreeSet<Text> splits = new TreeSet<>();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(tableName);
      List<Range> ranges = new ArrayList<>();
      final int alphabetLength = 26;
      try (BatchWriter writer = client.createBatchWriter(tableName, new BatchWriterConfig())) {
        for (int i = 0; i < alphabetLength; i++) {
          byte[] row = new byte[] {(byte) (START_ROW + i)};
          Text beginRow = new Text(row);
          Mutation m = new Mutation(beginRow);
          m.put(new Text(), new Text(), new Value());
          writer.addMutation(m);
          Text endRow = new Text(row);
          endRow.append("\0".getBytes(UTF_8), 0, 1);
          ranges.add(new Range(new Text(row), endRow));
          if (i % 4 == 0) {
            splits.add(beginRow);
          }
        }
        client.tableOperations().addSplits(tableName, splits);
        writer.flush();
      }

      log.info("Creating batch scanner");
      try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY, 1)) {
        final IteratorSetting cfg = new IteratorSetting(100, YieldingIterator.class);
        scanner.addScanIterator(cfg);
        scanner.setRanges(ranges);

        final AtomicInteger keyCount = new AtomicInteger();
        scanner.stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
          log.info("{}: Got key '{}' with value '{}'", keyCount, entry.getKey(), entry.getValue());
          // verify we got the expected key
          char expected = (char) (START_ROW + keyCount.get());
          assertEquals(Character.toString(expected), entry.getKey().getRow().toString(),
              "Unexpected row");
          keyCount.getAndIncrement();
        });
        assertEquals(alphabetLength, keyCount.get(), "Did not get the expected number of results");
      }
    }
  }
}
