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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitMillionIT extends AccumuloClusterHarness {

  @Test
  public void testOneMillionTablets() throws Exception {
    Logger log = LoggerFactory.getLogger(SplitIT.class);

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);

      SortedSet<Text> splits = new TreeSet<>();

      for (int i = 100; i < 100_000_000; i += 100) {
        String split = String.format("%010d", i);

        splits.add(new Text(split));

        if (splits.size() >= 10000) {
          addSplits(c, tableName, splits, log);
        }
      }

      if (!splits.isEmpty()) {
        addSplits(c, tableName, splits, log);
      }

      var rows = IntStream
          .concat(new Random().ints(98, 0, 100_000_000).flatMap(i -> IntStream.of(i, i + 1)),
              IntStream.of(0, 1, 99_999_998, 99_999_999))
          .toArray();

      // read and write to a few of the 1 million tablets. The following should touch the first,
      // last, and a few middle tablets.
      for (var rowInt : rows) {

        var row = String.format("%010d", rowInt);

        long t1 = System.currentTimeMillis();
        try (var scanner = c.createScanner(tableName)) {
          scanner.setRange(new Range(row));
          assertEquals(0, scanner.stream().count());
        }

        long t2 = System.currentTimeMillis();

        // TODO the batch writer takes a while to bring an ondemand tablet online
        try (var writer = c.createBatchWriter(tableName)) {
          Mutation m = new Mutation(row);
          m.put("c", "x", "200");
          m.put("c", "y", "900");
          m.put("c", "z", "300");
          writer.addMutation(m);
        }

        long t3 = System.currentTimeMillis();

        try (var scanner = c.createScanner(tableName)) {
          scanner.setRange(new Range(row));
          Map<String,String> coords = scanner.stream().collect(Collectors.toMap(
              e -> e.getKey().getColumnQualifier().toString(), e -> e.getValue().toString()));
          assertEquals(Map.of("x", "200", "y", "900", "z", "300"), coords);
        }

        long t4 = System.currentTimeMillis();
        log.info("Row: {} scan1: {}ms write: {}ms scan2: {}ms", row, t2 - t1, t3 - t2, t4 - t3);
      }

      long t1 = System.currentTimeMillis();
      long count = c.tableOperations().getTabletInformation(tableName, new Range()).count();
      long t2 = System.currentTimeMillis();
      assertEquals(1_000_000, count);
      log.info("Time to scan all tablets : {}ms", t2 - t1);

      t1 = System.currentTimeMillis();
      c.tableOperations().delete(tableName);
      t2 = System.currentTimeMillis();
      log.info("Time to delete table : {}ms", t2 - t1);

    }
  }

  private static void addSplits(AccumuloClient c, String tableName, SortedSet<Text> splits,
      Logger log) throws Exception {
    long t1 = System.currentTimeMillis();
    c.tableOperations().addSplits(tableName, splits);
    long t2 = System.currentTimeMillis();
    log.info("Added {} splits in {}ms", splits.size(), t2 - t1);
    splits.clear();
  }
}
