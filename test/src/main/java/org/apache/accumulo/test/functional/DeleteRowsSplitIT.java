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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// attempt to reproduce ACCUMULO-315
public class DeleteRowsSplitIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(DeleteRowsSplitIT.class);

  private static final String LETTERS = "abcdefghijklmnopqrstuvwxyz";
  static final SortedSet<Text> SPLITS = new TreeSet<>();
  static final List<String> ROWS = new ArrayList<>();
  static {
    for (byte b : LETTERS.getBytes(UTF_8)) {
      SPLITS.add(new Text(new byte[] {b}));
      ROWS.add(new String(new byte[] {b}, UTF_8));
    }
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Test
  public void run() throws Exception {
    // Delete ranges of rows, and verify the are removed
    // Do this while adding many splits
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      // Eliminate whole tablets
      for (int test = 0; test < 10; test++) {
        // create a table
        log.info("Test {}", test);
        client.tableOperations().create(tableName);

        // put some data in it
        fillTable(client, tableName);

        // generate a random delete range
        final Text start = new Text();
        final Text end = new Text();
        generateRandomRange(start, end);

        // initiate the delete range
        final boolean[] fail = {false};
        Thread t = new Thread(() -> {
          try {
            // split the table
            final SortedSet<Text> afterEnd = SPLITS.tailSet(new Text(end + "\0"));
            client.tableOperations().addSplits(tableName, afterEnd);
          } catch (Exception ex) {
            log.error("Exception", ex);
            synchronized (fail) {
              fail[0] = true;
            }
          }
        });
        t.start();

        sleepUninterruptibly(test * 2, TimeUnit.MILLISECONDS);

        client.tableOperations().deleteRows(tableName, start, end);

        t.join();
        synchronized (fail) {
          assertTrue(!fail[0]);
        }

        // scan the table
        try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
          for (Entry<Key,Value> entry : scanner) {
            Text row = entry.getKey().getRow();
            assertTrue(row.compareTo(start) <= 0 || row.compareTo(end) > 0);
          }

          // delete the table
          client.tableOperations().delete(tableName);
        }
      }
    }
  }

  private void generateRandomRange(Text start, Text end) {
    List<String> bunch = new ArrayList<>(ROWS);
    Collections.shuffle(bunch);
    if (bunch.get(0).compareTo(bunch.get(1)) < 0) {
      start.set(bunch.get(0));
      end.set(bunch.get(1));
    } else {
      start.set(bunch.get(1));
      end.set(bunch.get(0));
    }

  }

  private void fillTable(AccumuloClient client, String table) throws Exception {
    try (BatchWriter bw = client.createBatchWriter(table)) {
      for (String row : ROWS) {
        Mutation m = new Mutation(row);
        m.put("cf", "cq", "value");
        bw.addMutation(m);
      }
    }
  }
}
