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

import static com.google.common.base.Charsets.UTF_8;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BatchWriterFlushIT extends AccumuloClusterIT {

  private static final int NUM_TO_FLUSH = 100000;

  @Override
  protected int defaultTimeoutSeconds() {
    return 90;
  }

  @Test
  public void run() throws Exception {
    Connector c = getConnector();
    String[] tableNames = getUniqueNames(2);
    String bwft = tableNames[0];
    c.tableOperations().create(bwft);
    String bwlt = tableNames[1];
    c.tableOperations().create(bwlt);
    runFlushTest(bwft);
    runLatencyTest(bwlt);

  }

  private void runLatencyTest(String tableName) throws Exception {
    // should automatically flush after 2 seconds
    BatchWriter bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig().setMaxLatency(1000, TimeUnit.MILLISECONDS));
    Scanner scanner = getConnector().createScanner(tableName, Authorizations.EMPTY);

    Mutation m = new Mutation(new Text(String.format("r_%10d", 1)));
    m.put(new Text("cf"), new Text("cq"), new Value("1".getBytes(UTF_8)));
    bw.addMutation(m);

    UtilWaitThread.sleep(500);

    int count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }

    if (count != 0) {
      throw new Exception("Flushed too soon");
    }

    UtilWaitThread.sleep(1500);

    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }

    if (count != 1) {
      throw new Exception("Did not flush");
    }

    bw.close();
  }

  private void runFlushTest(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, MutationsRejectedException,
      Exception {
    BatchWriter bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());
    Scanner scanner = getConnector().createScanner(tableName, Authorizations.EMPTY);
    Random r = new Random();

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < NUM_TO_FLUSH; j++) {
        int row = i * NUM_TO_FLUSH + j;

        Mutation m = new Mutation(new Text(String.format("r_%10d", row)));
        m.put(new Text("cf"), new Text("cq"), new Value(("" + row).getBytes()));
        bw.addMutation(m);
      }

      bw.flush();

      // do a few random lookups into the data just flushed

      for (int k = 0; k < 10; k++) {
        int rowToLookup = r.nextInt(NUM_TO_FLUSH) + i * NUM_TO_FLUSH;

        scanner.setRange(new Range(new Text(String.format("r_%10d", rowToLookup))));

        Iterator<Entry<Key,Value>> iter = scanner.iterator();

        if (!iter.hasNext())
          throw new Exception(" row " + rowToLookup + " not found after flush");

        Entry<Key,Value> entry = iter.next();

        if (iter.hasNext())
          throw new Exception("Scanner returned too much");

        verifyEntry(rowToLookup, entry);
      }

      // scan all data just flushed
      scanner.setRange(new Range(new Text(String.format("r_%10d", i * NUM_TO_FLUSH)), true, new Text(String.format("r_%10d", (i + 1) * NUM_TO_FLUSH)), false));
      Iterator<Entry<Key,Value>> iter = scanner.iterator();

      for (int j = 0; j < NUM_TO_FLUSH; j++) {
        int row = i * NUM_TO_FLUSH + j;

        if (!iter.hasNext())
          throw new Exception("Scan stopped permaturely at " + row);

        Entry<Key,Value> entry = iter.next();

        verifyEntry(row, entry);
      }

      if (iter.hasNext())
        throw new Exception("Scanner returned too much");

    }

    bw.close();

    // test adding a mutation to a closed batch writer
    boolean caught = false;
    try {
      bw.addMutation(new Mutation(new Text("foobar")));
    } catch (IllegalStateException ise) {
      caught = true;
    }

    if (!caught) {
      throw new Exception("Adding to closed batch writer did not fail");
    }
  }

  private void verifyEntry(int row, Entry<Key,Value> entry) throws Exception {
    if (!entry.getKey().getRow().toString().equals(String.format("r_%10d", row))) {
      throw new Exception("Unexpected key returned, expected " + row + " got " + entry.getKey());
    }

    if (!entry.getValue().toString().equals("" + row)) {
      throw new Exception("Unexpected value, expected " + row + " got " + entry.getValue());
    }
  }

}
