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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ScanRangeIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  private static final int TS_LIMIT = 1;
  private static final int CQ_LIMIT = 5;
  private static final int CF_LIMIT = 5;
  private static final int ROW_LIMIT = 100;

  @Test
  public void run() throws Exception {
    Connector c = getConnector();
    String[] tableNames = getUniqueNames(2);
    String table1 = tableNames[0];
    c.tableOperations().create(table1);
    String table2 = tableNames[1];
    c.tableOperations().create(table2);
    TreeSet<Text> splitRows = new TreeSet<>();
    int splits = 3;
    for (int i = (ROW_LIMIT / splits); i < ROW_LIMIT; i += (ROW_LIMIT / splits))
      splitRows.add(createRow(i));
    c.tableOperations().addSplits(table2, splitRows);

    insertData(c, table1);
    scanTable(c, table1);

    insertData(c, table2);
    scanTable(c, table2);
  }

  private void scanTable(Connector c, String table) throws Exception {
    scanRange(c, table, new IntKey(0, 0, 0, 0), new IntKey(1, 0, 0, 0));

    scanRange(c, table, new IntKey(0, 0, 0, 0), new IntKey(ROW_LIMIT - 1, CF_LIMIT - 1, CQ_LIMIT - 1, 0));

    scanRange(c, table, null, null);

    for (int i = 0; i < ROW_LIMIT; i += (ROW_LIMIT / 3)) {
      for (int j = 0; j < CF_LIMIT; j += (CF_LIMIT / 2)) {
        for (int k = 1; k < CQ_LIMIT; k += (CQ_LIMIT / 2)) {
          scanRange(c, table, null, new IntKey(i, j, k, 0));
          scanRange(c, table, new IntKey(0, 0, 0, 0), new IntKey(i, j, k, 0));

          scanRange(c, table, new IntKey(i, j, k, 0), new IntKey(ROW_LIMIT - 1, CF_LIMIT - 1, CQ_LIMIT - 1, 0));

          scanRange(c, table, new IntKey(i, j, k, 0), null);

        }
      }
    }

    for (int i = 0; i < ROW_LIMIT; i++) {
      scanRange(c, table, new IntKey(i, 0, 0, 0), new IntKey(i, CF_LIMIT - 1, CQ_LIMIT - 1, 0));

      if (i > 0 && i < ROW_LIMIT - 1) {
        scanRange(c, table, new IntKey(i - 1, 0, 0, 0), new IntKey(i + 1, CF_LIMIT - 1, CQ_LIMIT - 1, 0));
      }
    }

  }

  private static class IntKey {
    private int row;
    private int cf;
    private int cq;
    private long ts;

    IntKey(IntKey ik) {
      this.row = ik.row;
      this.cf = ik.cf;
      this.cq = ik.cq;
      this.ts = ik.ts;
    }

    IntKey(int row, int cf, int cq, long ts) {
      this.row = row;
      this.cf = cf;
      this.cq = cq;
      this.ts = ts;
    }

    Key createKey() {
      Text trow = createRow(row);
      Text tcf = createCF(cf);
      Text tcq = createCQ(cq);

      return new Key(trow, tcf, tcq, ts);
    }

    IntKey increment() {

      IntKey ik = new IntKey(this);

      ik.ts++;
      if (ik.ts >= TS_LIMIT) {
        ik.ts = 0;
        ik.cq++;
        if (ik.cq >= CQ_LIMIT) {
          ik.cq = 0;
          ik.cf++;
          if (ik.cf >= CF_LIMIT) {
            ik.cf = 0;
            ik.row++;
          }
        }
      }

      return ik;
    }

  }

  private void scanRange(Connector c, String table, IntKey ik1, IntKey ik2) throws Exception {
    scanRange(c, table, ik1, false, ik2, false);
    scanRange(c, table, ik1, false, ik2, true);
    scanRange(c, table, ik1, true, ik2, false);
    scanRange(c, table, ik1, true, ik2, true);
  }

  private void scanRange(Connector c, String table, IntKey ik1, boolean inclusive1, IntKey ik2, boolean inclusive2) throws Exception {
    Scanner scanner = c.createScanner(table, Authorizations.EMPTY);

    Key key1 = null;
    Key key2 = null;

    IntKey expectedIntKey;
    IntKey expectedEndIntKey;

    if (ik1 != null) {
      key1 = ik1.createKey();
      expectedIntKey = ik1;

      if (!inclusive1) {
        expectedIntKey = expectedIntKey.increment();
      }
    } else {
      expectedIntKey = new IntKey(0, 0, 0, 0);
    }

    if (ik2 != null) {
      key2 = ik2.createKey();
      expectedEndIntKey = ik2;

      if (inclusive2) {
        expectedEndIntKey = expectedEndIntKey.increment();
      }
    } else {
      expectedEndIntKey = new IntKey(ROW_LIMIT, 0, 0, 0);
    }

    Range range = new Range(key1, inclusive1, key2, inclusive2);

    scanner.setRange(range);

    for (Entry<Key,Value> entry : scanner) {

      Key expectedKey = expectedIntKey.createKey();
      if (!expectedKey.equals(entry.getKey())) {
        throw new Exception(" " + expectedKey + " != " + entry.getKey());
      }

      expectedIntKey = expectedIntKey.increment();
    }

    if (!expectedIntKey.createKey().equals(expectedEndIntKey.createKey())) {
      throw new Exception(" " + expectedIntKey.createKey() + " != " + expectedEndIntKey.createKey());
    }
  }

  private static Text createCF(int cf) {
    Text tcf = new Text(String.format("cf_%03d", cf));
    return tcf;
  }

  private static Text createCQ(int cf) {
    Text tcf = new Text(String.format("cq_%03d", cf));
    return tcf;
  }

  private static Text createRow(int row) {
    Text trow = new Text(String.format("r_%06d", row));
    return trow;
  }

  private void insertData(Connector c, String table) throws Exception {

    BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig());

    for (int i = 0; i < ROW_LIMIT; i++) {
      Mutation m = new Mutation(createRow(i));

      for (int j = 0; j < CF_LIMIT; j++) {
        for (int k = 0; k < CQ_LIMIT; k++) {
          for (int t = 0; t < TS_LIMIT; t++) {
            m.put(createCF(j), createCQ(k), t, new Value(String.format("%06d_%03d_%03d_%03d", i, j, k, t).getBytes(UTF_8)));
          }
        }
      }

      bw.addMutation(m);
    }

    bw.close();
  }
}
