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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class FindMaxIT extends AccumuloClusterHarness {

  private static Mutation nm(byte[] row) {
    Mutation m = new Mutation(new Text(row));
    m.put("cf", "cq", "v");
    return m;
  }

  private static Mutation nm(String row) {
    Mutation m = new Mutation(row);
    m.put("cf", "cq", "v");
    return m;
  }

  @Test
  public void test1() throws Exception {
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName);

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());

    bw.addMutation(nm(new byte[] {0}));
    bw.addMutation(nm(new byte[] {0, 0}));
    bw.addMutation(nm(new byte[] {0, 1}));
    bw.addMutation(nm(new byte[] {0, 1, 0}));
    bw.addMutation(nm(new byte[] {1, 0}));
    bw.addMutation(nm(new byte[] {'a', 'b', 'c'}));
    bw.addMutation(nm(new byte[] {(byte) 0xff}));
    bw.addMutation(nm(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}));

    for (int i = 0; i < 1000; i += 5) {
      bw.addMutation(nm(String.format("r%05d", i)));
    }

    bw.close();

    Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);

    ArrayList<Text> rows = new ArrayList<>();

    for (Entry<Key,Value> entry : scanner) {
      rows.add(entry.getKey().getRow());
    }

    for (int i = rows.size() - 1; i > 0; i--) {
      Text max = conn.tableOperations().getMaxRow(tableName, Authorizations.EMPTY, null, true, rows.get(i), false);
      assertEquals(rows.get(i - 1), max);

      max = conn.tableOperations().getMaxRow(tableName, Authorizations.EMPTY, rows.get(i - 1), true, rows.get(i), false);
      assertEquals(rows.get(i - 1), max);

      max = conn.tableOperations().getMaxRow(tableName, Authorizations.EMPTY, rows.get(i - 1), false, rows.get(i), false);
      assertNull(max);

      max = conn.tableOperations().getMaxRow(tableName, Authorizations.EMPTY, null, true, rows.get(i), true);
      assertEquals(rows.get(i), max);

      max = conn.tableOperations().getMaxRow(tableName, Authorizations.EMPTY, rows.get(i), true, rows.get(i), true);
      assertEquals(rows.get(i), max);

      max = conn.tableOperations().getMaxRow(tableName, Authorizations.EMPTY, rows.get(i - 1), false, rows.get(i), true);
      assertEquals(rows.get(i), max);

    }

    Text max = conn.tableOperations().getMaxRow(tableName, Authorizations.EMPTY, null, true, null, true);
    assertEquals(rows.get(rows.size() - 1), max);

    max = conn.tableOperations().getMaxRow(tableName, Authorizations.EMPTY, null, true, new Text(new byte[] {0}), false);
    assertNull(max);

    max = conn.tableOperations().getMaxRow(tableName, Authorizations.EMPTY, null, true, new Text(new byte[] {0}), true);
    assertEquals(rows.get(0), max);
  }
}
