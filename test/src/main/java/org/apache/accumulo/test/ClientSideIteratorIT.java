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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClientSideIteratorIT extends AccumuloClusterHarness {
  private List<Key> resultSet1;
  private List<Key> resultSet2;
  private List<Key> resultSet3;

  @BeforeEach
  public void setupData() {
    resultSet1 = new ArrayList<>();
    resultSet1.add(new Key("row1", "colf", "colq", 4L));
    resultSet1.add(new Key("row1", "colf", "colq", 3L));
    resultSet2 = new ArrayList<>();
    resultSet2.add(new Key("row1", "colf", "colq", 4L));
    resultSet2.add(new Key("row1", "colf", "colq", 3L));
    resultSet2.add(new Key("row1", "colf", "colq", 2L));
    resultSet2.add(new Key("row1", "colf", "colq", 1L));
    resultSet3 = new ArrayList<>();
    resultSet3.add(new Key("part1", "", "doc2"));
    resultSet3.add(new Key("part2", "", "DOC2"));
  }

  private void checkResults(final Iterable<Entry<Key,Value>> scanner, final List<Key> results,
      final PartialKey pk) {
    int i = 0;
    for (Entry<Key,Value> entry : scanner) {
      assertTrue(entry.getKey().equals(results.get(i++), pk));
    }
    assertEquals(i, results.size());
  }

  private AccumuloClient client;
  private String tableName;

  @BeforeEach
  public void setupInstance() {
    client = Accumulo.newClient().from(getClientProps()).build();
    tableName = getUniqueNames(1)[0];
  }

  @AfterEach
  public void closeClient() {
    client.close();
  }

  @Test
  public void testIntersect() throws Exception {
    client.tableOperations().create(tableName);
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      Mutation m = new Mutation("part1");
      m.put("bar", "doc1", "value");
      m.put("bar", "doc2", "value");
      m.put("dog", "doc3", "value");
      m.put("foo", "doc2", "value");
      m.put("foo", "doc3", "value");
      bw.addMutation(m);
      m = new Mutation("part2");
      m.put("bar", "DOC1", "value");
      m.put("bar", "DOC2", "value");
      m.put("dog", "DOC3", "value");
      m.put("foo", "DOC2", "value");
      m.put("foo", "DOC3", "value");
      bw.addMutation(m);
      bw.flush();
    }

    final IteratorSetting si = new IteratorSetting(10, tableName, IntersectingIterator.class);
    try (ClientSideIteratorScanner csis =
        new ClientSideIteratorScanner(client.createScanner(tableName, new Authorizations()))) {
      IntersectingIterator.setColumnFamilies(si, new Text[] {new Text("bar"), new Text("foo")});
      csis.addScanIterator(si);
      checkResults(csis, resultSet3, PartialKey.ROW_COLFAM_COLQUAL);
    }
  }

  @Test
  public void testVersioning() throws Exception {
    client.tableOperations().create(tableName);
    client.tableOperations().removeProperty(tableName, "table.iterator.scan.vers");
    client.tableOperations().removeProperty(tableName, "table.iterator.majc.vers");
    client.tableOperations().removeProperty(tableName, "table.iterator.minc.vers");
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      Mutation m = new Mutation("row1");
      m.put("colf", "colq", 1L, "value");
      m.put("colf", "colq", 2L, "value");
      bw.addMutation(m);
      bw.flush();
      m = new Mutation("row1");
      m.put("colf", "colq", 3L, "value");
      m.put("colf", "colq", 4L, "value");
      bw.addMutation(m);
      bw.flush();
    }

    try (Scanner scanner = client.createScanner(tableName, new Authorizations());
        ClientSideIteratorScanner csis = new ClientSideIteratorScanner(scanner)) {

      final IteratorSetting si = new IteratorSetting(10, "localvers", VersioningIterator.class);
      si.addOption("maxVersions", "2");
      csis.addScanIterator(si);

      checkResults(csis, resultSet1, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME);
      checkResults(scanner, resultSet2, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME);

      csis.fetchColumnFamily("colf");
      checkResults(csis, resultSet1, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME);
      csis.clearColumns();
      csis.fetchColumnFamily("none");
      assertFalse(csis.iterator().hasNext());
    }
  }
}
