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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.OfflineScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
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

  private static final AtomicBoolean initCalled = new AtomicBoolean(false);

  public static class TestPropFilter extends Filter {

    private Predicate<Key> keyPredicate;

    private Predicate<Key> createRegexPredicate(String regex) {
      Predicate<Key> kp = k -> true;
      if (regex != null) {
        var pattern = Pattern.compile(regex);
        kp = k -> pattern.matcher(k.getRowData().toString()).matches();
      }

      return kp;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      Predicate<Key> generalPredicate =
          createRegexPredicate(env.getPluginEnv().getConfiguration().getCustom("testRegex"));
      Predicate<Key> tablePredicate = createRegexPredicate(
          env.getPluginEnv().getConfiguration(env.getTableId()).getTableCustom("testRegex"));
      keyPredicate = generalPredicate.and(tablePredicate);
      initCalled.set(true);
    }

    @Override
    public boolean accept(Key k, Value v) {
      return keyPredicate.test(k);
    }
  }

  private void runPluginEnvTest(Set<String> expected) throws Exception {
    try (var scanner = client.createScanner(tableName)) {
      initCalled.set(false);
      var csis = new ClientSideIteratorScanner(scanner);
      csis.addScanIterator(new IteratorSetting(100, "filter", TestPropFilter.class));
      assertEquals(expected,
          csis.stream().map(e -> e.getKey().getRowData().toString()).collect(Collectors.toSet()));
      // this check is here to ensure the iterator executed client side and not server side
      assertTrue(initCalled.get());
    }

    // The offline scanner also runs iterators client side, so test its client side access to
    // accumulo config from iterators also.
    client.tableOperations().offline(tableName, true);
    var context = (ClientContext) client;
    try (OfflineScanner offlineScanner =
        new OfflineScanner(context, context.getTableId(tableName), Authorizations.EMPTY)) {
      initCalled.set(false);
      offlineScanner.addScanIterator(new IteratorSetting(100, "filter", TestPropFilter.class));
      assertEquals(expected, offlineScanner.stream().map(e -> e.getKey().getRowData().toString())
          .collect(Collectors.toSet()));
      assertTrue(initCalled.get());
    }
    client.tableOperations().online(tableName, true);
  }

  /**
   * Test an iterators ability to access accumulo config in an iterator running client side.
   */
  @Test
  public void testPluginEnv() throws Exception {
    Set<String> rows = Set.of("1234", "abc", "xyz789");

    client.tableOperations().create(tableName);
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      for (var row : rows) {
        Mutation m = new Mutation(row);
        m.put("f", "q", "v");
        bw.addMutation(m);
      }
    }

    runPluginEnvTest(rows);

    // The iterator should see the following system property and filter based on it
    client.instanceOperations().setProperty("general.custom.testRegex", ".*[a-z]+.*");
    runPluginEnvTest(Set.of("abc", "xyz789"));

    // The iterator should see the following table property and filter based on the table and system
    // property
    client.tableOperations().setProperty(tableName, "table.custom.testRegex", ".*[0-9]+.*");
    runPluginEnvTest(Set.of("xyz789"));

    // Remove the system property, so filtering should only happen based on the table property
    client.instanceOperations().removeProperty("general.custom.testRegex");
    runPluginEnvTest(Set.of("1234", "xyz789"));

    // Iterator should do no filtering after removing this property
    client.tableOperations().removeProperty(tableName, "table.custom.testRegex");
    runPluginEnvTest(rows);
  }
}
