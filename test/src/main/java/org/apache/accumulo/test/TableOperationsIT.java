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
package org.apache.accumulo.test;

import static org.apache.accumulo.core.Constants.MAX_TABLE_NAME_LEN;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.functional.BadIterator;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TableOperationsIT extends AccumuloClusterHarness {

  private AccumuloClient accumuloClient;

  @Override
  public int defaultTimeoutSeconds() {
    return 90;
  }

  @Before
  public void setup() {
    accumuloClient = Accumulo.newClient().from(getClientProps()).build();
  }

  @After
  public void checkForDanglingFateLocks() {
    if (getClusterType() == ClusterType.MINI) {
      FunctionalTestUtils.assertNoDanglingFateLocks((ClientContext) accumuloClient, getCluster());
      accumuloClient.close();
    }
  }

  @Test
  public void getDiskUsageErrors() throws TableExistsException, AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    String tableName = getUniqueNames(1)[0];
    accumuloClient.tableOperations().create(tableName);
    List<DiskUsage> diskUsage =
        accumuloClient.tableOperations().getDiskUsage(Collections.singleton(tableName));
    assertEquals(1, diskUsage.size());
    assertEquals(0, (long) diskUsage.get(0).getUsage());
    assertEquals(tableName, diskUsage.get(0).getTables().iterator().next());

    accumuloClient.securityOperations().revokeTablePermission(getAdminPrincipal(), tableName,
        TablePermission.READ);
    assertThrows(AccumuloSecurityException.class,
        () -> accumuloClient.tableOperations().getDiskUsage(Collections.singleton(tableName)));

    accumuloClient.tableOperations().delete(tableName);
    assertThrows(TableNotFoundException.class,
        () -> accumuloClient.tableOperations().getDiskUsage(Collections.singleton(tableName)));
  }

  @Test
  public void getDiskUsage() throws TableExistsException, AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    final String[] names = getUniqueNames(2);
    String tableName = names[0];
    accumuloClient.tableOperations().create(tableName);

    // verify 0 disk usage
    List<DiskUsage> diskUsages =
        accumuloClient.tableOperations().getDiskUsage(Collections.singleton(tableName));
    assertEquals(1, diskUsages.size());
    assertEquals(1, diskUsages.get(0).getTables().size());
    assertEquals(Long.valueOf(0), diskUsages.get(0).getUsage());
    assertEquals(tableName, diskUsages.get(0).getTables().first());

    // add some data
    try (BatchWriter bw = accumuloClient.createBatchWriter(tableName)) {
      Mutation m = new Mutation("a");
      m.put("b", "c", new Value("abcde"));
      bw.addMutation(m);
      bw.flush();
    }

    accumuloClient.tableOperations().compact(tableName, new Text("A"), new Text("z"), true, true);

    // verify we have usage
    diskUsages = accumuloClient.tableOperations().getDiskUsage(Collections.singleton(tableName));
    assertEquals(1, diskUsages.size());
    assertEquals(1, diskUsages.get(0).getTables().size());
    assertTrue(diskUsages.get(0).getUsage() > 0);
    assertEquals(tableName, diskUsages.get(0).getTables().first());

    String newTable = names[1];

    // clone table
    accumuloClient.tableOperations().clone(tableName, newTable, false, null, null);

    // verify tables are exactly the same
    Set<String> tables = new HashSet<>();
    tables.add(tableName);
    tables.add(newTable);
    diskUsages = accumuloClient.tableOperations().getDiskUsage(tables);
    assertEquals(1, diskUsages.size());
    assertEquals(2, diskUsages.get(0).getTables().size());
    assertTrue(diskUsages.get(0).getUsage() > 0);

    accumuloClient.tableOperations().compact(tableName, new Text("A"), new Text("z"), true, true);
    accumuloClient.tableOperations().compact(newTable, new Text("A"), new Text("z"), true, true);

    // verify tables have differences
    diskUsages = accumuloClient.tableOperations().getDiskUsage(tables);
    assertEquals(2, diskUsages.size());
    assertEquals(1, diskUsages.get(0).getTables().size());
    assertEquals(1, diskUsages.get(1).getTables().size());
    assertTrue(diskUsages.get(0).getUsage() > 0);
    assertTrue(diskUsages.get(1).getUsage() > 0);

    accumuloClient.tableOperations().delete(tableName);
  }

  @Test
  public void createTable() throws TableExistsException, AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    String tableName = getUniqueNames(1)[0];
    accumuloClient.tableOperations().create(tableName);
    Iterable<Map.Entry<String,String>> itrProps =
        accumuloClient.tableOperations().getProperties(tableName);
    Map<String,String> props = propsToMap(itrProps);
    assertEquals(DefaultKeySizeConstraint.class.getName(),
        props.get(Property.TABLE_CONSTRAINT_PREFIX + "1"));
    accumuloClient.tableOperations().delete(tableName);
  }

  @Test
  public void createTableWithTableNameLengthLimit()
      throws AccumuloException, AccumuloSecurityException, TableExistsException {
    TableOperations tableOps = accumuloClient.tableOperations();
    String t0 = StringUtils.repeat('a', MAX_TABLE_NAME_LEN - 1);
    tableOps.create(t0);
    assertTrue(tableOps.exists(t0));

    String t1 = StringUtils.repeat('b', MAX_TABLE_NAME_LEN);
    tableOps.create(t1);
    assertTrue(tableOps.exists(t1));

    String t2 = StringUtils.repeat('c', MAX_TABLE_NAME_LEN + 1);
    assertThrows(IllegalArgumentException.class, () -> tableOps.create(t2));
    assertFalse(tableOps.exists(t2));
  }

  @Test
  public void createMergeClonedTable() throws Exception {
    String[] names = getUniqueNames(2);
    String originalTable = names[0];
    TableOperations tops = accumuloClient.tableOperations();

    TreeSet<Text> splits =
        Sets.newTreeSet(Arrays.asList(new Text("a"), new Text("b"), new Text("c"), new Text("d")));

    tops.create(originalTable);
    tops.addSplits(originalTable, splits);

    try (BatchWriter bw = accumuloClient.createBatchWriter(originalTable)) {
      for (Text row : splits) {
        Mutation m = new Mutation(row);
        for (int i = 0; i < 10; i++) {
          for (int j = 0; j < 10; j++) {
            m.put(Integer.toString(i), Integer.toString(j), Integer.toString(i + j));
          }
        }
        bw.addMutation(m);
      }
    }

    String clonedTable = names[1];

    tops.clone(originalTable, clonedTable, true, null, null);
    tops.merge(clonedTable, null, new Text("b"));

    Map<String,Integer> rowCounts = new HashMap<>();
    try (Scanner s = accumuloClient.createScanner(clonedTable, new Authorizations())) {
      for (Entry<Key,Value> entry : s) {
        final Key key = entry.getKey();
        String row = key.getRow().toString();
        String cf = key.getColumnFamily().toString(), cq = key.getColumnQualifier().toString();
        String value = entry.getValue().toString();

        if (rowCounts.containsKey(row)) {
          rowCounts.put(row, rowCounts.get(row) + 1);
        } else {
          rowCounts.put(row, 1);
        }

        assertEquals(Integer.parseInt(cf) + Integer.parseInt(cq), Integer.parseInt(value));
      }
    }

    Collection<Text> clonedSplits = tops.listSplits(clonedTable);
    Set<Text> expectedSplits = Sets.newHashSet(new Text("b"), new Text("c"), new Text("d"));
    for (Text clonedSplit : clonedSplits) {
      assertTrue("Encountered unexpected split on the cloned table: " + clonedSplit,
          expectedSplits.remove(clonedSplit));
    }
    assertTrue("Did not find all expected splits on the cloned table: " + expectedSplits,
        expectedSplits.isEmpty());
  }

  private Map<String,String> propsToMap(Iterable<Map.Entry<String,String>> props) {
    Map<String,String> map = new HashMap<>();
    for (Map.Entry<String,String> prop : props) {
      map.put(prop.getKey(), prop.getValue());
    }
    return map;
  }

  /** Compare only the row, column family and column qualifier. */
  static class KeyRowColFColQComparator implements Comparator<Key> {
    @Override
    public int compare(Key k1, Key k2) {
      return k1.compareTo(k2, PartialKey.ROW_COLFAM_COLQUAL);
    }
  }

  static final KeyRowColFColQComparator COMPARE_KEY_TO_COLQ = new KeyRowColFColQComparator();

  /** Test recovery from bad majc iterator via compaction cancel. */
  @Test
  public void testCompactEmptyTablesWithBadIterator_FailsAndCancel() throws TableExistsException,
      AccumuloException, AccumuloSecurityException, TableNotFoundException {
    String tableName = getUniqueNames(1)[0];
    accumuloClient.tableOperations().create(tableName);

    List<IteratorSetting> list = new ArrayList<>();
    list.add(new IteratorSetting(15, BadIterator.class));
    accumuloClient.tableOperations().compact(tableName, null, null, list, true, false); // don't
                                                                                        // block
    sleepUninterruptibly(2, TimeUnit.SECONDS); // start compaction
    accumuloClient.tableOperations().cancelCompaction(tableName);

    try (Scanner scanner = accumuloClient.createScanner(tableName, Authorizations.EMPTY)) {
      Map<Key,Value> actual = new TreeMap<>();
      for (Map.Entry<Key,Value> entry : scanner)
        actual.put(entry.getKey(), entry.getValue());
      assertTrue("Should be empty. Actual is " + actual, actual.isEmpty());
      accumuloClient.tableOperations().delete(tableName);
    }
  }

}
