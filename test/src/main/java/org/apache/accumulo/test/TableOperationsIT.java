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

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
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
import java.util.SortedSet;
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
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.functional.BadIterator;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

public class TableOperationsIT extends AccumuloClusterHarness {

  private AccumuloClient accumuloClient;
  private static final int MAX_TABLE_NAME_LEN = 1024;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(90);
  }

  @BeforeEach
  public void setup() {
    accumuloClient = Accumulo.newClient().from(getClientProps()).build();
  }

  @AfterEach
  public void checkForDanglingFateLocks() {
    if (getClusterType() == ClusterType.MINI) {
      FunctionalTestUtils.assertNoDanglingFateLocks(getCluster());
    }
    accumuloClient.close();
  }

  @Test
  public void getDiskUsageErrors() throws TableExistsException, AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    String tableName = getUniqueNames(1)[0];
    accumuloClient.tableOperations().create(tableName);
    List<DiskUsage> diskUsage =
        accumuloClient.tableOperations().getDiskUsage(Collections.singleton(tableName));
    assertEquals(1, diskUsage.size());
    assertEquals(0, diskUsage.get(0).getUsage());
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
    Map<String,String> props = accumuloClient.tableOperations().getConfiguration(tableName);
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
      assertTrue(expectedSplits.remove(clonedSplit),
          "Encountered unexpected split on the cloned table: " + clonedSplit);
    }
    assertTrue(expectedSplits.isEmpty(),
        "Did not find all expected splits on the cloned table: " + expectedSplits);
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
      for (Map.Entry<Key,Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      assertTrue(actual.isEmpty(), "Should be empty. Actual is " + actual);
      accumuloClient.tableOperations().delete(tableName);
    }
  }

  @Test
  public void getTimeTypeTest() throws TableNotFoundException, AccumuloException,
      TableExistsException, AccumuloSecurityException {
    String[] tableNames = getUniqueNames(5);

    // Create table with default MILLIS TimeType
    // By default, tables are created with the default MILLIS TimeType.
    accumuloClient.tableOperations().create(tableNames[0]);
    TimeType timeType = accumuloClient.tableOperations().getTimeType(tableNames[0]);
    assertEquals(TimeType.MILLIS, timeType);

    // Create table, explicitly setting TimeType to MILLIS
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.setTimeType(TimeType.MILLIS);
    accumuloClient.tableOperations().create(tableNames[1], ntc);
    timeType = accumuloClient.tableOperations().getTimeType(tableNames[1]);
    assertEquals(TimeType.MILLIS, timeType);

    // Create table with LOGICAL TimeType.
    ntc = new NewTableConfiguration();
    ntc.setTimeType(TimeType.LOGICAL);
    accumuloClient.tableOperations().create(tableNames[2], ntc);
    timeType = accumuloClient.tableOperations().getTimeType(tableNames[2]);
    assertEquals(TimeType.LOGICAL, timeType);

    // Create some split points
    SortedSet<Text> splits = new TreeSet<>();
    splits.add(new Text("F"));
    splits.add(new Text("M"));
    splits.add(new Text("S"));

    // Create table with MILLIS TimeType. Use splits to create multiple tablets
    ntc = new NewTableConfiguration();
    ntc.withSplits(splits);
    accumuloClient.tableOperations().create(tableNames[3], ntc);
    timeType = accumuloClient.tableOperations().getTimeType(tableNames[3]);
    assertEquals(TimeType.MILLIS, timeType);

    // Create table with LOGICAL TimeType. Use splits to create multiple tablets
    ntc = new NewTableConfiguration();
    ntc.setTimeType(TimeType.LOGICAL).withSplits(splits);
    accumuloClient.tableOperations().create(tableNames[4], ntc);
    timeType = accumuloClient.tableOperations().getTimeType(tableNames[4]);
    assertEquals(TimeType.LOGICAL, timeType);

    // check system tables
    timeType = accumuloClient.tableOperations().getTimeType(MetadataTable.NAME);
    assertEquals(TimeType.LOGICAL, timeType);

    timeType = accumuloClient.tableOperations().getTimeType(RootTable.NAME);
    assertEquals(TimeType.LOGICAL, timeType);

    // test non-existent table
    assertThrows(TableNotFoundException.class,
        () -> accumuloClient.tableOperations().getTimeType("notatable"),
        "specified table does not exist");
  }

}
