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

import static java.util.concurrent.TimeUnit.SECONDS;
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
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.AvailabilityForTablet;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.metadata.AccumuloTable;
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
  public void createTableWithBadProperties()
      throws AccumuloException, AccumuloSecurityException, TableExistsException {
    TableOperations tableOps = accumuloClient.tableOperations();
    String t0 = getUniqueNames(1)[0];
    tableOps.create(t0);
    assertTrue(tableOps.exists(t0));
    assertThrows(AccumuloException.class,
        () -> tableOps.setProperty(t0, Property.TABLE_BLOOM_ENABLED.getKey(), "foo"));
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
      AccumuloException, AccumuloSecurityException, TableNotFoundException, InterruptedException {
    String tableName = getUniqueNames(1)[0];
    accumuloClient.tableOperations().create(tableName);

    List<IteratorSetting> list = new ArrayList<>();
    list.add(new IteratorSetting(15, BadIterator.class));
    // don't block
    accumuloClient.tableOperations().compact(tableName, null, null, list, true, false);

    Thread.sleep(SECONDS.toMillis(2)); // start compaction

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
    timeType = accumuloClient.tableOperations().getTimeType(AccumuloTable.METADATA.tableName());
    assertEquals(TimeType.LOGICAL, timeType);

    timeType = accumuloClient.tableOperations().getTimeType(AccumuloTable.ROOT.tableName());
    assertEquals(TimeType.LOGICAL, timeType);

    // test non-existent table
    assertThrows(TableNotFoundException.class,
        () -> accumuloClient.tableOperations().getTimeType("notatable"),
        "specified table does not exist");
  }

  // This test will create a total of six tables.
  // This test will create three tables with no additional parameters, i.e., no initial splits, etc.
  // For each of the first three tablets, set ONDEMAND, HOSTED, and UNHOSTED as the tablet
  // availability, respectively.
  // Retrieving the TabletAvailability should return the above values back in a single tablet.
  //
  // The other three tables will be created with initial splits and then queried for
  // TabletAvailability's
  // For each table a list of tablets will be returned with the corresponding TabletAvailability
  // verified for correctness.
  // The last three tables will also be queried for ranges within the table and only expect to see
  // tablets with those ranges returned.
  @Test
  public void testGetTabletAvailability_DefaultTableCreations() throws AccumuloException,
      TableExistsException, AccumuloSecurityException, TableNotFoundException {

    final String[] tableNames = getUniqueNames(6);
    final String tableOnDemand = tableNames[0];
    final String tableHosted = tableNames[1];
    final String tableUnhosted = tableNames[2];
    final String tableOnDemandWithSplits = tableNames[3];
    final String tableHostedWithSplits = tableNames[4];
    final String tableUnhostedWithSplits = tableNames[5];

    SortedSet<Text> splits =
        Sets.newTreeSet(Arrays.asList(new Text("d"), new Text("m"), new Text("s")));
    NewTableConfiguration ntc = new NewTableConfiguration();

    try {
      // create all the tables with initial tablet availability values and splits
      ntc = ntc.withInitialTabletAvailability(TabletAvailability.ONDEMAND);
      accumuloClient.tableOperations().create(tableOnDemand, ntc);

      ntc = ntc.withInitialTabletAvailability(TabletAvailability.HOSTED);
      accumuloClient.tableOperations().create(tableHosted, ntc);

      ntc = ntc.withInitialTabletAvailability(TabletAvailability.UNHOSTED);
      accumuloClient.tableOperations().create(tableUnhosted, ntc);

      ntc = ntc.withSplits(splits).withInitialTabletAvailability(TabletAvailability.ONDEMAND);
      accumuloClient.tableOperations().create(tableOnDemandWithSplits, ntc);

      ntc = ntc.withSplits(splits).withInitialTabletAvailability(TabletAvailability.HOSTED);
      accumuloClient.tableOperations().create(tableHostedWithSplits, ntc);

      ntc = ntc.withSplits(splits).withInitialTabletAvailability(TabletAvailability.UNHOSTED);
      accumuloClient.tableOperations().create(tableUnhostedWithSplits, ntc);

      Map<String,String> idMap = accumuloClient.tableOperations().tableIdMap();

      List<AvailabilityForTablet> expectedTabletAvailability = new ArrayList<>();
      setExpectedTabletAvailability(expectedTabletAvailability, idMap.get(tableOnDemand), null,
          null, TabletAvailability.ONDEMAND);
      verifyTabletAvailabilites(tableOnDemand, new Range(), expectedTabletAvailability);

      expectedTabletAvailability.clear();
      setExpectedTabletAvailability(expectedTabletAvailability, idMap.get(tableHosted), null, null,
          TabletAvailability.HOSTED);
      verifyTabletAvailabilites(tableHosted, new Range(), expectedTabletAvailability);

      expectedTabletAvailability.clear();
      setExpectedTabletAvailability(expectedTabletAvailability, idMap.get(tableUnhosted), null,
          null, TabletAvailability.UNHOSTED);
      verifyTabletAvailabilites(tableUnhosted, new Range(), expectedTabletAvailability);

      verifyTablesWithSplits(tableOnDemandWithSplits, idMap, splits, TabletAvailability.ONDEMAND);
      verifyTablesWithSplits(tableHostedWithSplits, idMap, splits, TabletAvailability.HOSTED);
      verifyTablesWithSplits(tableUnhostedWithSplits, idMap, splits, TabletAvailability.UNHOSTED);

    } finally {
      accumuloClient.tableOperations().delete(tableOnDemand);
      accumuloClient.tableOperations().delete(tableHosted);
      accumuloClient.tableOperations().delete(tableUnhosted);
      accumuloClient.tableOperations().delete(tableOnDemandWithSplits);
      accumuloClient.tableOperations().delete(tableHostedWithSplits);
      accumuloClient.tableOperations().delete(tableUnhostedWithSplits);
    }
  }

  // This test creates a table with splits at creation time
  // Once created, the four tablets are provided separate tablet availabilities.
  // The test verifies that each tablet is assigned the correct tablet availability.
  @Test
  public void testGetTabletAvailability_MixedAvailabilities() throws AccumuloException,
      TableExistsException, AccumuloSecurityException, TableNotFoundException {

    String tableName = getUniqueNames(1)[0];
    List<AvailabilityForTablet> expectedTabletAvailability;
    SortedSet<Text> splits =
        Sets.newTreeSet(Arrays.asList(new Text("d"), new Text("m"), new Text("s")));

    try {
      // create table with initial splits at creation time
      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
      accumuloClient.tableOperations().create(tableName, ntc);

      // set each tablet with a different availability and query to see if they are set accordingly
      Range range = new Range(null, false, new Text("d"), true);
      accumuloClient.tableOperations().setTabletAvailability(tableName, range,
          TabletAvailability.UNHOSTED);
      range = new Range(new Text("m"), false, new Text("s"), true);
      accumuloClient.tableOperations().setTabletAvailability(tableName, range,
          TabletAvailability.HOSTED);
      range = new Range(new Text("s"), false, null, true);
      accumuloClient.tableOperations().setTabletAvailability(tableName, range,
          TabletAvailability.UNHOSTED);

      Map<String,String> idMap = accumuloClient.tableOperations().tableIdMap();
      expectedTabletAvailability = new ArrayList<>();
      String tableId = idMap.get(tableName);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "d", null,
          TabletAvailability.UNHOSTED);
      // this range was intentionally not set above, checking that the tablet has the default
      // tablet availability
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "m", "d",
          TabletAvailability.ONDEMAND);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "s", "m",
          TabletAvailability.HOSTED);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, null, "s",
          TabletAvailability.UNHOSTED);
      verifyTabletAvailabilites(tableName, new Range(), expectedTabletAvailability);
    } finally {
      accumuloClient.tableOperations().delete(tableName);
    }
  }

  // This tests creates a tables with initial splits and then queries getgoal using ranges that
  // are not on split point boundaries
  @Test
  public void testGetTabletAvailability_NonSplitBoundaries() throws AccumuloException,
      TableExistsException, AccumuloSecurityException, TableNotFoundException {

    String tableName = getUniqueNames(1)[0];
    SortedSet<Text> splits =
        Sets.newTreeSet(Arrays.asList(new Text("d"), new Text("m"), new Text("s")));
    List<AvailabilityForTablet> expectedTabletAvailability = new ArrayList<>();
    Map<String,String> idMap;
    String tableId;

    try {
      // create table with initial splits at creation time
      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
      accumuloClient.tableOperations().create(tableName, ntc);

      // set each different availability for each tablet and query to see if they are set
      // accordingly
      accumuloClient.tableOperations().setTabletAvailability(tableName, new Range(new Text("d")),
          TabletAvailability.HOSTED);
      accumuloClient.tableOperations().setTabletAvailability(tableName, new Range(new Text("m")),
          TabletAvailability.UNHOSTED);
      accumuloClient.tableOperations().setTabletAvailability(tableName, new Range(new Text("s")),
          TabletAvailability.HOSTED);

      idMap = accumuloClient.tableOperations().tableIdMap();
      tableId = idMap.get(tableName);

      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "d", null,
          TabletAvailability.HOSTED);
      // test using row as range constructor
      verifyTabletAvailabilites(tableName, new Range("a"), expectedTabletAvailability);

      // test using startRowInclusive set to true
      Range range = new Range(new Text("c"), true, new Text("c"), true);
      verifyTabletAvailabilites(tableName, range, expectedTabletAvailability);

      expectedTabletAvailability.clear();
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "m", "d",
          TabletAvailability.UNHOSTED);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "s", "m",
          TabletAvailability.HOSTED);

      range = new Range(new Text("m"), new Text("p"));
      verifyTabletAvailabilites(tableName, range, expectedTabletAvailability);

      expectedTabletAvailability.clear();
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "d", null,
          TabletAvailability.HOSTED);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "m", "d",
          TabletAvailability.UNHOSTED);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "s", "m",
          TabletAvailability.HOSTED);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, null, "s",
          TabletAvailability.ONDEMAND);

      range = new Range("b", false, "t", true);
      verifyTabletAvailabilites(tableName, range, expectedTabletAvailability);

    } finally {
      accumuloClient.tableOperations().delete(tableName);
    }
  }

  // This test creates a table with no initial splits. The splits are added after table creation.
  // This test verifies that the existing tablet availability is properly propagated to the metadata
  // table for each tablet.
  @Test
  public void testGetTabletAvailability_DelayedSplits() throws AccumuloException,
      TableExistsException, AccumuloSecurityException, TableNotFoundException {
    String tableName = getUniqueNames(1)[0];

    try {
      accumuloClient.tableOperations().create(tableName);
      Map<String,String> idMap = accumuloClient.tableOperations().tableIdMap();

      // set goals to HOSTED
      accumuloClient.tableOperations().setTabletAvailability(tableName, new Range(),
          TabletAvailability.HOSTED);

      List<AvailabilityForTablet> expectedTabletAvailability = new ArrayList<>();
      String tableId = idMap.get(tableName);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, null, null,
          TabletAvailability.HOSTED);
      verifyTabletAvailabilites(tableName, new Range(), expectedTabletAvailability);

      // Add splits after the fact
      SortedSet<Text> splits =
          Sets.newTreeSet(Arrays.asList(new Text("g"), new Text("n"), new Text("r")));
      accumuloClient.tableOperations().addSplits(tableName, splits);

      expectedTabletAvailability.clear();
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "g", null,
          TabletAvailability.HOSTED);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "n", "g",
          TabletAvailability.HOSTED);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "r", "n",
          TabletAvailability.HOSTED);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, null, "r",
          TabletAvailability.HOSTED);
      verifyTabletAvailabilites(tableName, new Range(), expectedTabletAvailability);
    } finally {
      accumuloClient.tableOperations().delete(tableName);
    }
  }

  // This test checks that tablets are correct when staggered splits are added to a table, i.e.,
  // a table is split and assigned differing goals. Later when the two existing tablets are
  // split, verify that the new splits contain the appropriate tablet availabilities of the tablet
  // from which they were split. Steps are as follows:
  // - create table
  // - add two splits; leave first tablet to default ONDEMAND, second to UNHOSTED, third to HOSTED
  // - add a split within each of the three existing tablets
  // - verify the newly created tablets are set with the Tablet Availabilities of the tablet from
  // which they are split.
  @Test
  public void testGetTabletAvailability_StaggeredSplits() throws AccumuloException,
      TableExistsException, AccumuloSecurityException, TableNotFoundException {

    String tableName = getUniqueNames(1)[0];

    try {
      accumuloClient.tableOperations().create(tableName);
      String tableId = accumuloClient.tableOperations().tableIdMap().get(tableName);

      // add split 'h' and 'q'. Leave first as ONDEMAND, set second to UNHOSTED, and third to HOSTED
      SortedSet<Text> splits = Sets.newTreeSet(Arrays.asList(new Text("h"), new Text("q")));
      accumuloClient.tableOperations().addSplits(tableName, splits);
      Range range = new Range(new Text("h"), false, new Text("q"), true);
      accumuloClient.tableOperations().setTabletAvailability(tableName, range,
          TabletAvailability.UNHOSTED);
      range = new Range(new Text("q"), false, null, true);
      accumuloClient.tableOperations().setTabletAvailability(tableName, range,
          TabletAvailability.HOSTED);

      // verify
      List<AvailabilityForTablet> expectedTabletAvailability = new ArrayList<>();
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "h", null,
          TabletAvailability.ONDEMAND);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "q", "h",
          TabletAvailability.UNHOSTED);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, null, "q",
          TabletAvailability.HOSTED);
      verifyTabletAvailabilites(tableName, new Range(), expectedTabletAvailability);

      // Add a split within each of the existing tablets. Adding 'd', 'm', and 'v'
      splits = Sets.newTreeSet(Arrays.asList(new Text("d"), new Text("m"), new Text("v")));
      accumuloClient.tableOperations().addSplits(tableName, splits);

      // verify results
      expectedTabletAvailability.clear();
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "d", null,
          TabletAvailability.ONDEMAND);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "h", "d",
          TabletAvailability.ONDEMAND);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "m", "h",
          TabletAvailability.UNHOSTED);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "q", "m",
          TabletAvailability.UNHOSTED);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, "v", "q",
          TabletAvailability.HOSTED);
      setExpectedTabletAvailability(expectedTabletAvailability, tableId, null, "v",
          TabletAvailability.HOSTED);
      verifyTabletAvailabilites(tableName, new Range(), expectedTabletAvailability);
    } finally {
      accumuloClient.tableOperations().delete(tableName);
    }
  }

  private void verifyTablesWithSplits(String tableName, Map<String,String> idMap,
      SortedSet<Text> splits, TabletAvailability tabletAvailability) throws TableNotFoundException {

    List<AvailabilityForTablet> expectedTabletAvailability = new ArrayList<>();
    String tableId = idMap.get(tableName);
    String[] splitPts = splits.stream().map(Text::toString).toArray(String[]::new);

    // retrieve all tablets for a table
    setExpectedTabletAvailability(expectedTabletAvailability, tableId, splitPts[0], null,
        tabletAvailability);
    setExpectedTabletAvailability(expectedTabletAvailability, tableId, splitPts[1], splitPts[0],
        tabletAvailability);
    setExpectedTabletAvailability(expectedTabletAvailability, tableId, splitPts[2], splitPts[1],
        tabletAvailability);
    setExpectedTabletAvailability(expectedTabletAvailability, tableId, null, splitPts[2],
        tabletAvailability);
    verifyTabletAvailabilites(tableName, new Range(), expectedTabletAvailability);

    // verify individual tablets can be retrieved
    expectedTabletAvailability.clear();
    setExpectedTabletAvailability(expectedTabletAvailability, tableId, splitPts[0], null,
        tabletAvailability);
    verifyTabletAvailabilites(tableName, new Range(null, new Text(splitPts[0])),
        expectedTabletAvailability);

    expectedTabletAvailability.clear();
    setExpectedTabletAvailability(expectedTabletAvailability, tableId, splitPts[1], splitPts[0],
        tabletAvailability);
    verifyTabletAvailabilites(tableName,
        new Range(new Text(splitPts[0]), false, new Text(splitPts[1]), true),
        expectedTabletAvailability);

    expectedTabletAvailability.clear();
    setExpectedTabletAvailability(expectedTabletAvailability, tableId, splitPts[2], splitPts[1],
        tabletAvailability);
    verifyTabletAvailabilites(tableName,
        new Range(new Text(splitPts[1]), false, new Text(splitPts[2]), true),
        expectedTabletAvailability);

    expectedTabletAvailability.clear();
    setExpectedTabletAvailability(expectedTabletAvailability, tableId, null, splitPts[2],
        tabletAvailability);
    verifyTabletAvailabilites(tableName, new Range(new Text(splitPts[2]), false, null, true),
        expectedTabletAvailability);

    expectedTabletAvailability.clear();
    setExpectedTabletAvailability(expectedTabletAvailability, tableId, splitPts[1], splitPts[0],
        tabletAvailability);
    setExpectedTabletAvailability(expectedTabletAvailability, tableId, splitPts[2], splitPts[1],
        tabletAvailability);
    verifyTabletAvailabilites(tableName,
        new Range(new Text(splitPts[0]), false, new Text(splitPts[2]), true),
        expectedTabletAvailability);
  }

  private void verifyTabletAvailabilites(String tableName, Range range,
      List<AvailabilityForTablet> expectedAvailability) throws TableNotFoundException {
    List<TabletInformation> tabletInfo = accumuloClient.tableOperations()
        .getTabletInformation(tableName, range).collect(Collectors.toList());
    assertEquals(expectedAvailability.size(), tabletInfo.size());
    for (var i = 0; i < expectedAvailability.size(); i++) {
      assertEquals(expectedAvailability.get(i).getTabletId(), tabletInfo.get(i).getTabletId());
      assertEquals(expectedAvailability.get(i).getTabletAvailability(),
          tabletInfo.get(i).getTabletAvailability());
    }
  }

  private void setExpectedTabletAvailability(List<AvailabilityForTablet> expected, String id,
      String endRow, String prevEndRow, TabletAvailability availability) {
    KeyExtent ke = new KeyExtent(TableId.of(id), endRow == null ? null : new Text(endRow),
        prevEndRow == null ? null : new Text(prevEndRow));
    expected.add(new AvailabilityForTablet(new TabletIdImpl(ke), availability));
  }

}
