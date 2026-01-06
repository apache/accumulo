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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.List;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Tests that iterator conflicts are detected and cause exceptions. Iterators can be added multiple
 * ways. This test ensures that:
 * <p>
 * - {@link Scanner#addScanIterator(IteratorSetting)}
 * <p>
 * - {@link TableOperations#setProperty(String, String, String)}
 * <p>
 * - {@link TableOperations#modifyProperties(String, Consumer)}
 * <p>
 * - {@link NewTableConfiguration#attachIterator(IteratorSetting, EnumSet)}
 * <p>
 * - {@link TableOperations#attachIterator(String, IteratorSetting, EnumSet)}
 * <p>
 * All fail when conflicts arise from:
 * <p>
 * - Iterators attached directly to a table
 * <p>
 * - Iterators attached to a namespace, inherited by a table
 * <p>
 * - Default table iterators, but should not fail if {@link NewTableConfiguration#withoutDefaults()}
 * is specified
 * <p>
 * - Adding the exact iterator already present should not fail
 * <p>
 */
public class IteratorConflictsIT extends SharedMiniClusterBase {
  private static TableOperations tops;
  private static NamespaceOperations nops;
  private static AccumuloClient client;
  private static final IteratorSetting iter1 = new IteratorSetting(99, "iter1name", "foo");
  private static final String iter1Key = Property.TABLE_ITERATOR_PREFIX
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + "." + iter1.getName();
  private static final String iter1Val = "99,foo";
  private static final IteratorSetting iter1PrioConflict =
      new IteratorSetting(99, "othername", "foo");
  private static final IteratorSetting iter1NameConflict =
      new IteratorSetting(101, iter1.getName(), "foo");
  private static final String iter1PrioConflictKey = Property.TABLE_ITERATOR_PREFIX
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + ".othername";
  private static final String iter1PrioConflictVal = "99,foo";
  private static final String iter1NameConflictKey = Property.TABLE_ITERATOR_PREFIX
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + "." + iter1.getName();
  private static final String iter1NameConflictVal = "101,foo";
  private static final IteratorSetting defaultIterPrioConflict =
      new IteratorSetting(20, "bar", "foo");
  private static final IteratorSetting defaultIterNameConflict =
      new IteratorSetting(101, "vers", "foo");
  private static final IteratorSetting defaultTableIter =
      IteratorConfigUtil.getInitialTableIteratorSettings().keySet().iterator().next();
  private static final String defaultIterPrioConflictKey = Property.TABLE_ITERATOR_PREFIX
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + ".foo";
  private static final String defaultIterPrioConflictVal = defaultTableIter.getPriority() + ",bar";
  private static final String defaultIterNameConflictKey = Property.TABLE_ITERATOR_PREFIX
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + "." + defaultTableIter.getName();
  private static final String defaultIterNameConflictVal = "99,bar";
  private static final String defaultIterKey = Property.TABLE_ITERATOR_PREFIX.getKey()
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + "." + defaultTableIter.getName();
  private static final String defaultIterVal =
      defaultTableIter.getPriority() + "," + defaultTableIter.getIteratorClass();

  @BeforeAll
  public static void startup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
    client = Accumulo.newClient().from(getClientProps()).build();
    tops = client.tableOperations();
    nops = client.namespaceOperations();
  }

  @AfterAll
  public static void shutdown() throws Exception {
    client.close();
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testTableIterConflict() throws Exception {
    final String[] tableNames = getUniqueNames(4);
    String table1 = tableNames[0];
    String table2 = tableNames[1];
    String table3 = tableNames[2];
    String table4 = tableNames[3];
    for (String table : tableNames) {
      tops.create(table);
    }

    // testing Scanner.addScanIterator
    try (var scanner = client.createScanner(table1)) {
      testTableIterConflict(table1, IllegalArgumentException.class,
          () -> scanner.addScanIterator(iter1PrioConflict),
          () -> scanner.addScanIterator(iter1NameConflict));
    }

    // testing TableOperations.setProperty
    testTableIterConflict(table2, AccumuloException.class,
        () -> tops.setProperty(table2, iter1PrioConflictKey, iter1PrioConflictVal),
        () -> tops.setProperty(table2, iter1NameConflictKey, iter1NameConflictVal));

    // testing TableOperations.modifyProperties
    testTableIterConflict(table3, AccumuloException.class,
        () -> tops.modifyProperties(table3,
            props -> props.put(iter1PrioConflictKey, iter1PrioConflictVal)),
        () -> tops.modifyProperties(table3,
            props -> props.put(iter1NameConflictKey, iter1NameConflictVal)));

    // NewTableConfiguration.attachIterator is not applicable for this test
    // Attaching the iterator to the table requires the table to exist, but testing
    // NewTableConfiguration.attachIterator requires that the table does not exist

    // testing TableOperations.attachIterator
    testTableIterConflict(table4, AccumuloException.class,
        () -> tops.attachIterator(table4, iter1PrioConflict),
        () -> tops.attachIterator(table4, iter1NameConflict));
  }

  private <T extends Exception> void testTableIterConflict(String table, Class<T> exceptionClass,
      Executable iterPrioConflictExec, Executable iterNameConflictExec) throws Exception {
    tops.attachIterator(table, iter1);
    var e = assertThrows(exceptionClass, iterPrioConflictExec);
    assertTrue(e.getMessage().contains("iterator priority conflict")
        && e.getMessage().contains(iter1.getName()));
    e = assertThrows(exceptionClass, iterNameConflictExec);
    assertTrue(e.getMessage().contains("iterator name conflict")
        && e.getMessage().contains(iter1.getName()));
  }

  @Test
  public void testNamespaceIterConflict() throws Exception {
    final String[] names = getUniqueNames(10);
    String ns1 = names[0];
    String table1 = ns1 + "." + names[1];
    String ns2 = names[2];
    String table2 = ns2 + "." + names[3];
    String ns3 = names[4];
    String table3 = ns3 + "." + names[5];
    String ns4 = names[6];
    String table4 = ns4 + "." + names[7];
    String ns5 = names[8];
    String table5 = ns5 + "." + names[9];
    for (String ns : List.of(ns1, ns2, ns3, ns4, ns5)) {
      nops.create(ns);
    }
    // don't create table4
    for (String table : List.of(table1, table2, table3, table5)) {
      tops.create(table);
    }

    // testing Scanner.addScanIterator
    try (var scanner = client.createScanner(table1)) {
      testNamespaceIterConflict(ns1, IllegalArgumentException.class,
          () -> scanner.addScanIterator(iter1PrioConflict),
          () -> scanner.addScanIterator(iter1NameConflict));
    }

    // testing TableOperations.setProperty
    testNamespaceIterConflict(ns2, AccumuloException.class,
        () -> tops.setProperty(table2, iter1PrioConflictKey, iter1PrioConflictVal),
        () -> tops.setProperty(table2, iter1NameConflictKey, iter1NameConflictVal));

    // testing TableOperations.modifyProperties
    testNamespaceIterConflict(ns3, AccumuloException.class,
        () -> tops.modifyProperties(table3,
            props -> props.put(iter1PrioConflictKey, iter1PrioConflictVal)),
        () -> tops.modifyProperties(table3,
            props -> props.put(iter1NameConflictKey, iter1NameConflictVal)));

    // testing NewTableConfiguration.attachIterator
    testNamespaceIterConflict(ns4, AccumuloException.class,
        () -> tops.create(table4, new NewTableConfiguration().attachIterator(iter1PrioConflict)),
        () -> tops.create(table4, new NewTableConfiguration().attachIterator(iter1NameConflict)));

    // testing TableOperations.attachIterator
    testNamespaceIterConflict(ns5, AccumuloException.class,
        () -> tops.attachIterator(table5, iter1PrioConflict),
        () -> tops.attachIterator(table5, iter1NameConflict));
  }

  private <T extends Exception> void testNamespaceIterConflict(String ns, Class<T> exceptionClass,
      Executable iterPrioConflictExec, Executable iterNameConflictExec) throws Exception {
    nops.attachIterator(ns, iter1);

    var e = assertThrows(exceptionClass, iterPrioConflictExec);
    assertTrue(e.getMessage().contains("iterator priority conflict")
        && e.getMessage().contains(iter1.getName()));
    e = assertThrows(exceptionClass, iterNameConflictExec);
    assertTrue(e.getMessage().contains("iterator name conflict")
        && e.getMessage().contains(iter1.getName()));
  }

  @Test
  public void testDefaultIterConflict() throws Throwable {
    final String[] tables = getUniqueNames(11);
    String defaultsTable1 = tables[0];
    String noDefaultsTable1 = tables[1];
    String defaultsTable2 = tables[2];
    String noDefaultsTable2 = tables[3];
    String defaultsTable3 = tables[4];
    String noDefaultsTable3 = tables[5];
    String defaultsTable4 = tables[6];
    String noDefaultsTable4 = tables[7];
    String noDefaultsTable5 = tables[9];
    String defaultsTable5 = tables[8];
    String noDefaultsTable6 = tables[10];
    // don't create defaultsTable4
    for (String defaultsTable : List.of(defaultsTable1, defaultsTable2, defaultsTable3,
        defaultsTable5)) {
      tops.create(defaultsTable);
    }
    // don't create noDefaultsTable4 or noDefaultsTable5
    for (String noDefaultsTable : List.of(noDefaultsTable1, noDefaultsTable2, noDefaultsTable3,
        noDefaultsTable6)) {
      tops.create(noDefaultsTable, new NewTableConfiguration().withoutDefaults());
    }

    // testing Scanner.addScanIterator
    try (var defaultsScanner = client.createScanner(defaultsTable1);
        var noDefaultsScanner = client.createScanner(noDefaultsTable1)) {
      testDefaultIterConflict(IllegalArgumentException.class,
          () -> defaultsScanner.addScanIterator(defaultIterPrioConflict),
          () -> defaultsScanner.addScanIterator(defaultIterNameConflict),
          () -> noDefaultsScanner.addScanIterator(defaultIterPrioConflict),
          () -> noDefaultsScanner.addScanIterator(defaultIterNameConflict));
    }

    // testing TableOperations.setProperty
    testDefaultIterConflict(AccumuloException.class,
        () -> tops.setProperty(defaultsTable2, defaultIterPrioConflictKey,
            defaultIterPrioConflictVal),
        () -> tops.setProperty(defaultsTable2, defaultIterNameConflictKey,
            defaultIterNameConflictVal),
        () -> tops.setProperty(noDefaultsTable2, defaultIterPrioConflictKey,
            defaultIterPrioConflictVal),
        () -> tops.setProperty(noDefaultsTable2, defaultIterNameConflictKey,
            defaultIterNameConflictVal));

    // testing TableOperations.modifyProperties
    testDefaultIterConflict(AccumuloException.class,
        () -> tops.modifyProperties(defaultsTable3,
            props -> props.put(defaultIterPrioConflictKey, defaultIterPrioConflictVal)),
        () -> tops.modifyProperties(defaultsTable3,
            props -> props.put(defaultIterNameConflictKey, defaultIterNameConflictVal)),
        () -> tops.modifyProperties(noDefaultsTable3,
            props -> props.put(defaultIterPrioConflictKey, defaultIterPrioConflictVal)),
        () -> tops.modifyProperties(noDefaultsTable3,
            props -> props.put(defaultIterNameConflictKey, defaultIterNameConflictVal)));

    // testing NewTableConfiguration.attachIterator
    testDefaultIterConflict(AccumuloException.class,
        () -> tops.create(defaultsTable4,
            new NewTableConfiguration().attachIterator(defaultIterPrioConflict)),
        () -> tops.create(defaultsTable4,
            new NewTableConfiguration().attachIterator(defaultIterNameConflict)),
        () -> tops.create(noDefaultsTable4,
            new NewTableConfiguration().attachIterator(defaultIterPrioConflict).withoutDefaults()),
        () -> tops.create(noDefaultsTable5,
            new NewTableConfiguration().attachIterator(defaultIterNameConflict).withoutDefaults()));

    // testing TableOperations.attachIterator
    testDefaultIterConflict(AccumuloException.class,
        () -> tops.attachIterator(defaultsTable5, defaultIterPrioConflict),
        () -> tops.attachIterator(defaultsTable5, defaultIterNameConflict),
        () -> tops.attachIterator(noDefaultsTable6, defaultIterPrioConflict),
        () -> tops.attachIterator(noDefaultsTable6, defaultIterNameConflict));
  }

  private <T extends Exception> void testDefaultIterConflict(Class<T> exceptionClass,
      Executable defaultsTableOp1, Executable defaultsTableOp2, Executable noDefaultsTableOp1,
      Executable noDefaultsTableOp2) throws Throwable {
    var e = assertThrows(exceptionClass, defaultsTableOp1);
    // exception message different depending on operation, just checking essential info
    assertTrue(
        e.getMessage().contains("VersioningIterator") && e.getMessage().contains("conflict"));

    e = assertThrows(exceptionClass, defaultsTableOp2);
    // exception message different depending on operation, just checking essential info
    assertTrue(
        e.getMessage().contains("VersioningIterator") && e.getMessage().contains("conflict"));

    noDefaultsTableOp1.execute(); // should NOT fail

    noDefaultsTableOp2.execute(); // should NOT fail
  }

  @Test
  public void testSameIterNoConflict() throws Throwable {
    final String[] names = getUniqueNames(7);
    final String table1 = names[0];
    final String table2 = names[1];
    final String table3 = names[2];
    final String ns = names[3];
    final String table4 = ns + "." + names[4];
    final String table5 = names[5];
    final String table6 = names[6];
    // don't create table4 or table5
    for (String table : List.of(table1, table2, table3, table6)) {
      tops.create(table);
      tops.attachIterator(table, iter1);
    }

    // testing Scanner.addScanIterator
    try (var scanner = client.createScanner(table1)) {
      testSameIterNoConflict(() -> scanner.addScanIterator(iter1),
          () -> scanner.addScanIterator(defaultTableIter));
    }

    // testing TableOperations.setProperty
    // note that this is not technically the exact same iterator since the default iterator has
    // options (which are separate properties), but this call has no effect on the
    // property map/iterators, so this call should not throw
    testSameIterNoConflict(() -> tops.setProperty(table2, iter1Key, iter1Val),
        () -> tops.setProperty(table2, defaultIterKey, defaultIterVal));

    // testing TableOperations.modifyProperties
    // note that this is not technically the exact same iterator since the default iterator has
    // options (which are separate properties), but this call has no effect on the
    // property map/iterators, so this call should not throw
    testSameIterNoConflict(
        () -> tops.modifyProperties(table3, props -> props.put(iter1Key, iter1Val)),
        () -> tops.modifyProperties(table3, props -> props.put(defaultIterKey, defaultIterVal)));

    // testing NewTableConfiguration.attachIterator
    nops.create(ns);
    nops.attachIterator(ns, iter1);
    testSameIterNoConflict(
        () -> tops.create(table4, new NewTableConfiguration().attachIterator(iter1)),
        () -> tops.create(table5, new NewTableConfiguration().attachIterator(defaultTableIter)));

    // testing TableOperations.attachIterator
    testSameIterNoConflict(() -> tops.attachIterator(table6, iter1),
        () -> tops.attachIterator(table6, defaultTableIter));
  }

  private void testSameIterNoConflict(Executable addIter1Executable,
      Executable addDefaultIterExecutable) throws Throwable {
    // should be able to add same exact iterator
    addIter1Executable.execute();
    addDefaultIterExecutable.execute();
  }
}
