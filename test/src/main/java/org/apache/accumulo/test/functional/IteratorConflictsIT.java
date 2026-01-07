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
 * - {@link NamespaceOperations#attachIterator(String, IteratorSetting, EnumSet)}
 * <p>
 * - {@link NamespaceOperations#setProperty(String, String, String)}
 * <p>
 * - {@link NamespaceOperations#modifyProperties(String, Consumer)}
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
    final String[] tableNames = getUniqueNames(10);
    String table1 = tableNames[0];
    String table2 = tableNames[1];
    String table3 = tableNames[2];
    String table4 = tableNames[3];
    String ns5 = tableNames[4];
    String table5 = ns5 + "." + tableNames[5];
    String ns6 = tableNames[6];
    String table6 = ns6 + "." + tableNames[7];
    String ns7 = tableNames[8];
    String table7 = ns7 + "." + tableNames[9];
    for (String ns : List.of(ns5, ns6, ns7)) {
      nops.create(ns);
    }
    for (String table : List.of(table1, table2, table3, table4, table5, table6, table7)) {
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

    // testing NamespaceOperations.attachIterator
    testTableIterConflict(table5, AccumuloException.class,
        () -> nops.attachIterator(ns5, iter1PrioConflict),
        () -> nops.attachIterator(ns5, iter1NameConflict));

    // testing NamespaceOperations.setProperty
    testTableIterConflict(table6, AccumuloException.class,
        () -> nops.setProperty(ns6, iter1PrioConflictKey, iter1PrioConflictVal),
        () -> nops.setProperty(ns6, iter1NameConflictKey, iter1NameConflictVal));

    // testing NamespaceOperations.modifyProperties
    testTableIterConflict(table7, AccumuloException.class,
        () -> nops.modifyProperties(ns7,
            props -> props.put(iter1PrioConflictKey, iter1PrioConflictVal)),
        () -> nops.modifyProperties(ns7,
            props -> props.put(iter1NameConflictKey, iter1NameConflictVal)));
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
    final String[] names = getUniqueNames(16);
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
    String ns6 = names[10];
    String table6 = ns5 + "." + names[11];
    String ns7 = names[12];
    String table7 = ns5 + "." + names[13];
    String ns8 = names[14];
    String table8 = ns5 + "." + names[15];
    for (String ns : List.of(ns1, ns2, ns3, ns4, ns5, ns6, ns7, ns8)) {
      nops.create(ns);
    }
    // don't create table4
    for (String table : List.of(table1, table2, table3, table5, table6, table7, table8)) {
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

    // testing NamespaceOperations.attachIterator
    testNamespaceIterConflict(ns6, AccumuloException.class,
        () -> nops.attachIterator(ns6, iter1PrioConflict),
        () -> nops.attachIterator(ns6, iter1NameConflict));

    // testing NamespaceOperations.setProperty
    testNamespaceIterConflict(ns6, AccumuloException.class,
        () -> nops.setProperty(ns6, iter1PrioConflictKey, iter1PrioConflictVal),
        () -> nops.setProperty(ns6, iter1NameConflictKey, iter1NameConflictVal));

    // testing NamespaceOperations.modifyProperties
    testNamespaceIterConflict(ns6, AccumuloException.class,
        () -> nops.modifyProperties(ns6,
            props -> props.put(iter1PrioConflictKey, iter1PrioConflictVal)),
        () -> nops.modifyProperties(ns6,
            props -> props.put(iter1NameConflictKey, iter1NameConflictVal)));
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
    final String[] tables = getUniqueNames(23);

    // testing Scanner.addScanIterator
    String defaultsTable1 = tables[0];
    tops.create(defaultsTable1);
    String noDefaultsTable1 = tables[1];
    tops.create(noDefaultsTable1, new NewTableConfiguration().withoutDefaults());
    try (var defaultsScanner = client.createScanner(defaultsTable1);
        var noDefaultsScanner = client.createScanner(noDefaultsTable1)) {
      testDefaultIterConflict(IllegalArgumentException.class,
          () -> defaultsScanner.addScanIterator(defaultIterPrioConflict),
          () -> defaultsScanner.addScanIterator(defaultIterNameConflict),
          () -> noDefaultsScanner.addScanIterator(defaultIterPrioConflict),
          () -> noDefaultsScanner.addScanIterator(defaultIterNameConflict));
    }

    // testing TableOperations.setProperty
    String defaultsTable2 = tables[2];
    tops.create(defaultsTable2);
    String noDefaultsTable2 = tables[3];
    tops.create(noDefaultsTable2, new NewTableConfiguration().withoutDefaults());
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
    String defaultsTable3 = tables[4];
    tops.create(defaultsTable3);
    String noDefaultsTable3 = tables[5];
    tops.create(noDefaultsTable3, new NewTableConfiguration().withoutDefaults());
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
    String defaultsTable4 = tables[6];
    String noDefaultsTable4 = tables[7];
    String noDefaultsTable5 = tables[8];
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
    String defaultsTable6 = tables[9];
    tops.create(defaultsTable6);
    String noDefaultsTable6 = tables[10];
    tops.create(noDefaultsTable6, new NewTableConfiguration().withoutDefaults());
    testDefaultIterConflict(AccumuloException.class,
        () -> tops.attachIterator(defaultsTable6, defaultIterPrioConflict),
        () -> tops.attachIterator(defaultsTable6, defaultIterNameConflict),
        () -> tops.attachIterator(noDefaultsTable6, defaultIterPrioConflict),
        () -> tops.attachIterator(noDefaultsTable6, defaultIterNameConflict));

    // testing NamespaceOperations.attachIterator
    String ns7 = tables[11];
    nops.create(ns7);
    String defaultsTable7 = ns7 + "." + tables[12];
    tops.create(defaultsTable7);
    String ns8 = tables[13];
    nops.create(ns8);
    String noDefaultsTable8 = ns8 + "." + tables[14];
    tops.create(noDefaultsTable8, new NewTableConfiguration().withoutDefaults());
    testDefaultIterConflict(AccumuloException.class,
        () -> nops.attachIterator(ns7, defaultIterPrioConflict),
        () -> nops.attachIterator(ns7, defaultIterNameConflict),
        () -> nops.attachIterator(ns8, defaultIterPrioConflict),
        () -> nops.attachIterator(ns8, defaultIterNameConflict));

    // testing NamespaceOperations.setProperty
    String ns9 = tables[15];
    nops.create(ns9);
    String defaultsTable9 = ns9 + "." + tables[16];
    tops.create(defaultsTable9);
    String ns10 = tables[17];
    nops.create(ns10);
    String noDefaultsTable10 = ns10 + "." + tables[18];
    tops.create(noDefaultsTable10, new NewTableConfiguration().withoutDefaults());
    testDefaultIterConflict(AccumuloException.class,
        () -> nops.setProperty(ns9, defaultIterPrioConflictKey, defaultIterPrioConflictVal),
        () -> nops.setProperty(ns9, defaultIterNameConflictKey, defaultIterNameConflictVal),
        () -> nops.setProperty(ns10, defaultIterPrioConflictKey, defaultIterPrioConflictVal),
        () -> nops.setProperty(ns10, defaultIterNameConflictKey, defaultIterNameConflictVal));

    // testing NamespaceOperations.modifyProperties
    String ns11 = tables[19];
    nops.create(ns11);
    String defaultsTable11 = ns11 + "." + tables[20];
    tops.create(defaultsTable11);
    String ns12 = tables[21];
    nops.create(ns12);
    String noDefaultsTable12 = ns12 + "." + tables[22];
    tops.create(noDefaultsTable12, new NewTableConfiguration().withoutDefaults());
    testDefaultIterConflict(AccumuloException.class,
        () -> nops.setProperty(ns11, defaultIterPrioConflictKey, defaultIterPrioConflictVal),
        () -> nops.setProperty(ns11, defaultIterNameConflictKey, defaultIterNameConflictVal),
        () -> nops.setProperty(ns12, defaultIterPrioConflictKey, defaultIterPrioConflictVal),
        () -> nops.setProperty(ns12, defaultIterNameConflictKey, defaultIterNameConflictVal));
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
    final String[] names = getUniqueNames(13);

    // testing Scanner.addScanIterator
    final String table1 = names[0];
    tops.create(table1);
    tops.attachIterator(table1, iter1);
    try (var scanner = client.createScanner(table1)) {
      testSameIterNoConflict(() -> scanner.addScanIterator(iter1),
          () -> scanner.addScanIterator(defaultTableIter));
    }

    // testing TableOperations.setProperty
    // note that this is not technically the exact same iterator since the default iterator has
    // options (which are separate properties), but this call has no effect on the
    // property map/iterators, so this call should not throw
    final String table2 = names[1];
    tops.create(table2);
    tops.attachIterator(table2, iter1);
    testSameIterNoConflict(() -> tops.setProperty(table2, iter1Key, iter1Val),
        () -> tops.setProperty(table2, defaultIterKey, defaultIterVal));

    // testing TableOperations.modifyProperties
    // note that this is not technically the exact same iterator since the default iterator has
    // options (which are separate properties), but this call has no effect on the
    // property map/iterators, so this call should not throw
    final String table3 = names[2];
    tops.create(table3);
    tops.attachIterator(table3, iter1);
    testSameIterNoConflict(
        () -> tops.modifyProperties(table3, props -> props.put(iter1Key, iter1Val)),
        () -> tops.modifyProperties(table3, props -> props.put(defaultIterKey, defaultIterVal)));

    // testing NewTableConfiguration.attachIterator
    final String ns1 = names[3];
    final String table4 = ns1 + "." + names[4];
    final String table5 = names[5];
    nops.create(ns1);
    nops.attachIterator(ns1, iter1);
    testSameIterNoConflict(
        () -> tops.create(table4, new NewTableConfiguration().attachIterator(iter1)),
        () -> tops.create(table5, new NewTableConfiguration().attachIterator(defaultTableIter)));

    // testing TableOperations.attachIterator
    final String table6 = names[6];
    tops.create(table6);
    tops.attachIterator(table6, iter1);
    testSameIterNoConflict(() -> tops.attachIterator(table6, iter1),
        () -> tops.attachIterator(table6, defaultTableIter));

    // testing NamespaceOperations.attachIterator
    final String ns2 = names[7];
    final String table7 = ns2 + "." + names[8];
    nops.create(ns2);
    tops.create(table7);
    tops.attachIterator(table7, iter1);
    testSameIterNoConflict(() -> nops.attachIterator(ns2, iter1),
        () -> nops.attachIterator(ns2, defaultTableIter));

    // testing NamespaceOperations.setProperty
    final String ns3 = names[9];
    final String table8 = ns3 + "." + names[10];
    nops.create(ns3);
    tops.create(table8);
    tops.attachIterator(table8, iter1);
    testSameIterNoConflict(() -> nops.setProperty(ns3, iter1Key, iter1Val),
        () -> nops.setProperty(ns3, defaultIterKey, defaultIterVal));

    // testing NamespaceOperations.modifyProperties
    final String ns4 = names[11];
    final String table9 = ns4 + "." + names[12];
    nops.create(ns4);
    tops.create(table9);
    tops.attachIterator(table9, iter1);
    testSameIterNoConflict(() -> nops.setProperty(ns4, iter1Key, iter1Val),
        () -> nops.setProperty(ns4, defaultIterKey, defaultIterVal));
  }

  private void testSameIterNoConflict(Executable addIter1Executable,
      Executable addDefaultIterExecutable) throws Throwable {
    // should be able to add same exact iterator
    addIter1Executable.execute();
    addDefaultIterExecutable.execute();
  }
}
