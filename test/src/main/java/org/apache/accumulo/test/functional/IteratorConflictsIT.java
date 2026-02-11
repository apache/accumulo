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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.util.Wait;
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
 * - {@link CloneConfiguration.Builder#setPropertiesToSet(Map)}
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
  // doesn't matter what iterator is used here
  private static final String iterClass = SlowIterator.class.getName();
  private static final IteratorSetting iter1 = new IteratorSetting(99, "iter1name", iterClass);
  private static final String iter1Key = Property.TABLE_ITERATOR_PREFIX
      + IteratorScope.scan.name().toLowerCase() + "." + iter1.getName();
  private static final String iter1Val = "99," + iterClass;
  private static final IteratorSetting iter1PrioConflict =
      new IteratorSetting(99, "othername", iterClass);
  private static final IteratorSetting iter1NameConflict =
      new IteratorSetting(101, iter1.getName(), iterClass);
  private static final String iter1PrioConflictKey =
      Property.TABLE_ITERATOR_PREFIX + IteratorScope.scan.name().toLowerCase() + ".othername";
  private static final String iter1PrioConflictVal = "99," + iterClass;
  private static final String iter1NameConflictKey = Property.TABLE_ITERATOR_PREFIX
      + IteratorScope.scan.name().toLowerCase() + "." + iter1.getName();
  private static final String iter1NameConflictVal = "101," + iterClass;
  private static final IteratorSetting defaultIterPrioConflict =
      new IteratorSetting(20, "bar", iterClass);
  private static final IteratorSetting defaultIterNameConflict =
      new IteratorSetting(101, "vers", iterClass);
  private static final IteratorSetting defaultTableIter =
      IteratorConfigUtil.getInitialTableIteratorSettings().keySet().iterator().next();
  private static final String defaultIterPrioConflictKey =
      Property.TABLE_ITERATOR_PREFIX + IteratorScope.scan.name().toLowerCase() + ".foo";
  private static final String defaultIterPrioConflictVal =
      defaultTableIter.getPriority() + "," + iterClass;
  private static final String defaultIterNameConflictKey = Property.TABLE_ITERATOR_PREFIX
      + IteratorScope.scan.name().toLowerCase() + "." + defaultTableIter.getName();
  private static final String defaultIterNameConflictVal = "99," + iterClass;
  private static final String defaultIterKey = Property.TABLE_ITERATOR_PREFIX.getKey()
      + IteratorScope.scan.name().toLowerCase() + "." + defaultTableIter.getName();
  private static final String defaultIterVal =
      defaultTableIter.getPriority() + "," + defaultTableIter.getIteratorClass();
  private static final String defaultIterOptKey = Property.TABLE_ITERATOR_PREFIX.getKey()
      + IteratorScope.scan.name().toLowerCase() + "." + defaultTableIter.getName() + ".opt."
      + defaultTableIter.getOptions().entrySet().iterator().next().getKey();
  private static final String defaultIterOptVal =
      defaultTableIter.getOptions().entrySet().iterator().next().getValue();

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
  public void testTableIterConflict() throws Throwable {
    final String[] tableNames = getUniqueNames(13);
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
    String table8 = tableNames[10];
    for (String ns : List.of(ns5, ns6, ns7)) {
      nops.create(ns);
    }
    for (String table : List.of(table1, table2, table3, table4, table5, table6, table7, table8)) {
      tops.create(table, new NewTableConfiguration().attachIterator(iter1));
    }

    // testing Scanner.addScanIterator
    try (var scanner1 = client.createScanner(table1); var scanner2 = client.createScanner(table1)) {
      testTableIterPrioConflict(table1, RuntimeException.class, () -> {
        scanner1.addScanIterator(iter1PrioConflict);
        scanner1.iterator().hasNext();
      }, false);
      testTableIterNameConflict(table1, RuntimeException.class, () -> {
        scanner2.addScanIterator(iter1NameConflict);
        scanner2.iterator().hasNext();
      }, false);
    }

    // testing TableOperations.setProperty
    testTableIterPrioConflict(table2, AccumuloException.class,
        () -> tops.setProperty(table2, iter1PrioConflictKey, iter1PrioConflictVal), true);

    // testing TableOperations.modifyProperties
    testTableIterPrioConflict(table3, AccumuloException.class, () -> tops.modifyProperties(table3,
        props -> props.put(iter1PrioConflictKey, iter1PrioConflictVal)), true);

    // NewTableConfiguration.attachIterator is not applicable for this test
    // Attaching the iterator to the table requires the table to exist, but testing
    // NewTableConfiguration.attachIterator requires that the table does not exist

    // testing TableOperations.attachIterator
    testTableIterPrioConflict(table4, AccumuloException.class,
        () -> tops.attachIterator(table4, iter1PrioConflict), true);
    testTableIterNameConflict(table4, AccumuloException.class,
        () -> tops.attachIterator(table4, iter1NameConflict), true);

    // testing NamespaceOperations.attachIterator
    testTableIterPrioConflict(table5, IllegalStateException.class, () -> {
      nops.attachIterator(ns5, iter1PrioConflict);
      try (var scanner = client.createScanner(table5)) {
        assertFalse(scanner.iterator().hasNext());
      }
    }, false);

    // testing NamespaceOperations.setProperty
    testTableIterPrioConflict(table6, IllegalStateException.class, () -> {
      nops.setProperty(ns6, iter1PrioConflictKey, iter1PrioConflictVal);
      try (var scanner = client.createScanner(table6)) {
        assertFalse(scanner.iterator().hasNext());
      }
    }, false);

    // testing NamespaceOperations.modifyProperties
    testTableIterPrioConflict(table7, IllegalStateException.class, () -> {
      nops.modifyProperties(ns7, props -> props.put(iter1PrioConflictKey, iter1PrioConflictVal));
      try (var scanner = client.createScanner(table7)) {
        assertFalse(scanner.iterator().hasNext());
      }
    }, false);

    // testing CloneConfiguration.Builder.setPropertiesToSet
    String table9 = tableNames[11];
    testTableIterPrioConflict(table8, AccumuloException.class,
        () -> tops.clone(table8, table9,
            CloneConfiguration.builder()
                .setPropertiesToSet(Map.of(iter1PrioConflictKey, iter1PrioConflictVal)).build()),
        true);
  }

  private <T extends Exception> void testTableIterNameConflict(String table,
      Class<T> exceptionClass, Executable iterNameConflictExec, boolean checkMessage)
      throws Throwable {
    var e = assertThrows(exceptionClass, iterNameConflictExec);
    if (checkMessage) {
      assertTrue(e.toString().contains("iterator name conflict"));
    }
    assertEquals(Set.of(iter1.getName(), "vers"), tops.listIterators(table).keySet());
    for (var scope : IteratorScope.values()) {
      assertEquals(iter1, tops.getIteratorSetting(table, iter1.getName(), scope));
    }
  }

  private <T extends Exception> void testTableIterPrioConflict(String table,
      Class<T> exceptionClass, Executable iterPrioConflictExec, boolean checkMessage)
      throws Throwable {
    var e = assertThrows(exceptionClass, iterPrioConflictExec);
    if (checkMessage) {
      assertTrue(e.toString().contains("iterator priority conflict"));
      assertEquals(Set.of(iter1.getName(), "vers"), tops.listIterators(table).keySet());
      for (var scope : IteratorScope.values()) {
        assertEquals(iter1, tops.getIteratorSetting(table, iter1.getName(), scope));
      }
    }
  }

  @Test
  public void testNamespaceIterConflict() throws Throwable {
    final String[] names = getUniqueNames(28);

    // testing Scanner.addScanIterator
    String ns1 = names[0];
    nops.create(ns1);
    String table1 = ns1 + "." + names[1];
    tops.create(table1);
    try (var scanner1 = client.createScanner(table1); var scanner2 = client.createScanner(table1)) {
      testNamespaceIterPrioConflict(ns1, RuntimeException.class, () -> {
        scanner1.addScanIterator(iter1PrioConflict);
        scanner1.iterator().hasNext();
      }, false);
      testNamespaceIterNameConflict(ns1, RuntimeException.class, () -> {
        scanner2.addScanIterator(iter1NameConflict);
        scanner2.iterator().hasNext();
      }, false);
    }

    // testing TableOperations.setProperty
    String ns2 = names[2];
    nops.create(ns2);
    String table2 = ns2 + "." + names[3];
    tops.create(table2);
    testNamespaceIterPrioConflict(ns2, AccumuloException.class,
        () -> tops.setProperty(table2, iter1PrioConflictKey, iter1PrioConflictVal), true);

    // testing TableOperations.modifyProperties
    String ns3 = names[4];
    nops.create(ns3);
    String table3 = ns3 + "." + names[5];
    tops.create(table3);
    testNamespaceIterPrioConflict(ns3, AccumuloException.class, () -> tops.modifyProperties(table3,
        props -> props.put(iter1PrioConflictKey, iter1PrioConflictVal)), true);

    // testing NewTableConfiguration.attachIterator
    String ns4 = names[6];
    nops.create(ns4);
    String table4 = ns4 + "." + names[7];
    testNamespaceIterPrioConflict(ns4, AccumuloException.class,
        () -> tops.create(table4, new NewTableConfiguration().attachIterator(iter1PrioConflict)),
        true);

    // testing TableOperations.attachIterator
    String ns5 = names[9];
    nops.create(ns5);
    String table6 = ns5 + "." + names[10];
    tops.create(table6);
    testNamespaceIterPrioConflict(ns5, AccumuloException.class,
        () -> tops.attachIterator(table6, iter1PrioConflict), true);
    testNamespaceIterNameConflict(ns5, AccumuloException.class,
        () -> tops.attachIterator(table6, iter1NameConflict), true);

    // testing NamespaceOperations.attachIterator
    String ns6 = names[11];
    nops.create(ns6);
    testNamespaceIterPrioConflict(ns6, AccumuloException.class,
        () -> nops.attachIterator(ns6, iter1PrioConflict), true);
    testNamespaceIterNameConflict(ns6, AccumuloException.class,
        () -> nops.attachIterator(ns6, iter1NameConflict), true);

    // testing NamespaceOperations.setProperty
    String ns7 = names[12];
    nops.create(ns7);
    testNamespaceIterPrioConflict(ns7, AccumuloException.class,
        () -> nops.setProperty(ns7, iter1PrioConflictKey, iter1PrioConflictVal), true);

    // testing NamespaceOperations.modifyProperties
    String ns8 = names[13];
    nops.create(ns8);
    testNamespaceIterPrioConflict(ns8, AccumuloException.class,
        () -> nops.modifyProperties(ns8,
            props -> props.put(iter1PrioConflictKey, iter1PrioConflictVal)),

        true);

    // testing CloneConfiguration.Builder.setPropertiesToSet
    // testing same src and dst namespace: should conflict
    String dstAndSrcNamespace1 = names[14];
    nops.create(dstAndSrcNamespace1);
    String src1 = dstAndSrcNamespace1 + "." + names[15];
    tops.create(src1);
    String dst1 = dstAndSrcNamespace1 + "." + names[16];
    String dst2 = dstAndSrcNamespace1 + "." + names[17];
    testNamespaceIterPrioConflict(dstAndSrcNamespace1, AccumuloException.class,
        () -> tops.clone(src1, dst1,
            CloneConfiguration.builder()
                .setPropertiesToSet(Map.of(iter1PrioConflictKey, iter1PrioConflictVal)).build()),
        true);
    // testing attached to src namespace, different dst namespace: should not conflict
    String srcNamespace2 = names[18];
    nops.create(srcNamespace2);
    nops.attachIterator(srcNamespace2, iter1);
    String src2 = srcNamespace2 + "." + names[19];
    tops.create(src2);
    String dstNamespace2 = names[20];
    nops.create(dstNamespace2);
    String dst3 = dstNamespace2 + "." + names[21];
    String dst4 = dstNamespace2 + "." + names[22];
    // should NOT throw
    tops.clone(src2, dst3, CloneConfiguration.builder()
        .setPropertiesToSet(Map.of(iter1PrioConflictKey, iter1PrioConflictVal)).build());
    // should NOT throw
    tops.clone(src2, dst4, CloneConfiguration.builder()
        .setPropertiesToSet(Map.of(iter1NameConflictKey, iter1NameConflictVal)).build());
    // testing attached to dst namespace, different src namespace: should conflict
    String srcNamespace3 = names[23];
    nops.create(srcNamespace3);
    String src3 = srcNamespace3 + "." + names[24];
    tops.create(src3);
    String dstNamespace3 = names[25];
    nops.create(dstNamespace3);
    String dst5 = dstNamespace3 + "." + names[26];
    testNamespaceIterPrioConflict(dstNamespace3, AccumuloException.class,
        () -> tops.clone(src3, dst5,
            CloneConfiguration.builder()
                .setPropertiesToSet(Map.of(iter1PrioConflictKey, iter1PrioConflictVal)).build()),
        true);
  }

  private <T extends Exception> void testNamespaceIterPrioConflict(String ns,
      Class<T> exceptionClass, Executable iterPrioConflictExec, boolean checkMessage)
      throws Throwable {
    nops.attachIterator(ns, iter1);
    Wait.waitFor(() -> nops.listIterators(ns).containsKey(iter1.getName()));
    var e = assertThrows(exceptionClass, iterPrioConflictExec);
    if (checkMessage) {
      assertTrue(e.toString().contains("iterator priority conflict"), e::getMessage);
    }
    assertEquals(Set.of(iter1.getName()), nops.listIterators(ns).keySet());
    for (var scope : IteratorScope.values()) {
      assertEquals(iter1, nops.getIteratorSetting(ns, iter1.getName(), scope));
    }
  }

  private <T extends Exception> void testNamespaceIterNameConflict(String ns,
      Class<T> exceptionClass, Executable iterPrioConflictExec, boolean checkMessage)
      throws Throwable {
    nops.attachIterator(ns, iter1);
    Wait.waitFor(() -> nops.listIterators(ns).containsKey(iter1.getName()));

    var e = assertThrows(exceptionClass, iterPrioConflictExec);
    if (checkMessage) {
      assertTrue(e.toString().contains("iterator name conflict"));
    }
    assertEquals(Set.of(iter1.getName()), nops.listIterators(ns).keySet());
    for (var scope : IteratorScope.values()) {
      assertEquals(iter1, nops.getIteratorSetting(ns, iter1.getName(), scope));
    }
  }

  @Test
  public void testDefaultIterConflict() throws Throwable {
    final String[] names = getUniqueNames(29);

    // testing Scanner.addScanIterator
    String defaultsTable1 = names[0];
    tops.create(defaultsTable1);
    String noDefaultsTable1 = names[1];
    tops.create(noDefaultsTable1, new NewTableConfiguration().withoutDefaults());
    try (var defaultsScanner1 = client.createScanner(defaultsTable1);
        var noDefaultsScanner1 = client.createScanner(noDefaultsTable1);
        var defaultsScanner2 = client.createScanner(defaultsTable1);
        var noDefaultsScanner2 = client.createScanner(noDefaultsTable1)) {
      testDefaultIterConflict(RuntimeException.class, () -> {
        defaultsScanner1.addScanIterator(defaultIterPrioConflict);
        defaultsScanner1.iterator().hasNext();
      }, () -> {
        defaultsScanner2.addScanIterator(defaultIterNameConflict);
        defaultsScanner2.iterator().hasNext();
      }, () -> {
        noDefaultsScanner1.addScanIterator(defaultIterPrioConflict);
        noDefaultsScanner1.iterator().hasNext();
      }, () -> {
        noDefaultsScanner2.addScanIterator(defaultIterNameConflict);
        noDefaultsScanner2.iterator().hasNext();
      }, false);
    }

    // testing TableOperations.setProperty
    String defaultsTable2 = names[2];
    tops.create(defaultsTable2);
    String noDefaultsTable2 = names[3];
    tops.create(noDefaultsTable2, new NewTableConfiguration().withoutDefaults());
    testDefaultIterConflict(AccumuloException.class,
        () -> tops
            .setProperty(defaultsTable2, defaultIterPrioConflictKey, defaultIterPrioConflictVal),
        null,
        () -> tops.setProperty(noDefaultsTable2, defaultIterPrioConflictKey,
            defaultIterPrioConflictVal),
        () -> tops.setProperty(noDefaultsTable2, defaultIterNameConflictKey,
            defaultIterNameConflictVal),
        true);

    // testing TableOperations.modifyProperties
    String defaultsTable3 = names[4];
    tops.create(defaultsTable3);
    String noDefaultsTable3 = names[5];
    tops.create(noDefaultsTable3, new NewTableConfiguration().withoutDefaults());
    testDefaultIterConflict(AccumuloException.class,
        () -> tops.modifyProperties(defaultsTable3,
            props -> props.put(defaultIterPrioConflictKey, defaultIterPrioConflictVal)),
        null,
        () -> tops.modifyProperties(noDefaultsTable3,
            props -> props.put(defaultIterPrioConflictKey, defaultIterPrioConflictVal)),
        () -> tops.modifyProperties(noDefaultsTable3,
            props -> props.put(defaultIterNameConflictKey, defaultIterNameConflictVal)),
        true);

    // testing NewTableConfiguration.attachIterator
    String defaultsTable4 = names[6];
    String noDefaultsTable4 = names[7];
    String noDefaultsTable5 = names[8];
    testDefaultIterConflict(IllegalArgumentException.class,
        () -> tops.create(defaultsTable4,
            new NewTableConfiguration().attachIterator(defaultIterPrioConflict)),
        () -> tops.create(defaultsTable4,
            new NewTableConfiguration().attachIterator(defaultIterNameConflict)),
        () -> tops.create(noDefaultsTable4,
            new NewTableConfiguration().attachIterator(defaultIterPrioConflict).withoutDefaults()),
        () -> tops.create(noDefaultsTable5,
            new NewTableConfiguration().attachIterator(defaultIterNameConflict).withoutDefaults()),
        true);

    // testing TableOperations.attachIterator
    String defaultsTable6 = names[9];
    tops.create(defaultsTable6);
    String noDefaultsTable6 = names[10];
    tops.create(noDefaultsTable6, new NewTableConfiguration().withoutDefaults());
    testDefaultIterConflict(AccumuloException.class,
        () -> tops.attachIterator(defaultsTable6, defaultIterPrioConflict),
        () -> tops.attachIterator(defaultsTable6, defaultIterNameConflict),
        () -> tops.attachIterator(noDefaultsTable6, defaultIterPrioConflict),
        () -> tops.attachIterator(noDefaultsTable6, defaultIterNameConflict), true);

    // testing NamespaceOperations.attachIterator
    String ns7 = names[11];
    nops.create(ns7);
    String defaultsTable7 = ns7 + "." + names[12];
    tops.create(defaultsTable7);
    String ns8 = names[13];
    nops.create(ns8);
    String noDefaultsTable8 = ns8 + "." + names[14];
    tops.create(noDefaultsTable8, new NewTableConfiguration().withoutDefaults());
    testDefaultIterConflict(IllegalStateException.class, () -> {
      nops.attachIterator(ns7, defaultIterPrioConflict);
      try (var scanner = client.createScanner(defaultsTable7)) {
        assertFalse(scanner.iterator().hasNext());
      }
    }, null, () -> nops.attachIterator(ns8, defaultIterPrioConflict),
        () -> nops.attachIterator(ns8, defaultIterNameConflict), false);

    // testing NamespaceOperations.setProperty
    String ns9 = names[15];
    nops.create(ns9);
    String defaultsTable9 = ns9 + "." + names[16];
    tops.create(defaultsTable9);
    String ns10 = names[17];
    nops.create(ns10);
    String noDefaultsTable10 = ns10 + "." + names[18];
    tops.create(noDefaultsTable10, new NewTableConfiguration().withoutDefaults());
    testDefaultIterConflict(IllegalStateException.class, () -> {
      nops.setProperty(ns9, defaultIterPrioConflictKey, defaultIterPrioConflictVal);
      try (var scanner = client.createScanner(defaultsTable9)) {
        assertFalse(scanner.iterator().hasNext());
      }
    }, null, () -> nops.setProperty(ns10, defaultIterPrioConflictKey, defaultIterPrioConflictVal),
        () -> nops.setProperty(ns10, defaultIterNameConflictKey, defaultIterNameConflictVal),
        false);

    // testing NamespaceOperations.modifyProperties
    String ns11 = names[19];
    nops.create(ns11);
    String defaultsTable11 = ns11 + "." + names[20];
    tops.create(defaultsTable11);
    String ns12 = names[21];
    nops.create(ns12);
    String noDefaultsTable12 = ns12 + "." + names[22];
    tops.create(noDefaultsTable12, new NewTableConfiguration().withoutDefaults());
    testDefaultIterConflict(IllegalStateException.class, () -> {
      nops.modifyProperties(ns11,
          props -> props.put(defaultIterPrioConflictKey, defaultIterPrioConflictVal));
      try (var scanner = client.createScanner(defaultsTable11)) {
        assertFalse(scanner.iterator().hasNext());
      }
    }, null,
        () -> nops.modifyProperties(ns12,
            props -> props.put(defaultIterPrioConflictKey, defaultIterPrioConflictVal)),
        () -> nops.modifyProperties(ns12,
            props -> props.put(defaultIterNameConflictKey, defaultIterNameConflictVal)),
        false);

    // testing CloneConfiguration.Builder.setPropertiesToSet
    String dst1 = names[23];
    String dst2 = names[24];
    String dst3 = names[25];
    String dst4 = names[26];
    String defaultsTable12 = names[27];
    tops.create(defaultsTable12);
    String noDefaultsTable13 = names[28];
    tops.create(noDefaultsTable13, new NewTableConfiguration().withoutDefaults());
    testDefaultIterConflict(AccumuloException.class,
        () -> tops
            .clone(defaultsTable12, dst1,
                CloneConfiguration.builder()
                    .setPropertiesToSet(
                        Map.of(defaultIterPrioConflictKey, defaultIterPrioConflictVal))
                    .build()),
        null,
        () -> tops
            .clone(noDefaultsTable13, dst3,
                CloneConfiguration.builder()
                    .setPropertiesToSet(
                        Map.of(defaultIterPrioConflictKey, defaultIterPrioConflictVal))
                    .build()),
        () -> tops.clone(noDefaultsTable13, dst4,
            CloneConfiguration.builder()
                .setPropertiesToSet(Map.of(defaultIterNameConflictKey, defaultIterNameConflictVal))
                .build()),
        true);
  }

  private <T extends Exception> void testDefaultIterConflict(Class<T> exceptionClass,
      Executable defaultsTableOp1, Executable defaultsTableOp2, Executable noDefaultsTableOp1,
      Executable noDefaultsTableOp2, boolean checkMessage) throws Throwable {
    var e = assertThrows(exceptionClass, defaultsTableOp1);
    if (checkMessage) {
      assertTrue(e.toString().contains("conflict with default table iterator")
          || e.toString().contains("iterator priority conflict"));
    }
    if (defaultsTableOp2 != null) {
      e = assertThrows(exceptionClass, defaultsTableOp2);
      if (checkMessage) {
        assertTrue(e.toString().contains("conflict with default table iterator")
            || e.toString().contains("iterator name conflict"));
      }
    }

    noDefaultsTableOp1.execute(); // should NOT fail
    noDefaultsTableOp2.execute(); // should NOT fail
  }

  @Test
  public void testSameIterNoConflict() throws Throwable {
    // note about setProperty calls in this test. The default table iter has an option so the
    // property representation of this iter is a property for the iter and a property for the
    // option (2 properties). Obviously we cannot call setProperty with both of these properties,
    // but calling setProperty for one of these properties should be fine as it has no effect on
    // the config.
    final String[] names = getUniqueNames(16);

    // testing Scanner.addScanIterator
    final String table1 = names[0];
    tops.create(table1, new NewTableConfiguration().attachIterator(iter1));
    try (var scanner1 = client.createScanner(table1); var scanner2 = client.createScanner(table1)) {
      testSameIterNoConflict(() -> {
        scanner1.addScanIterator(iter1);
        scanner1.iterator().hasNext();
      }, () -> {
        scanner2.addScanIterator(defaultTableIter);
        scanner2.iterator().hasNext();
      });
    }

    // testing TableOperations.setProperty
    final String table2 = names[1];
    tops.create(table2, new NewTableConfiguration().attachIterator(iter1));
    testSameIterNoConflict(() -> tops.setProperty(table2, iter1Key, iter1Val),
        () -> tops.setProperty(table2, defaultIterKey, defaultIterVal));

    // testing TableOperations.modifyProperties
    final String table3 = names[2];
    tops.create(table3, new NewTableConfiguration().attachIterator(iter1));
    testSameIterNoConflict(
        () -> tops.modifyProperties(table3, props -> props.put(iter1Key, iter1Val)),
        () -> tops.modifyProperties(table3, props -> {
          props.put(defaultIterKey, defaultIterVal);
          props.put(defaultIterOptKey, defaultIterOptVal);
        }));

    // testing NewTableConfiguration.attachIterator
    final String ns1 = names[3];
    final String table4 = ns1 + "." + names[4];
    final String table5 = names[5];
    nops.create(ns1);
    nops.attachIterator(ns1, iter1);
    Wait.waitFor(() -> nops.listIterators(ns1).containsKey(iter1.getName()));
    testSameIterNoConflict(
        () -> tops.create(table4, new NewTableConfiguration().attachIterator(iter1)),
        () -> tops.create(table5, new NewTableConfiguration().attachIterator(defaultTableIter)));

    // testing TableOperations.attachIterator
    final String table6 = names[6];
    tops.create(table6, new NewTableConfiguration().attachIterator(iter1));
    testSameIterNoConflict(() -> tops.attachIterator(table6, iter1),
        () -> tops.attachIterator(table6, defaultTableIter));

    // testing NamespaceOperations.attachIterator
    final String ns2 = names[7];
    final String table7 = ns2 + "." + names[8];
    nops.create(ns2);
    tops.create(table7, new NewTableConfiguration().attachIterator(iter1));
    testSameIterNoConflict(() -> nops.attachIterator(ns2, iter1),
        () -> nops.attachIterator(ns2, defaultTableIter));

    // testing NamespaceOperations.setProperty
    final String ns3 = names[9];
    final String table8 = ns3 + "." + names[10];
    nops.create(ns3);
    tops.create(table8, new NewTableConfiguration().attachIterator(iter1));
    testSameIterNoConflict(() -> nops.setProperty(ns3, iter1Key, iter1Val),
        () -> nops.setProperty(ns3, defaultIterKey, defaultIterVal));

    // testing NamespaceOperations.modifyProperties
    final String ns4 = names[11];
    final String table9 = ns4 + "." + names[12];
    nops.create(ns4);
    tops.create(table9, new NewTableConfiguration().attachIterator(iter1));
    testSameIterNoConflict(() -> nops.modifyProperties(ns4, props -> props.put(iter1Key, iter1Val)),
        () -> nops.modifyProperties(ns4, props -> {
          props.put(defaultIterKey, defaultIterVal);
          props.put(defaultIterOptKey, defaultIterOptVal);
        }));

    // testing CloneConfiguration.Builder.setPropertiesToSet
    final String src = names[13];
    final String dst1 = names[14];
    final String dst2 = names[15];
    tops.create(src, new NewTableConfiguration().attachIterator(iter1));
    testSameIterNoConflict(
        () -> tops.clone(src, dst1,
            CloneConfiguration.builder().setPropertiesToSet(Map.of(iter1Key, iter1Val)).build()),
        () -> tops.clone(src, dst2,
            CloneConfiguration.builder()
                .setPropertiesToSet(
                    Map.of(defaultIterKey, defaultIterVal, defaultIterOptKey, defaultIterOptVal))
                .build()));
  }

  private void testSameIterNoConflict(Executable addIter1Executable,
      Executable addDefaultIterExecutable) throws Throwable {
    // should be able to add same exact iterator
    addIter1Executable.execute();
    addDefaultIterExecutable.execute();
  }
}
