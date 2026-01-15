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

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
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
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + "." + iter1.getName();
  private static final String iter1Val = "99," + iterClass;
  private static final IteratorSetting iter1PrioConflict =
      new IteratorSetting(99, "othername", iterClass);
  private static final IteratorSetting iter1NameConflict =
      new IteratorSetting(101, iter1.getName(), iterClass);
  private static final String iter1PrioConflictKey = Property.TABLE_ITERATOR_PREFIX
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + ".othername";
  private static final String iter1PrioConflictVal = "99," + iterClass;
  private static final String iter1NameConflictKey = Property.TABLE_ITERATOR_PREFIX
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + "." + iter1.getName();
  private static final String iter1NameConflictVal = "101," + iterClass;
  private static final IteratorSetting defaultIterPrioConflict =
      new IteratorSetting(20, "bar", iterClass);
  private static final IteratorSetting defaultIterNameConflict =
      new IteratorSetting(101, "vers", iterClass);
  private static final IteratorSetting defaultTableIter =
      IteratorConfigUtil.getInitialTableIteratorSettings().keySet().iterator().next();
  private static final String defaultIterPrioConflictKey = Property.TABLE_ITERATOR_PREFIX
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + ".foo";
  private static final String defaultIterPrioConflictVal =
      defaultTableIter.getPriority() + "," + iterClass;
  private static final String defaultIterNameConflictKey = Property.TABLE_ITERATOR_PREFIX
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + "." + defaultTableIter.getName();
  private static final String defaultIterNameConflictVal = "99," + iterClass;
  private static final String defaultIterKey = Property.TABLE_ITERATOR_PREFIX.getKey()
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + "." + defaultTableIter.getName();
  private static final String defaultIterVal =
      defaultTableIter.getPriority() + "," + defaultTableIter.getIteratorClass();
  private static final String defaultIterOptKey = Property.TABLE_ITERATOR_PREFIX.getKey()
      + IteratorUtil.IteratorScope.scan.name().toLowerCase() + "." + defaultTableIter.getName()
      + ".opt." + defaultTableIter.getOptions().entrySet().iterator().next().getKey();
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
  public void testTableIterConflict() throws Exception {
    final String[] tableNames = getUniqueNames(12);
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
      tops.create(table);
    }

    // testing Scanner.addScanIterator
    try (var scanner1 = client.createScanner(table1); var scanner2 = client.createScanner(table1)) {
      testTableIterConflict(table1, RuntimeException.class, () -> {
        scanner1.addScanIterator(iter1PrioConflict);
        scanner1.iterator().hasNext();
      }, () -> {
        scanner2.addScanIterator(iter1NameConflict);
        scanner2.iterator().hasNext();
      });
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

    // testing CloneConfiguration.Builder.setPropertiesToSet
    String table9 = tableNames[11];
    testTableIterConflict(table8, AccumuloException.class,
        () -> tops.clone(table8, table9,
            CloneConfiguration.builder()
                .setPropertiesToSet(Map.of(iter1PrioConflictKey, iter1PrioConflictVal)).build()),
        () -> tops.clone(table8, table9, CloneConfiguration.builder()
            .setPropertiesToSet(Map.of(iter1NameConflictKey, iter1NameConflictVal)).build()));
  }

  private <T extends Exception> void testTableIterConflict(String table, Class<T> exceptionClass,
      Executable iterPrioConflictExec, Executable iterNameConflictExec) throws Exception {
    tops.attachIterator(table, iter1);
    assertThrows(exceptionClass, iterPrioConflictExec);
    assertThrows(exceptionClass, iterNameConflictExec);
  }

  @Test
  public void testNamespaceIterConflict() throws Exception {
    final String[] names = getUniqueNames(25);

    // testing Scanner.addScanIterator
    String ns1 = names[0];
    nops.create(ns1);
    String table1 = ns1 + "." + names[1];
    tops.create(table1);
    try (var scanner1 = client.createScanner(table1); var scanner2 = client.createScanner(table1)) {
      testNamespaceIterConflict(ns1, RuntimeException.class, () -> {
        scanner1.addScanIterator(iter1PrioConflict);
        scanner1.iterator().hasNext();
      }, () -> {
        scanner2.addScanIterator(iter1NameConflict);
        scanner2.iterator().hasNext();
      });
    }

    // testing TableOperations.setProperty
    String ns2 = names[2];
    nops.create(ns2);
    String table2 = ns2 + "." + names[3];
    tops.create(table2);
    testNamespaceIterConflict(ns2, AccumuloException.class,
        () -> tops.setProperty(table2, iter1PrioConflictKey, iter1PrioConflictVal),
        () -> tops.setProperty(table2, iter1NameConflictKey, iter1NameConflictVal));

    // testing TableOperations.modifyProperties
    String ns3 = names[4];
    nops.create(ns3);
    String table3 = ns3 + "." + names[5];
    tops.create(table3);
    testNamespaceIterConflict(ns3, AccumuloException.class,
        () -> tops.modifyProperties(table3,
            props -> props.put(iter1PrioConflictKey, iter1PrioConflictVal)),
        () -> tops.modifyProperties(table3,
            props -> props.put(iter1NameConflictKey, iter1NameConflictVal)));

    // testing NewTableConfiguration.attachIterator
    String ns4 = names[6];
    nops.create(ns4);
    String table4 = ns4 + "." + names[7];
    testNamespaceIterConflict(ns4, AccumuloException.class,
        () -> tops.create(table4, new NewTableConfiguration().attachIterator(iter1PrioConflict)),
        () -> tops.create(table4, new NewTableConfiguration().attachIterator(iter1NameConflict)));

    // testing TableOperations.attachIterator
    String ns5 = names[8];
    nops.create(ns5);
    String table5 = ns5 + "." + names[9];
    tops.create(table5);
    testNamespaceIterConflict(ns5, AccumuloException.class,
        () -> tops.attachIterator(table5, iter1PrioConflict),
        () -> tops.attachIterator(table5, iter1NameConflict));

    // testing NamespaceOperations.attachIterator
    String ns6 = names[10];
    nops.create(ns6);
    testNamespaceIterConflict(ns6, AccumuloException.class,
        () -> nops.attachIterator(ns6, iter1PrioConflict),
        () -> nops.attachIterator(ns6, iter1NameConflict));

    // testing NamespaceOperations.setProperty
    String ns7 = names[11];
    nops.create(ns7);
    testNamespaceIterConflict(ns7, AccumuloException.class,
        () -> nops.setProperty(ns7, iter1PrioConflictKey, iter1PrioConflictVal),
        () -> nops.setProperty(ns7, iter1NameConflictKey, iter1NameConflictVal));

    // testing NamespaceOperations.modifyProperties
    String ns8 = names[12];
    nops.create(ns8);
    testNamespaceIterConflict(ns8, AccumuloException.class,
        () -> nops.modifyProperties(ns8,
            props -> props.put(iter1PrioConflictKey, iter1PrioConflictVal)),
        () -> nops.modifyProperties(ns8,
            props -> props.put(iter1NameConflictKey, iter1NameConflictVal)));

    // testing CloneConfiguration.Builder.setPropertiesToSet
    // testing same src and dst namespace: should conflict
    String dstAndSrcNamespace1 = names[13];
    nops.create(dstAndSrcNamespace1);
    String src1 = dstAndSrcNamespace1 + "." + names[14];
    tops.create(src1);
    String dst1 = dstAndSrcNamespace1 + "." + names[15];
    testNamespaceIterConflict(dstAndSrcNamespace1, AccumuloException.class,
        () -> tops.clone(src1, dst1,
            CloneConfiguration.builder()
                .setPropertiesToSet(Map.of(iter1PrioConflictKey, iter1PrioConflictVal)).build()),
        () -> tops.clone(src1, dst1, CloneConfiguration.builder()
            .setPropertiesToSet(Map.of(iter1NameConflictKey, iter1NameConflictVal)).build()));
    // testing attached to src namespace, different dst namespace: should not conflict
    String srcNamespace2 = names[16];
    nops.create(srcNamespace2);
    nops.attachIterator(srcNamespace2, iter1);
    String src2 = srcNamespace2 + "." + names[17];
    tops.create(src2);
    String dstNamespace2 = names[18];
    nops.create(dstNamespace2);
    String dst2 = dstNamespace2 + "." + names[19];
    String dst3 = dstNamespace2 + "." + names[20];
    // should NOT throw
    tops.clone(src2, dst2, CloneConfiguration.builder()
        .setPropertiesToSet(Map.of(iter1PrioConflictKey, iter1PrioConflictVal)).build());
    // should NOT throw
    tops.clone(src2, dst3, CloneConfiguration.builder()
        .setPropertiesToSet(Map.of(iter1NameConflictKey, iter1NameConflictVal)).build());
    // testing attached to dst namespace, different src namespace: should conflict
    String srcNamespace3 = names[21];
    nops.create(srcNamespace3);
    String src3 = srcNamespace3 + "." + names[22];
    tops.create(src3);
    String dstNamespace3 = names[23];
    nops.create(dstNamespace3);
    String dst4 = dstNamespace3 + "." + names[24];
    testNamespaceIterConflict(dstNamespace3, AccumuloException.class,
        () -> tops.clone(src3, dst4,
            CloneConfiguration.builder()
                .setPropertiesToSet(Map.of(iter1PrioConflictKey, iter1PrioConflictVal)).build()),
        () -> tops.clone(src3, dst4, CloneConfiguration.builder()
            .setPropertiesToSet(Map.of(iter1NameConflictKey, iter1NameConflictVal)).build()));
  }

  private <T extends Exception> void testNamespaceIterConflict(String ns, Class<T> exceptionClass,
      Executable iterPrioConflictExec, Executable iterNameConflictExec) throws Exception {
    nops.attachIterator(ns, iter1);

    assertThrows(exceptionClass, iterPrioConflictExec);
    assertThrows(exceptionClass, iterNameConflictExec);
  }

  @Test
  public void testDefaultIterConflict() throws Throwable {
    final String[] tables = getUniqueNames(27);

    // testing Scanner.addScanIterator
    String defaultsTable1 = tables[0];
    tops.create(defaultsTable1);
    String noDefaultsTable1 = tables[1];
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
      });
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
    testDefaultIterConflict(IllegalArgumentException.class,
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
        () -> nops.modifyProperties(ns11,
            props -> props.put(defaultIterPrioConflictKey, defaultIterPrioConflictVal)),
        () -> nops.modifyProperties(ns11,
            props -> props.put(defaultIterNameConflictKey, defaultIterNameConflictVal)),
        () -> nops.modifyProperties(ns12,
            props -> props.put(defaultIterPrioConflictKey, defaultIterPrioConflictVal)),
        () -> nops.modifyProperties(ns12,
            props -> props.put(defaultIterNameConflictKey, defaultIterNameConflictVal)));

    // testing CloneConfiguration.Builder.setPropertiesToSet
    String dst1 = tables[23];
    String dst2 = tables[24];
    String defaultsTable12 = tables[25];
    tops.create(defaultsTable12);
    String noDefaultsTable13 = tables[26];
    tops.create(noDefaultsTable13, new NewTableConfiguration().withoutDefaults());
    testDefaultIterConflict(AccumuloException.class,
        () -> tops
            .clone(defaultsTable12, dst1,
                CloneConfiguration.builder()
                    .setPropertiesToSet(
                        Map.of(defaultIterPrioConflictKey, defaultIterPrioConflictVal))
                    .build()),
        () -> tops
            .clone(defaultsTable12, dst1,
                CloneConfiguration.builder()
                    .setPropertiesToSet(
                        Map.of(defaultIterNameConflictKey, defaultIterNameConflictVal))
                    .build()),
        () -> tops
            .clone(noDefaultsTable13, dst1,
                CloneConfiguration.builder()
                    .setPropertiesToSet(
                        Map.of(defaultIterPrioConflictKey, defaultIterPrioConflictVal))
                    .build()),
        () -> tops.clone(noDefaultsTable13, dst2,
            CloneConfiguration.builder()
                .setPropertiesToSet(Map.of(defaultIterNameConflictKey, defaultIterNameConflictVal))
                .build()));
  }

  private <T extends Exception> void testDefaultIterConflict(Class<T> exceptionClass,
      Executable defaultsTableOp1, Executable defaultsTableOp2, Executable noDefaultsTableOp1,
      Executable noDefaultsTableOp2) throws Throwable {
    assertThrows(exceptionClass, defaultsTableOp1);
    assertThrows(exceptionClass, defaultsTableOp2);

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
    tops.create(table1);
    tops.attachIterator(table1, iter1);
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
    tops.create(table2);
    tops.attachIterator(table2, iter1);
    testSameIterNoConflict(() -> tops.setProperty(table2, iter1Key, iter1Val),
        () -> tops.setProperty(table2, defaultIterKey, defaultIterVal));

    // testing TableOperations.modifyProperties
    final String table3 = names[2];
    tops.create(table3);
    tops.attachIterator(table3, iter1);
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
    testSameIterNoConflict(() -> nops.modifyProperties(ns4, props -> props.put(iter1Key, iter1Val)),
        () -> nops.modifyProperties(ns4, props -> {
          props.put(defaultIterKey, defaultIterVal);
          props.put(defaultIterOptKey, defaultIterOptVal);
        }));

    // testing CloneConfiguration.Builder.setPropertiesToSet
    final String src = names[13];
    final String dst1 = names[14];
    final String dst2 = names[15];
    tops.create(src);
    tops.attachIterator(src, iter1);
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
