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
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ImportConfiguration;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.constraints.NumericValueConstraint;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Test different namespace permissions
 */
@Tag(MINI_CLUSTER_ONLY)
public class NamespacesIT extends SharedMiniClusterBase {

  private AccumuloClient c;
  private String namespace;
  private static final int MAX_NAMESPACE_LEN = 1024;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @BeforeEach
  public void setupConnectorAndNamespace() {
    // prepare a unique namespace and get a new root client for each test
    c = Accumulo.newClient().from(getClientProps()).build();
    namespace = "ns_" + getUniqueNames(1)[0];
  }

  @AfterEach
  public void swingMjölnir() throws Exception {
    if (c == null) {
      return;
    }
    // clean up any added tables, namespaces, and users, after each test
    for (String t : c.tableOperations().list()) {
      if (!TableNameUtil.qualify(t).getFirst().equals(Namespace.ACCUMULO.name())) {
        c.tableOperations().delete(t);
      }
    }
    assertEquals(2, c.tableOperations().list().size());
    for (String n : c.namespaceOperations().list()) {
      if (!n.equals(Namespace.ACCUMULO.name()) && !n.equals(Namespace.DEFAULT.name())) {
        c.namespaceOperations().delete(n);
      }
    }
    assertEquals(2, c.namespaceOperations().list().size());
    for (String u : c.securityOperations().listLocalUsers()) {
      if (!getAdminPrincipal().equals(u)) {
        c.securityOperations().dropLocalUser(u);
      }
    }
    assertEquals(1, c.securityOperations().listLocalUsers().size());

    c.close();
  }

  @Test
  public void checkReservedNamespaces() {
    assertEquals(c.namespaceOperations().defaultNamespace(), Namespace.DEFAULT.name());
    assertEquals(c.namespaceOperations().systemNamespace(), Namespace.ACCUMULO.name());
  }

  @Test
  public void checkBuiltInNamespaces() throws Exception {
    assertTrue(c.namespaceOperations().exists(Namespace.DEFAULT.name()));
    assertTrue(c.namespaceOperations().exists(Namespace.ACCUMULO.name()));
  }

  @Test
  public void createTableInDefaultNamespace() throws Exception {
    String tableName = "1";
    c.tableOperations().create(tableName);
    assertTrue(c.tableOperations().exists(tableName));
  }

  @Test
  public void createTableInAccumuloNamespace() {
    String tableName = Namespace.ACCUMULO.name() + ".1";
    assertFalse(c.tableOperations().exists(tableName));
    assertThrows(AccumuloException.class, () -> c.tableOperations().create(tableName));
  }

  @Test
  public void deleteBuiltinNamespaces() {
    assertThrows(AccumuloSecurityException.class,
        () -> c.namespaceOperations().delete(Namespace.DEFAULT.name()));
    assertThrows(AccumuloSecurityException.class,
        () -> c.namespaceOperations().delete(Namespace.ACCUMULO.name()));
  }

  @Test
  public void createTableInMissingNamespace() throws Exception {
    String t = namespace + ".1";
    assertFalse(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(t));
    var e = assertThrows(AccumuloException.class, () -> c.tableOperations().create(t));
    assertEquals(NamespaceNotFoundException.class, e.getCause().getClass());
    assertFalse(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(t));
  }

  @Test
  public void createNamespaceWithNamespaceLengthLimit()
      throws AccumuloException, AccumuloSecurityException, NamespaceExistsException {
    NamespaceOperations nsOps = c.namespaceOperations();
    String n0 = StringUtils.repeat('a', MAX_NAMESPACE_LEN - 1);
    nsOps.create(n0);
    assertTrue(nsOps.exists(n0));

    String n1 = StringUtils.repeat('b', MAX_NAMESPACE_LEN);
    nsOps.create(n1);
    assertTrue(nsOps.exists(n1));

    String n2 = StringUtils.repeat('c', MAX_NAMESPACE_LEN + 1);
    assertThrows(IllegalArgumentException.class, () -> nsOps.create(n2));
    assertFalse(nsOps.exists(n2));
  }

  @Test
  public void createAndDeleteNamespace() throws Exception {
    String t1 = namespace + ".1";
    String t2 = namespace + ".2";
    assertFalse(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(t1));
    assertFalse(c.tableOperations().exists(t2));
    assertThrows(NamespaceNotFoundException.class, () -> c.namespaceOperations().delete(namespace));
    var e = assertThrows(TableNotFoundException.class, () -> c.tableOperations().delete(t1));
    assertEquals(NamespaceNotFoundException.class, e.getCause().getClass());
    c.namespaceOperations().create(namespace);
    assertTrue(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(t1));
    assertFalse(c.tableOperations().exists(t2));
    c.tableOperations().create(t1);
    assertTrue(c.namespaceOperations().exists(namespace));
    assertTrue(c.tableOperations().exists(t1));
    assertFalse(c.tableOperations().exists(t2));
    c.tableOperations().create(t2);
    assertTrue(c.namespaceOperations().exists(namespace));
    assertTrue(c.tableOperations().exists(t1));
    assertTrue(c.tableOperations().exists(t2));
    c.tableOperations().delete(t1);
    assertTrue(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(t1));
    assertTrue(c.tableOperations().exists(t2));
    c.tableOperations().delete(t2);
    assertTrue(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(t1));
    assertFalse(c.tableOperations().exists(t2));
    c.namespaceOperations().delete(namespace);
    assertFalse(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(t1));
    assertFalse(c.tableOperations().exists(t2));
  }

  @Test
  public void deleteNonEmptyNamespace() throws Exception {
    String tableName1 = namespace + ".1";
    assertFalse(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(tableName1));
    c.namespaceOperations().create(namespace);
    c.tableOperations().create(tableName1);
    assertTrue(c.namespaceOperations().exists(namespace));
    assertTrue(c.tableOperations().exists(tableName1));
    assertThrows(NamespaceNotEmptyException.class, () -> c.namespaceOperations().delete(namespace));
  }

  @Test
  public void verifyPropertyInheritance() throws Exception {

    try (AccumuloClient client =
        getCluster().createAccumuloClient(getPrincipal(), new PasswordToken(getRootPassword()))) {
      client.securityOperations().grantNamespacePermission(getPrincipal(), "",
          NamespacePermission.ALTER_NAMESPACE);
    }

    String t0 = "0";
    String t1 = namespace + ".1";
    String t2 = namespace + ".2";

    String k = Property.TABLE_SCAN_MAXMEM.getKey();
    String v = "42K";

    assertFalse(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(t1));
    assertFalse(c.tableOperations().exists(t2));
    c.namespaceOperations().create(namespace);
    c.tableOperations().create(t1);
    c.tableOperations().create(t0);
    assertTrue(c.namespaceOperations().exists(namespace));
    assertTrue(c.tableOperations().exists(t1));
    assertTrue(c.tableOperations().exists(t0));

    // verify no property
    assertFalse(checkNamespaceHasProp(namespace, k, v));
    assertFalse(checkTableHasProp(t1, k, v));
    assertFalse(checkNamespaceHasProp(Namespace.DEFAULT.name(), k, v));
    assertFalse(checkTableHasProp(t0, k, v));

    // set property and verify
    c.namespaceOperations().setProperty(namespace, k, v);
    assertTrue(checkNamespaceHasProp(namespace, k, v));
    assertTrue(checkTableHasProp(t1, k, v));
    assertFalse(checkNamespaceHasProp(Namespace.DEFAULT.name(), k, v));
    assertFalse(checkTableHasProp(t0, k, v));

    // add a new table to namespace and verify
    assertFalse(c.tableOperations().exists(t2));
    c.tableOperations().create(t2);
    assertTrue(c.tableOperations().exists(t2));
    assertTrue(checkNamespaceHasProp(namespace, k, v));
    assertTrue(checkTableHasProp(t1, k, v));
    assertTrue(checkTableHasProp(t2, k, v));
    assertFalse(checkNamespaceHasProp(Namespace.DEFAULT.name(), k, v));
    assertFalse(checkTableHasProp(t0, k, v));

    // remove property and verify
    c.namespaceOperations().removeProperty(namespace, k);
    assertFalse(checkNamespaceHasProp(namespace, k, v));
    assertFalse(checkTableHasProp(t1, k, v));
    assertFalse(checkTableHasProp(t2, k, v));
    assertFalse(checkNamespaceHasProp(Namespace.DEFAULT.name(), k, v));
    assertFalse(checkTableHasProp(t0, k, v));

    // set property on default namespace and verify
    c.namespaceOperations().setProperty(Namespace.DEFAULT.name(), k, v);
    assertFalse(checkNamespaceHasProp(namespace, k, v));
    assertFalse(checkTableHasProp(t1, k, v));
    assertFalse(checkTableHasProp(t2, k, v));
    assertTrue(checkNamespaceHasProp(Namespace.DEFAULT.name(), k, v));
    assertTrue(checkTableHasProp(t0, k, v));

    // test that table properties override namespace properties
    String k2 = Property.TABLE_FILE_MAX.getKey();
    String v2 = "42";
    String table_v2 = "13";

    // set new property on some
    c.namespaceOperations().setProperty(namespace, k2, v2);
    c.tableOperations().setProperty(t2, k2, table_v2);
    assertTrue(checkNamespaceHasProp(namespace, k2, v2));
    assertTrue(checkTableHasProp(t1, k2, v2));
    assertTrue(checkTableHasProp(t2, k2, table_v2));

    // modify multiple at once
    String k3 = Property.TABLE_FILE_BLOCK_SIZE.getKey();
    String v3 = "52";
    String table_v3 = "73";
    c.namespaceOperations().modifyProperties(namespace, properties -> {
      properties.remove(k2);
      properties.put(k3, v3);
    });
    c.tableOperations().modifyProperties(t2, properties -> {
      properties.remove(k2);
      properties.put(k3, table_v3);
    });
    assertTrue(checkNamespaceHasProp(namespace, k3, v3));
    assertTrue(checkTableHasProp(t1, k3, v3));
    assertTrue(checkTableHasProp(t2, k3, table_v3));
    assertFalse(checkNamespaceHasProp(namespace, k2, v2));
    assertFalse(checkTableHasProp(t1, k2, v2));
    assertFalse(checkTableHasProp(t2, k2, table_v2));

    c.tableOperations().delete(t1);
    c.tableOperations().delete(t2);
    c.tableOperations().delete(t0);
    c.namespaceOperations().delete(namespace);
  }

  @Test
  public void verifyIteratorInheritance() throws Exception {
    String t1 = namespace + ".1";
    c.namespaceOperations().create(namespace);
    c.tableOperations().create(t1);
    String iterName = namespace + "_iter";

    try (BatchWriter bw = c.createBatchWriter(t1)) {
      Mutation m = new Mutation("r");
      m.put("a", "b", new Value("abcde"));
      bw.addMutation(m);
      bw.flush();
    }

    IteratorSetting setting = new IteratorSetting(250, iterName, SimpleFilter.class.getName());

    // verify can see inserted entry
    try (Scanner s = c.createScanner(t1)) {
      assertTrue(s.iterator().hasNext());
      assertFalse(c.namespaceOperations().listIterators(namespace).containsKey(iterName));
      assertFalse(c.tableOperations().listIterators(t1).containsKey(iterName));

      // verify entry is filtered out (also, verify conflict checking API)
      c.namespaceOperations().checkIteratorConflicts(namespace, setting,
          EnumSet.allOf(IteratorScope.class));
      c.namespaceOperations().attachIterator(namespace, setting);
      sleepUninterruptibly(2, TimeUnit.SECONDS);
      var e = assertThrows(AccumuloException.class, () -> c.namespaceOperations()
          .checkIteratorConflicts(namespace, setting, EnumSet.allOf(IteratorScope.class)));
      assertEquals(IllegalArgumentException.class, e.getCause().getClass());
      IteratorSetting setting2 = c.namespaceOperations().getIteratorSetting(namespace,
          setting.getName(), IteratorScope.scan);
      assertEquals(setting, setting2);
      assertTrue(c.namespaceOperations().listIterators(namespace).containsKey(iterName));
      assertTrue(c.tableOperations().listIterators(t1).containsKey(iterName));
    }

    try (Scanner s = c.createScanner(t1, Authorizations.EMPTY)) {
      assertFalse(s.iterator().hasNext());

      // verify can see inserted entry again
      c.namespaceOperations().removeIterator(namespace, setting.getName(),
          EnumSet.allOf(IteratorScope.class));
      sleepUninterruptibly(2, TimeUnit.SECONDS);
      assertFalse(c.namespaceOperations().listIterators(namespace).containsKey(iterName));
      assertFalse(c.tableOperations().listIterators(t1).containsKey(iterName));
    }

    try (Scanner s = c.createScanner(t1, Authorizations.EMPTY)) {
      assertTrue(s.iterator().hasNext());
    }
  }

  @Test
  public void cloneTable() throws Exception {
    String namespace2 = namespace + "_clone";
    String t1 = namespace + ".1";
    String t2 = namespace + ".2";
    String t3 = namespace2 + ".2";
    String k1 = Property.TABLE_FILE_MAX.getKey();
    String k2 = Property.TABLE_FILE_REPLICATION.getKey();
    String k1v1 = "55";
    String k1v2 = "66";
    String k2v1 = "5";
    String k2v2 = "6";

    c.namespaceOperations().create(namespace);
    c.tableOperations().create(t1);
    assertTrue(c.tableOperations().exists(t1));
    assertFalse(c.namespaceOperations().exists(namespace2));
    assertFalse(c.tableOperations().exists(t2));
    assertFalse(c.tableOperations().exists(t3));

    // try to clone before namespace exists
    var e = assertThrows(AccumuloException.class,
        () -> c.tableOperations().clone(t1, t3, false, null, null));
    assertEquals(NamespaceNotFoundException.class, e.getCause().getClass());

    // try to clone before when target tables exist
    c.namespaceOperations().create(namespace2);
    c.tableOperations().create(t2);
    c.tableOperations().create(t3);
    for (String t : Arrays.asList(t2, t3)) {
      assertThrows(TableExistsException.class,
          () -> c.tableOperations().clone(t1, t, false, null, null));
      c.tableOperations().delete(t);
    }

    assertTrue(c.tableOperations().exists(t1));
    assertTrue(c.namespaceOperations().exists(namespace2));
    assertFalse(c.tableOperations().exists(t2));
    assertFalse(c.tableOperations().exists(t3));

    // set property with different values in two namespaces and a separate property with different
    // values on the table and both namespaces
    assertFalse(checkNamespaceHasProp(namespace, k1, k1v1));
    assertFalse(checkNamespaceHasProp(namespace2, k1, k1v2));
    assertFalse(checkTableHasProp(t1, k1, k1v1));
    assertFalse(checkTableHasProp(t1, k1, k1v2));
    assertFalse(checkNamespaceHasProp(namespace, k2, k2v1));
    assertFalse(checkNamespaceHasProp(namespace2, k2, k2v1));
    assertFalse(checkTableHasProp(t1, k2, k2v1));
    assertFalse(checkTableHasProp(t1, k2, k2v2));
    c.namespaceOperations().setProperty(namespace, k1, k1v1);
    c.namespaceOperations().setProperty(namespace2, k1, k1v2);
    c.namespaceOperations().setProperty(namespace, k2, k2v1);
    c.namespaceOperations().setProperty(namespace2, k2, k2v1);
    c.tableOperations().setProperty(t1, k2, k2v2);
    assertTrue(checkNamespaceHasProp(namespace, k1, k1v1));
    assertTrue(checkNamespaceHasProp(namespace2, k1, k1v2));
    assertTrue(checkTableHasProp(t1, k1, k1v1));
    assertFalse(checkTableHasProp(t1, k1, k1v2));
    assertTrue(checkNamespaceHasProp(namespace, k2, k2v1));
    assertTrue(checkNamespaceHasProp(namespace2, k2, k2v1));
    assertFalse(checkTableHasProp(t1, k2, k2v1));
    assertTrue(checkTableHasProp(t1, k2, k2v2));

    // clone twice, once in same namespace, once in another
    for (String t : Arrays.asList(t2, t3)) {
      c.tableOperations().clone(t1, t, false, null, null);
    }

    assertTrue(c.namespaceOperations().exists(namespace2));
    assertTrue(c.tableOperations().exists(t1));
    assertTrue(c.tableOperations().exists(t2));
    assertTrue(c.tableOperations().exists(t3));

    // verify the properties got transferred
    assertTrue(checkTableHasProp(t1, k1, k1v1));
    assertTrue(checkTableHasProp(t2, k1, k1v1));
    assertTrue(checkTableHasProp(t3, k1, k1v2));
    assertTrue(checkTableHasProp(t1, k2, k2v2));
    assertTrue(checkTableHasProp(t2, k2, k2v2));
    assertTrue(checkTableHasProp(t3, k2, k2v2));
  }

  @Test
  public void renameNamespaceWithTable() throws Exception {
    String namespace2 = namespace + "_renamed";
    String t1 = namespace + ".t";
    String t2 = namespace2 + ".t";

    c.namespaceOperations().create(namespace);
    c.tableOperations().create(t1);
    assertTrue(c.namespaceOperations().exists(namespace));
    assertTrue(c.tableOperations().exists(t1));
    assertFalse(c.namespaceOperations().exists(namespace2));
    assertFalse(c.tableOperations().exists(t2));

    String namespaceId = c.namespaceOperations().namespaceIdMap().get(namespace);
    String tableId = c.tableOperations().tableIdMap().get(t1);

    c.namespaceOperations().rename(namespace, namespace2);
    assertFalse(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(t1));
    assertTrue(c.namespaceOperations().exists(namespace2));
    assertTrue(c.tableOperations().exists(t2));

    // verify id's didn't change
    String namespaceId2 = c.namespaceOperations().namespaceIdMap().get(namespace2);
    String tableId2 = c.tableOperations().tableIdMap().get(t2);

    assertEquals(namespaceId, namespaceId2);
    assertEquals(tableId, tableId2);
  }

  @Test
  public void verifyConstraintInheritance() throws Exception {
    String t1 = namespace + ".1";
    c.namespaceOperations().create(namespace);
    c.tableOperations().create(t1, new NewTableConfiguration().withoutDefaultIterators());
    String constraintClassName = NumericValueConstraint.class.getName();

    assertFalse(
        c.namespaceOperations().listConstraints(namespace).containsKey(constraintClassName));
    assertFalse(c.tableOperations().listConstraints(t1).containsKey(constraintClassName));

    c.namespaceOperations().addConstraint(namespace, constraintClassName);
    // loop until constraint is seen (or until test timeout)
    while (!c.namespaceOperations().listConstraints(namespace).containsKey(constraintClassName)
        || !c.tableOperations().listConstraints(t1).containsKey(constraintClassName)) {
      Thread.sleep(500);
    }

    Integer namespaceNum = null;
    Integer tableNum = null;
    // loop until constraint is seen in namespace and table (or until test times out)
    while (namespaceNum == null || tableNum == null) {
      namespaceNum = c.namespaceOperations().listConstraints(namespace).get(constraintClassName);
      tableNum = c.tableOperations().listConstraints(t1).get(constraintClassName);
      if (namespaceNum == null || tableNum == null) {
        Thread.sleep(500);
      }
    }
    assertEquals(namespaceNum, tableNum);

    Mutation m1 = new Mutation("r1");
    Mutation m2 = new Mutation("r2");
    Mutation m3 = new Mutation("r3");
    m1.put("a", "b", new Value("abcde"));
    m2.put("e", "f", new Value("123"));
    m3.put("c", "d", new Value("zyxwv"));

    // loop until constraint is activated and rejects mutations (or until test timeout)
    boolean mutationsRejected = false;
    while (!mutationsRejected) {
      BatchWriter bw = c.createBatchWriter(t1);
      bw.addMutations(Arrays.asList(m1, m2, m3));
      try {
        bw.close();
        Thread.sleep(500);
      } catch (MutationsRejectedException e) {
        mutationsRejected = true;
        assertEquals(1, e.getConstraintViolationSummaries().size());
        assertEquals(2, e.getConstraintViolationSummaries().get(0).getNumberOfViolatingMutations());
      }
    }

    assertNotNull(namespaceNum, "Namespace constraint ID should not be null");
    c.namespaceOperations().removeConstraint(namespace, namespaceNum);

    // loop until constraint is removed from config (or until test timeout)
    while (c.namespaceOperations().listConstraints(namespace).containsKey(constraintClassName)
        || c.tableOperations().listConstraints(t1).containsKey(constraintClassName)) {
      Thread.sleep(500);
    }

    // loop until constraint is removed and stops rejecting (or until test timeout)
    boolean mutationsAccepted = false;
    while (!mutationsAccepted) {
      BatchWriter bw = c.createBatchWriter(t1);
      try {
        bw.addMutations(Arrays.asList(m1, m2, m3));
        bw.close();
        mutationsAccepted = true;
      } catch (MutationsRejectedException e) {
        Thread.sleep(500);
      }
    }
  }

  @Test
  public void renameTable() throws Exception {
    String namespace2 = namespace + "_renamed";
    String t1 = namespace + ".1";
    String t2 = namespace2 + ".2";
    String t3 = namespace + ".3";
    String t4 = namespace + ".4";
    String t5 = "5";

    c.namespaceOperations().create(namespace);
    c.namespaceOperations().create(namespace2);

    assertTrue(c.namespaceOperations().exists(namespace));
    assertTrue(c.namespaceOperations().exists(namespace2));
    assertFalse(c.tableOperations().exists(t1));
    assertFalse(c.tableOperations().exists(t2));
    assertFalse(c.tableOperations().exists(t3));
    assertFalse(c.tableOperations().exists(t4));
    assertFalse(c.tableOperations().exists(t5));

    c.tableOperations().create(t1);

    var e1 = assertThrows(AccumuloException.class, () -> c.tableOperations().rename(t1, t2));
    // this is expected, because we don't allow renames across namespaces
    assertEquals(ThriftTableOperationException.class, e1.getCause().getClass());
    assertEquals(TableOperation.RENAME, ((ThriftTableOperationException) e1.getCause()).getOp());
    assertEquals(TableOperationExceptionType.INVALID_NAME,
        ((ThriftTableOperationException) e1.getCause()).getType());

    var e2 = assertThrows(AccumuloException.class, () -> c.tableOperations().rename(t1, t5));
    // this is expected, because we don't allow renames across namespaces
    assertEquals(ThriftTableOperationException.class, e2.getCause().getClass());
    assertEquals(TableOperation.RENAME, ((ThriftTableOperationException) e2.getCause()).getOp());
    assertEquals(TableOperationExceptionType.INVALID_NAME,
        ((ThriftTableOperationException) e2.getCause()).getType());

    assertTrue(c.tableOperations().exists(t1));
    assertFalse(c.tableOperations().exists(t2));
    assertFalse(c.tableOperations().exists(t3));
    assertFalse(c.tableOperations().exists(t4));
    assertFalse(c.tableOperations().exists(t5));

    // fully qualified rename
    c.tableOperations().rename(t1, t3);
    assertFalse(c.tableOperations().exists(t1));
    assertFalse(c.tableOperations().exists(t2));
    assertTrue(c.tableOperations().exists(t3));
    assertFalse(c.tableOperations().exists(t4));
    assertFalse(c.tableOperations().exists(t5));
  }

  private void loginAs(ClusterUser user) throws IOException {
    user.getToken();
  }

  /**
   * Tests new Namespace permissions as well as modifications to Table permissions because of
   * namespaces. Checks each permission to first make sure the user doesn't have permission to
   * perform the action, then root grants them the permission and we check to make sure they could
   * perform the action.
   */
  @Test
  public void testPermissions() throws Exception {
    ClusterUser user1 = getUser(0), user2 = getUser(1), root = getAdminUser();
    String u1 = user1.getPrincipal();
    String u2 = user2.getPrincipal();
    PasswordToken pass =
        (user1.getPassword() != null ? new PasswordToken(user1.getPassword()) : null);

    String n1 = namespace;
    String t1 = n1 + ".1";
    String t2 = n1 + ".2";
    String t3 = n1 + ".3";

    String n2 = namespace + "_2";

    loginAs(root);
    c.namespaceOperations().create(n1);
    c.tableOperations().create(t1);

    c.securityOperations().createLocalUser(u1, pass);

    loginAs(user1);
    try (AccumuloClient user1Con =
        Accumulo.newClient().from(c.properties()).as(u1, user1.getToken()).build()) {

      assertSecurityException(SecurityErrorCode.PERMISSION_DENIED,
          () -> user1Con.tableOperations().create(t2));

      loginAs(root);
      c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.CREATE_TABLE);
      loginAs(user1);
      user1Con.tableOperations().create(t2);
      loginAs(root);
      assertTrue(c.tableOperations().list().contains(t2));
      c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.CREATE_TABLE);

      loginAs(user1);
      assertSecurityException(SecurityErrorCode.PERMISSION_DENIED,
          () -> user1Con.tableOperations().delete(t1));

      loginAs(root);
      c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.DROP_TABLE);
      loginAs(user1);
      user1Con.tableOperations().delete(t1);
      loginAs(root);
      assertFalse(c.tableOperations().list().contains(t1));
      c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.DROP_TABLE);

      c.tableOperations().create(t3);
      try (BatchWriter bw = c.createBatchWriter(t3)) {
        Mutation m = new Mutation("row");
        m.put("cf", "cq", "value");
        bw.addMutation(m);
      }

      loginAs(user1);
      final Iterator<Entry<Key,Value>> i1 =
          user1Con.createScanner(t3, new Authorizations()).iterator();
      var e1 = assertThrows(RuntimeException.class, i1::next);
      assertEquals(AccumuloSecurityException.class, e1.getCause().getClass());
      assertSame(SecurityErrorCode.PERMISSION_DENIED,
          ((AccumuloSecurityException) e1.getCause()).getSecurityErrorCode());

      loginAs(user1);
      Mutation m = new Mutation(u1);
      m.put("cf", "cq", "turtles");
      BatchWriter bw = user1Con.createBatchWriter(t3);
      bw.addMutation(m);
      var e = assertThrows(MutationsRejectedException.class, bw::close);
      assertEquals(1, e.getSecurityErrorCodes().size());
      assertEquals(1, e.getSecurityErrorCodes().entrySet().iterator().next().getValue().size());
      assertSame(SecurityErrorCode.PERMISSION_DENIED,
          e.getSecurityErrorCodes().entrySet().iterator().next().getValue().iterator().next());

      loginAs(root);
      c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.READ);
      loginAs(user1);
      final Iterator<Entry<Key,Value>> i2 =
          user1Con.createScanner(t3, new Authorizations()).iterator();
      assertTrue(i2.hasNext());
      loginAs(root);
      c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.READ);
      c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.WRITE);

      loginAs(user1);
      try (BatchWriter bw2 = user1Con.createBatchWriter(t3)) {
        m = new Mutation(u1);
        m.put("cf", "cq", "turtles");
        bw2.addMutation(m);
      }
      loginAs(root);
      c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.WRITE);

      loginAs(user1);
      assertSecurityException(SecurityErrorCode.PERMISSION_DENIED,
          () -> user1Con.tableOperations().setProperty(t3, Property.TABLE_FILE_MAX.getKey(), "42"));
      assertSecurityException(SecurityErrorCode.PERMISSION_DENIED,
          () -> user1Con.tableOperations().modifyProperties(t3,
              properties -> properties.put(Property.TABLE_FILE_MAX.getKey(), "55")));

      loginAs(root);
      c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.ALTER_TABLE);
      c.securityOperations().grantTablePermission(u1, t3, TablePermission.ALTER_TABLE);
      loginAs(user1);
      user1Con.tableOperations().setProperty(t3, Property.TABLE_FILE_MAX.getKey(), "42");
      user1Con.tableOperations().modifyProperties(t3,
          properties -> properties.put(Property.TABLE_FILE_MAX.getKey(), "43"));
      user1Con.tableOperations().removeProperty(t3, Property.TABLE_FILE_MAX.getKey());
      loginAs(root);
      c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.ALTER_TABLE);

      loginAs(user1);
      assertSecurityException(SecurityErrorCode.PERMISSION_DENIED, () -> user1Con
          .namespaceOperations().setProperty(n1, Property.TABLE_FILE_MAX.getKey(), "55"));
      assertSecurityException(SecurityErrorCode.PERMISSION_DENIED,
          () -> user1Con.namespaceOperations().modifyProperties(n1,
              properties -> properties.put(Property.TABLE_FILE_MAX.getKey(), "55")));

      loginAs(root);
      c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.ALTER_NAMESPACE);
      loginAs(user1);
      user1Con.namespaceOperations().setProperty(n1, Property.TABLE_FILE_MAX.getKey(), "42");
      user1Con.namespaceOperations().modifyProperties(n1, properties -> {
        properties.put(Property.TABLE_FILE_MAX.getKey(), "43");
      });
      user1Con.namespaceOperations().removeProperty(n1, Property.TABLE_FILE_MAX.getKey());
      loginAs(root);
      c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.ALTER_NAMESPACE);

      loginAs(root);
      c.securityOperations().createLocalUser(u2,
          (root.getPassword() == null ? null : new PasswordToken(user2.getPassword())));
      loginAs(user1);
      assertSecurityException(SecurityErrorCode.PERMISSION_DENIED,
          () -> user1Con.securityOperations().grantNamespacePermission(u2, n1,
              NamespacePermission.ALTER_NAMESPACE));

      loginAs(root);
      c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.GRANT);
      loginAs(user1);
      user1Con.securityOperations().grantNamespacePermission(u2, n1,
          NamespacePermission.ALTER_NAMESPACE);
      user1Con.securityOperations().revokeNamespacePermission(u2, n1,
          NamespacePermission.ALTER_NAMESPACE);
      loginAs(root);
      c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.GRANT);

      loginAs(user1);
      assertSecurityException(SecurityErrorCode.PERMISSION_DENIED,
          () -> user1Con.namespaceOperations().create(n2));

      loginAs(root);
      c.securityOperations().grantSystemPermission(u1, SystemPermission.CREATE_NAMESPACE);
      loginAs(user1);
      user1Con.namespaceOperations().create(n2);
      loginAs(root);
      c.securityOperations().revokeSystemPermission(u1, SystemPermission.CREATE_NAMESPACE);

      c.securityOperations().revokeNamespacePermission(u1, n2, NamespacePermission.DROP_NAMESPACE);
      loginAs(user1);
      assertSecurityException(SecurityErrorCode.PERMISSION_DENIED,
          () -> user1Con.namespaceOperations().delete(n2));

      loginAs(root);
      c.securityOperations().grantSystemPermission(u1, SystemPermission.DROP_NAMESPACE);
      loginAs(user1);
      user1Con.namespaceOperations().delete(n2);
      loginAs(root);
      c.securityOperations().revokeSystemPermission(u1, SystemPermission.DROP_NAMESPACE);

      loginAs(user1);
      assertSecurityException(SecurityErrorCode.PERMISSION_DENIED, () -> user1Con
          .namespaceOperations().setProperty(n1, Property.TABLE_FILE_MAX.getKey(), "33"));

      loginAs(root);
      c.securityOperations().grantSystemPermission(u1, SystemPermission.ALTER_NAMESPACE);
      loginAs(user1);
      user1Con.namespaceOperations().setProperty(n1, Property.TABLE_FILE_MAX.getKey(), "33");
      user1Con.namespaceOperations().removeProperty(n1, Property.TABLE_FILE_MAX.getKey());
      loginAs(root);
      c.securityOperations().revokeSystemPermission(u1, SystemPermission.ALTER_NAMESPACE);
    }
  }

  @Test
  public void verifySystemPropertyInheritance() throws Exception {

    try (AccumuloClient client =
        getCluster().createAccumuloClient(getPrincipal(), new PasswordToken(getRootPassword()))) {
      client.securityOperations().grantNamespacePermission(getPrincipal(), "accumulo",
          NamespacePermission.ALTER_NAMESPACE);
      client.securityOperations().grantNamespacePermission(getPrincipal(), "",
          NamespacePermission.ALTER_NAMESPACE);
    }

    String t1 = "1";
    String t2 = namespace + "." + t1;
    c.tableOperations().create(t1);
    c.namespaceOperations().create(namespace);
    c.tableOperations().create(t2);

    // verify iterator inheritance
    _verifySystemPropertyInheritance(t1, t2, Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.sum",
        "20," + SimpleFilter.class.getName(), false);

    // verify constraint inheritance
    _verifySystemPropertyInheritance(t1, t2, Property.TABLE_CONSTRAINT_PREFIX.getKey() + "42",
        NumericValueConstraint.class.getName(), false);

    // verify other inheritance
    _verifySystemPropertyInheritance(t1, t2,
        Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "dummy", "dummy", true);
  }

  private void _verifySystemPropertyInheritance(String defaultNamespaceTable, String namespaceTable,
      String k, String v, boolean systemNamespaceShouldInherit) throws Exception {
    // nobody should have any of these properties yet
    assertFalse(c.instanceOperations().getSystemConfiguration().containsValue(v));
    assertFalse(checkNamespaceHasProp(Namespace.ACCUMULO.name(), k, v));
    assertFalse(checkTableHasProp(RootTable.NAME, k, v));
    assertFalse(checkTableHasProp(MetadataTable.NAME, k, v));
    assertFalse(checkNamespaceHasProp(Namespace.DEFAULT.name(), k, v));
    assertFalse(checkTableHasProp(defaultNamespaceTable, k, v));
    assertFalse(checkNamespaceHasProp(namespace, k, v));
    assertFalse(checkTableHasProp(namespaceTable, k, v));

    // set the filter, verify that accumulo namespace is the only one unaffected
    c.instanceOperations().setProperty(k, v);
    // doesn't take effect immediately, needs time to propagate to tserver's ZooKeeper cache
    sleepUninterruptibly(250, TimeUnit.MILLISECONDS);
    assertTrue(c.instanceOperations().getSystemConfiguration().containsValue(v));
    assertEquals(systemNamespaceShouldInherit,
        checkNamespaceHasProp(Namespace.ACCUMULO.name(), k, v));
    assertEquals(systemNamespaceShouldInherit, checkTableHasProp(RootTable.NAME, k, v));
    assertEquals(systemNamespaceShouldInherit, checkTableHasProp(MetadataTable.NAME, k, v));
    assertTrue(checkNamespaceHasProp(Namespace.DEFAULT.name(), k, v));
    assertTrue(checkTableHasProp(defaultNamespaceTable, k, v));
    assertTrue(checkNamespaceHasProp(namespace, k, v));
    assertTrue(checkTableHasProp(namespaceTable, k, v));

    // verify it is no longer inherited
    c.instanceOperations().removeProperty(k);
    // doesn't take effect immediately, needs time to propagate to tserver's ZooKeeper cache
    sleepUninterruptibly(250, TimeUnit.MILLISECONDS);
    assertFalse(c.instanceOperations().getSystemConfiguration().containsValue(v));
    assertFalse(checkNamespaceHasProp(Namespace.ACCUMULO.name(), k, v));
    assertFalse(checkTableHasProp(RootTable.NAME, k, v));
    assertFalse(checkTableHasProp(MetadataTable.NAME, k, v));
    assertFalse(checkNamespaceHasProp(Namespace.DEFAULT.name(), k, v));
    assertFalse(checkTableHasProp(defaultNamespaceTable, k, v));
    assertFalse(checkNamespaceHasProp(namespace, k, v));
    assertFalse(checkTableHasProp(namespaceTable, k, v));
  }

  @Test
  public void listNamespaces() throws Exception {
    SortedSet<String> namespaces = c.namespaceOperations().list();
    Map<String,String> map = c.namespaceOperations().namespaceIdMap();
    assertEquals(2, namespaces.size());
    assertEquals(2, map.size());
    assertTrue(namespaces.contains(Namespace.ACCUMULO.name()));
    assertTrue(namespaces.contains(Namespace.DEFAULT.name()));
    assertFalse(namespaces.contains(namespace));
    assertEquals(Namespace.ACCUMULO.id().canonical(), map.get(Namespace.ACCUMULO.name()));
    assertEquals(Namespace.DEFAULT.id().canonical(), map.get(Namespace.DEFAULT.name()));
    assertNull(map.get(namespace));

    c.namespaceOperations().create(namespace);
    namespaces = c.namespaceOperations().list();
    map = c.namespaceOperations().namespaceIdMap();
    assertEquals(3, namespaces.size());
    assertEquals(3, map.size());
    assertTrue(namespaces.contains(Namespace.ACCUMULO.name()));
    assertTrue(namespaces.contains(Namespace.DEFAULT.name()));
    assertTrue(namespaces.contains(namespace));
    assertEquals(Namespace.ACCUMULO.id().canonical(), map.get(Namespace.ACCUMULO.name()));
    assertEquals(Namespace.DEFAULT.id().canonical(), map.get(Namespace.DEFAULT.name()));
    assertNotNull(map.get(namespace));

    c.namespaceOperations().delete(namespace);
    namespaces = c.namespaceOperations().list();
    map = c.namespaceOperations().namespaceIdMap();
    assertEquals(2, namespaces.size());
    assertEquals(2, map.size());
    assertTrue(namespaces.contains(Namespace.ACCUMULO.name()));
    assertTrue(namespaces.contains(Namespace.DEFAULT.name()));
    assertFalse(namespaces.contains(namespace));
    assertEquals(Namespace.ACCUMULO.id().canonical(), map.get(Namespace.ACCUMULO.name()));
    assertEquals(Namespace.DEFAULT.id().canonical(), map.get(Namespace.DEFAULT.name()));
    assertNull(map.get(namespace));
  }

  @Test
  public void loadClass() throws Exception {
    assertTrue(c.namespaceOperations().testClassLoad(Namespace.DEFAULT.name(),
        VersioningIterator.class.getName(), SortedKeyValueIterator.class.getName()));
    assertFalse(c.namespaceOperations().testClassLoad(Namespace.DEFAULT.name(), "dummy",
        SortedKeyValueIterator.class.getName()));
    assertThrows(NamespaceNotFoundException.class,
        () -> c.namespaceOperations().testClassLoad(namespace, "dummy", "dummy"));
  }

  @Test
  public void testModifyingPermissions() throws Exception {
    String tableName = namespace + ".modify";
    c.namespaceOperations().create(namespace);
    c.tableOperations().create(tableName);
    assertTrue(
        c.securityOperations().hasTablePermission(c.whoami(), tableName, TablePermission.READ));
    c.securityOperations().revokeTablePermission(c.whoami(), tableName, TablePermission.READ);
    assertFalse(
        c.securityOperations().hasTablePermission(c.whoami(), tableName, TablePermission.READ));
    c.securityOperations().grantTablePermission(c.whoami(), tableName, TablePermission.READ);
    assertTrue(
        c.securityOperations().hasTablePermission(c.whoami(), tableName, TablePermission.READ));
    c.tableOperations().delete(tableName);

    assertSecurityException(SecurityErrorCode.TABLE_DOESNT_EXIST, () -> c.securityOperations()
        .hasTablePermission(c.whoami(), tableName, TablePermission.READ));
    assertSecurityException(SecurityErrorCode.TABLE_DOESNT_EXIST, () -> c.securityOperations()
        .grantTablePermission(c.whoami(), tableName, TablePermission.READ));
    assertSecurityException(SecurityErrorCode.TABLE_DOESNT_EXIST, () -> c.securityOperations()
        .revokeTablePermission(c.whoami(), tableName, TablePermission.READ));

    assertTrue(c.securityOperations().hasNamespacePermission(c.whoami(), namespace,
        NamespacePermission.READ));
    c.securityOperations().revokeNamespacePermission(c.whoami(), namespace,
        NamespacePermission.READ);
    assertFalse(c.securityOperations().hasNamespacePermission(c.whoami(), namespace,
        NamespacePermission.READ));
    c.securityOperations().grantNamespacePermission(c.whoami(), namespace,
        NamespacePermission.READ);
    assertTrue(c.securityOperations().hasNamespacePermission(c.whoami(), namespace,
        NamespacePermission.READ));

    c.namespaceOperations().delete(namespace);

    assertSecurityException(SecurityErrorCode.TABLE_DOESNT_EXIST, () -> c.securityOperations()
        .hasTablePermission(c.whoami(), tableName, TablePermission.READ));
    assertSecurityException(SecurityErrorCode.TABLE_DOESNT_EXIST, () -> c.securityOperations()
        .grantTablePermission(c.whoami(), tableName, TablePermission.READ));
    assertSecurityException(SecurityErrorCode.TABLE_DOESNT_EXIST, () -> c.securityOperations()
        .revokeTablePermission(c.whoami(), tableName, TablePermission.READ));

    assertSecurityException(SecurityErrorCode.NAMESPACE_DOESNT_EXIST, () -> c.securityOperations()
        .hasNamespacePermission(c.whoami(), namespace, NamespacePermission.READ));
    assertSecurityException(SecurityErrorCode.NAMESPACE_DOESNT_EXIST, () -> c.securityOperations()
        .grantNamespacePermission(c.whoami(), namespace, NamespacePermission.READ));
    assertSecurityException(SecurityErrorCode.NAMESPACE_DOESNT_EXIST, () -> c.securityOperations()
        .revokeNamespacePermission(c.whoami(), namespace, NamespacePermission.READ));
  }

  @Test
  public void validatePermissions() throws Exception {
    // Create namespace.
    c.namespaceOperations().create(namespace);

    assertTrue(c.securityOperations().hasNamespacePermission(c.whoami(), namespace,
        NamespacePermission.READ));
    c.securityOperations().revokeNamespacePermission(c.whoami(), namespace,
        NamespacePermission.READ);
    assertFalse(c.securityOperations().hasNamespacePermission(c.whoami(), namespace,
        NamespacePermission.READ));
    c.securityOperations().grantNamespacePermission(c.whoami(), namespace,
        NamespacePermission.READ);
    assertTrue(c.securityOperations().hasNamespacePermission(c.whoami(), namespace,
        NamespacePermission.READ));

    c.namespaceOperations().delete(namespace);

    assertSecurityException(SecurityErrorCode.NAMESPACE_DOESNT_EXIST, () -> c.securityOperations()
        .hasNamespacePermission(c.whoami(), namespace, NamespacePermission.READ));
    assertSecurityException(SecurityErrorCode.NAMESPACE_DOESNT_EXIST, () -> c.securityOperations()
        .grantNamespacePermission(c.whoami(), namespace, NamespacePermission.READ));
    assertSecurityException(SecurityErrorCode.NAMESPACE_DOESNT_EXIST, () -> c.securityOperations()
        .revokeNamespacePermission(c.whoami(), namespace, NamespacePermission.READ));
  }

  @Test
  public void verifyTableOperationsExceptions() throws Exception {
    String tableName = namespace + ".1";
    IteratorSetting setting = new IteratorSetting(200, VersioningIterator.class);
    Text a = new Text("a");
    Text z = new Text("z");
    TableOperations ops = c.tableOperations();

    // this one doesn't throw an exception; just check that it works
    assertFalse(ops.exists(tableName));

    // table operations that should throw an AccumuloException caused by NamespaceNotFoundException
    assertAccumuloExceptionNoNamespace(() -> ops.create(tableName));
    ops.create("a");
    assertAccumuloExceptionNoNamespace(
        () -> ops.clone("a", tableName, true, Collections.emptyMap(), Collections.emptySet()));
    ops.offline("a", true);
    ops.exportTable("a", System.getProperty("user.dir") + "/target");
    assertAccumuloExceptionNoNamespace(() -> ops.importTable(tableName,
        Set.of(System.getProperty("user.dir") + "/target"), ImportConfiguration.empty()));

    // table operations that should throw an AccumuloException caused by a TableNotFoundException
    // caused by a NamespaceNotFoundException
    // these are here because we didn't declare TableNotFoundException in the API :(
    assertAccumuloExceptionNoTableNoNamespace(() -> ops.removeConstraint(tableName, 0));
    assertAccumuloExceptionNoTableNoNamespace(() -> ops.removeProperty(tableName, "a"));
    assertAccumuloExceptionNoTableNoNamespace(() -> ops.setProperty(tableName, "a", "b"));

    // table operations that should throw a TableNotFoundException caused by
    // NamespaceNotFoundException
    assertNoTableNoNamespace(
        () -> ops.addConstraint(tableName, NumericValueConstraint.class.getName()));
    assertNoTableNoNamespace(() -> ops.addSplits(tableName, new TreeSet<>()));
    assertNoTableNoNamespace(() -> ops.attachIterator(tableName, setting));
    assertNoTableNoNamespace(() -> ops.cancelCompaction(tableName));
    assertNoTableNoNamespace(
        () -> ops.checkIteratorConflicts(tableName, setting, EnumSet.allOf(IteratorScope.class)));
    assertNoTableNoNamespace(() -> ops.clearLocatorCache(tableName));
    assertNoTableNoNamespace(
        () -> ops.clone(tableName, "2", true, Collections.emptyMap(), Collections.emptySet()));
    assertNoTableNoNamespace(() -> ops.compact(tableName, a, z, true, true));
    assertNoTableNoNamespace(() -> ops.delete(tableName));
    assertNoTableNoNamespace(() -> ops.deleteRows(tableName, a, z));
    assertNoTableNoNamespace(() -> ops.splitRangeByTablets(tableName, new Range(), 10));
    assertNoTableNoNamespace(() -> ops.exportTable(tableName, namespace + "_dir"));
    assertNoTableNoNamespace(() -> ops.flush(tableName, a, z, true));
    assertNoTableNoNamespace(() -> ops.getDiskUsage(Set.of(tableName)));
    assertNoTableNoNamespace(() -> ops.getIteratorSetting(tableName, "a", IteratorScope.scan));
    assertNoTableNoNamespace(() -> ops.getLocalityGroups(tableName));
    assertNoTableNoNamespace(
        () -> ops.getMaxRow(tableName, Authorizations.EMPTY, a, true, z, true));
    assertNoTableNoNamespace(() -> ops.getConfiguration(tableName));
    assertNoTableNoNamespace(() -> ops.importDirectory("").to(tableName).load());
    assertNoTableNoNamespace(() -> ops.testClassLoad(tableName, VersioningIterator.class.getName(),
        SortedKeyValueIterator.class.getName()));
    assertNoTableNoNamespace(() -> ops.listConstraints(tableName));
    assertNoTableNoNamespace(() -> ops.listIterators(tableName));
    assertNoTableNoNamespace(() -> ops.listSplits(tableName));
    assertNoTableNoNamespace(() -> ops.merge(tableName, a, z));
    assertNoTableNoNamespace(() -> ops.offline(tableName, true));
    assertNoTableNoNamespace(() -> ops.online(tableName, true));
    assertNoTableNoNamespace(
        () -> ops.removeIterator(tableName, "a", EnumSet.of(IteratorScope.scan)));
    assertNoTableNoNamespace(() -> ops.rename(tableName, tableName + "2"));
    assertNoTableNoNamespace(() -> ops.setLocalityGroups(tableName, Collections.emptyMap()));
  }

  @Test
  public void verifyNamespaceOperationsExceptions() throws Exception {
    IteratorSetting setting = new IteratorSetting(200, VersioningIterator.class);
    NamespaceOperations ops = c.namespaceOperations();

    // this one doesn't throw an exception; just check that it works
    assertFalse(ops.exists(namespace));

    // namespace operations that should throw a NamespaceNotFoundException
    assertNoNamespace(() -> ops.addConstraint(namespace, NumericValueConstraint.class.getName()));
    assertNoNamespace(() -> ops.attachIterator(namespace, setting));
    assertNoNamespace(
        () -> ops.checkIteratorConflicts(namespace, setting, EnumSet.of(IteratorScope.scan)));
    assertNoNamespace(() -> ops.delete(namespace));
    assertNoNamespace(() -> ops.getIteratorSetting(namespace, "thing", IteratorScope.scan));
    assertNoNamespace(() -> ops.getConfiguration(namespace));
    assertNoNamespace(() -> ops.listConstraints(namespace));
    assertNoNamespace(() -> ops.listIterators(namespace));
    assertNoNamespace(() -> ops.removeConstraint(namespace, 1));
    assertNoNamespace(
        () -> ops.removeIterator(namespace, "thing", EnumSet.allOf(IteratorScope.class)));
    assertNoNamespace(() -> ops.removeProperty(namespace, "a"));
    assertNoNamespace(() -> ops.rename(namespace, namespace + "2"));
    assertNoNamespace(() -> ops.setProperty(namespace, "k", "v"));
    assertNoNamespace(() -> ops.testClassLoad(namespace, VersioningIterator.class.getName(),
        SortedKeyValueIterator.class.getName()));

    // namespace operations that should throw a NamespaceExistsException
    assertNamespaceExists(() -> ops.create(Namespace.DEFAULT.name()));
    assertNamespaceExists(() -> ops.create(Namespace.ACCUMULO.name()));

    ops.create(namespace + "0");
    ops.create(namespace + "1");

    assertNamespaceExists(() -> ops.create(namespace + "0"));
    assertNamespaceExists(() -> ops.rename(namespace + "0", namespace + "1"));
    assertNamespaceExists(() -> ops.rename(namespace + "0", Namespace.DEFAULT.name()));
    assertNamespaceExists(() -> ops.rename(namespace + "0", Namespace.ACCUMULO.name()));
  }

  private boolean checkTableHasProp(String t, String propKey, String propVal) throws Exception {
    return checkHasProperty(t, propKey, propVal, true);
  }

  private boolean checkNamespaceHasProp(String n, String propKey, String propVal) throws Exception {
    return checkHasProperty(n, propKey, propVal, false);
  }

  private boolean checkHasProperty(String name, String propKey, String propVal, boolean nameIsTable)
      throws Exception {
    Map<String,String> props = nameIsTable ? c.tableOperations().getConfiguration(name)
        : c.namespaceOperations().getConfiguration(name);
    for (Entry<String,String> e : props.entrySet()) {
      if (propKey.equals(e.getKey())) {
        return propVal.equals(e.getValue());
      }
    }
    return false;
  }

  public static class SimpleFilter extends Filter {
    @Override
    public boolean accept(Key k, Value v) {
      return !k.getColumnFamily().toString().equals("a");
    }
  }

  private void assertNamespaceExists(Executable runnable) {
    assertThrows(NamespaceExistsException.class, runnable);
  }

  private void assertNoNamespace(Executable runnable) {
    assertThrows(NamespaceNotFoundException.class, runnable);
  }

  private void assertNoTableNoNamespace(Executable runnable) {
    var e = assertThrows(TableNotFoundException.class, runnable);
    assertEquals(NamespaceNotFoundException.class, e.getCause().getClass());
  }

  private void assertAccumuloExceptionNoNamespace(Executable runnable) {
    var e = assertThrows(AccumuloException.class, runnable);
    assertEquals(NamespaceNotFoundException.class, e.getCause().getClass());
  }

  private void assertAccumuloExceptionNoTableNoNamespace(Executable runnable) {
    var e = assertThrows(AccumuloException.class, runnable);
    assertEquals(TableNotFoundException.class, e.getCause().getClass());
    assertEquals(NamespaceNotFoundException.class, e.getCause().getCause().getClass());
  }

  private void assertSecurityException(SecurityErrorCode code, Executable runnable) {
    var e = assertThrows(AccumuloSecurityException.class, runnable);
    assertSame(code, e.getSecurityErrorCode());
  }

}
