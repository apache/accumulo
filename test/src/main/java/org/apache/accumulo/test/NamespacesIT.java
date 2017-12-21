/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
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
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.accumulo.test.constraints.NumericValueConstraint;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

// Testing default namespace configuration with inheritance requires altering the system state and restoring it back to normal
// Punt on this for now and just let it use a minicluster.
@Category(MiniClusterOnlyTests.class)
public class NamespacesIT extends AccumuloClusterHarness {

  private Connector c;
  private String namespace;

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Before
  public void setupConnectorAndNamespace() throws Exception {
    Assume.assumeTrue(ClusterType.MINI == getClusterType());

    // prepare a unique namespace and get a new root connector for each test
    c = getConnector();
    namespace = "ns_" + getUniqueNames(1)[0];
  }

  @After
  public void swingMjölnir() throws Exception {
    if (null == c) {
      return;
    }
    // clean up any added tables, namespaces, and users, after each test
    for (String t : c.tableOperations().list())
      if (!Tables.qualify(t).getFirst().equals(Namespace.ACCUMULO))
        c.tableOperations().delete(t);
    assertEquals(3, c.tableOperations().list().size());
    for (String n : c.namespaceOperations().list())
      if (!n.equals(Namespace.ACCUMULO) && !n.equals(Namespace.DEFAULT))
        c.namespaceOperations().delete(n);
    assertEquals(2, c.namespaceOperations().list().size());
    for (String u : c.securityOperations().listLocalUsers())
      if (!getAdminPrincipal().equals(u))
        c.securityOperations().dropLocalUser(u);
    assertEquals(1, c.securityOperations().listLocalUsers().size());
  }

  @Test
  public void checkReservedNamespaces() throws Exception {
    assertEquals(c.namespaceOperations().defaultNamespace(), Namespace.DEFAULT);
    assertEquals(c.namespaceOperations().systemNamespace(), Namespace.ACCUMULO);
  }

  @Test
  public void checkBuiltInNamespaces() throws Exception {
    assertTrue(c.namespaceOperations().exists(Namespace.DEFAULT));
    assertTrue(c.namespaceOperations().exists(Namespace.ACCUMULO));
  }

  @Test
  public void createTableInDefaultNamespace() throws Exception {
    String tableName = "1";
    c.tableOperations().create(tableName);
    assertTrue(c.tableOperations().exists(tableName));
  }

  @Test(expected = AccumuloException.class)
  public void createTableInAccumuloNamespace() throws Exception {
    String tableName = Namespace.ACCUMULO + ".1";
    assertFalse(c.tableOperations().exists(tableName));
    c.tableOperations().create(tableName); // should fail
  }

  @Test(expected = AccumuloSecurityException.class)
  public void deleteDefaultNamespace() throws Exception {
    c.namespaceOperations().delete(Namespace.DEFAULT); // should fail
  }

  @Test(expected = AccumuloSecurityException.class)
  public void deleteAccumuloNamespace() throws Exception {
    c.namespaceOperations().delete(Namespace.ACCUMULO); // should fail
  }

  @Test
  public void createTableInMissingNamespace() throws Exception {
    String t = namespace + ".1";
    assertFalse(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(t));
    try {
      c.tableOperations().create(t);
      fail();
    } catch (AccumuloException e) {
      assertEquals(NamespaceNotFoundException.class.getName(), e.getCause().getClass().getName());
      assertFalse(c.namespaceOperations().exists(namespace));
      assertFalse(c.tableOperations().exists(t));
    }
  }

  @Test
  public void createAndDeleteNamespace() throws Exception {
    String t1 = namespace + ".1";
    String t2 = namespace + ".2";
    assertFalse(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(t1));
    assertFalse(c.tableOperations().exists(t2));
    try {
      c.namespaceOperations().delete(namespace);
    } catch (NamespaceNotFoundException e) {}
    try {
      c.tableOperations().delete(t1);
    } catch (TableNotFoundException e) {
      assertEquals(NamespaceNotFoundException.class.getName(), e.getCause().getClass().getName());
    }
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

  @Test(expected = NamespaceNotEmptyException.class)
  public void deleteNonEmptyNamespace() throws Exception {
    String tableName1 = namespace + ".1";
    assertFalse(c.namespaceOperations().exists(namespace));
    assertFalse(c.tableOperations().exists(tableName1));
    c.namespaceOperations().create(namespace);
    c.tableOperations().create(tableName1);
    assertTrue(c.namespaceOperations().exists(namespace));
    assertTrue(c.tableOperations().exists(tableName1));
    c.namespaceOperations().delete(namespace); // should fail
  }

  @Test
  public void verifyPropertyInheritance() throws Exception {
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
    assertFalse(checkNamespaceHasProp(Namespace.DEFAULT, k, v));
    assertFalse(checkTableHasProp(t0, k, v));

    // set property and verify
    c.namespaceOperations().setProperty(namespace, k, v);
    assertTrue(checkNamespaceHasProp(namespace, k, v));
    assertTrue(checkTableHasProp(t1, k, v));
    assertFalse(checkNamespaceHasProp(Namespace.DEFAULT, k, v));
    assertFalse(checkTableHasProp(t0, k, v));

    // add a new table to namespace and verify
    assertFalse(c.tableOperations().exists(t2));
    c.tableOperations().create(t2);
    assertTrue(c.tableOperations().exists(t2));
    assertTrue(checkNamespaceHasProp(namespace, k, v));
    assertTrue(checkTableHasProp(t1, k, v));
    assertTrue(checkTableHasProp(t2, k, v));
    assertFalse(checkNamespaceHasProp(Namespace.DEFAULT, k, v));
    assertFalse(checkTableHasProp(t0, k, v));

    // remove property and verify
    c.namespaceOperations().removeProperty(namespace, k);
    assertFalse(checkNamespaceHasProp(namespace, k, v));
    assertFalse(checkTableHasProp(t1, k, v));
    assertFalse(checkTableHasProp(t2, k, v));
    assertFalse(checkNamespaceHasProp(Namespace.DEFAULT, k, v));
    assertFalse(checkTableHasProp(t0, k, v));

    // set property on default namespace and verify
    c.namespaceOperations().setProperty(Namespace.DEFAULT, k, v);
    assertFalse(checkNamespaceHasProp(namespace, k, v));
    assertFalse(checkTableHasProp(t1, k, v));
    assertFalse(checkTableHasProp(t2, k, v));
    assertTrue(checkNamespaceHasProp(Namespace.DEFAULT, k, v));
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

    BatchWriter bw = c.createBatchWriter(t1, new BatchWriterConfig());
    Mutation m = new Mutation("r");
    m.put("a", "b", new Value("abcde".getBytes()));
    bw.addMutation(m);
    bw.flush();
    bw.close();

    IteratorSetting setting = new IteratorSetting(250, iterName, SimpleFilter.class.getName());

    // verify can see inserted entry
    try (Scanner s = c.createScanner(t1, Authorizations.EMPTY)) {
      assertTrue(s.iterator().hasNext());
      assertFalse(c.namespaceOperations().listIterators(namespace).containsKey(iterName));
      assertFalse(c.tableOperations().listIterators(t1).containsKey(iterName));

      // verify entry is filtered out (also, verify conflict checking API)
      c.namespaceOperations().checkIteratorConflicts(namespace, setting, EnumSet.allOf(IteratorScope.class));
      c.namespaceOperations().attachIterator(namespace, setting);
      sleepUninterruptibly(2, TimeUnit.SECONDS);
      try {
        c.namespaceOperations().checkIteratorConflicts(namespace, setting, EnumSet.allOf(IteratorScope.class));
        fail();
      } catch (AccumuloException e) {
        assertEquals(IllegalArgumentException.class.getName(), e.getCause().getClass().getName());
      }
      IteratorSetting setting2 = c.namespaceOperations().getIteratorSetting(namespace, setting.getName(), IteratorScope.scan);
      assertEquals(setting, setting2);
      assertTrue(c.namespaceOperations().listIterators(namespace).containsKey(iterName));
      assertTrue(c.tableOperations().listIterators(t1).containsKey(iterName));
    }

    try (Scanner s = c.createScanner(t1, Authorizations.EMPTY)) {
      assertFalse(s.iterator().hasNext());

      // verify can see inserted entry again
      c.namespaceOperations().removeIterator(namespace, setting.getName(), EnumSet.allOf(IteratorScope.class));
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

    try {
      // try to clone before namespace exists
      c.tableOperations().clone(t1, t3, false, null, null); // should fail
      fail();
    } catch (AccumuloException e) {
      assertEquals(NamespaceNotFoundException.class.getName(), e.getCause().getClass().getName());
    }

    // try to clone before when target tables exist
    c.namespaceOperations().create(namespace2);
    c.tableOperations().create(t2);
    c.tableOperations().create(t3);
    for (String t : Arrays.asList(t2, t3)) {
      try {
        c.tableOperations().clone(t1, t, false, null, null); // should fail
        fail();
      } catch (TableExistsException e) {
        c.tableOperations().delete(t);
      }
    }

    assertTrue(c.tableOperations().exists(t1));
    assertTrue(c.namespaceOperations().exists(namespace2));
    assertFalse(c.tableOperations().exists(t2));
    assertFalse(c.tableOperations().exists(t3));

    // set property with different values in two namespaces and a separate property with different values on the table and both namespaces
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
    for (String t : Arrays.asList(t2, t3))
      c.tableOperations().clone(t1, t, false, null, null);

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

    assertFalse(c.namespaceOperations().listConstraints(namespace).containsKey(constraintClassName));
    assertFalse(c.tableOperations().listConstraints(t1).containsKey(constraintClassName));

    c.namespaceOperations().addConstraint(namespace, constraintClassName);
    boolean passed = false;
    for (int i = 0; i < 5; i++) {
      if (!c.namespaceOperations().listConstraints(namespace).containsKey(constraintClassName)) {
        Thread.sleep(500);
        continue;
      }
      if (!c.tableOperations().listConstraints(t1).containsKey(constraintClassName)) {
        Thread.sleep(500);
        continue;
      }
      passed = true;
      break;
    }
    assertTrue("Failed to observe newly-added constraint", passed);

    passed = false;
    Integer namespaceNum = null;
    for (int i = 0; i < 5; i++) {
      namespaceNum = c.namespaceOperations().listConstraints(namespace).get(constraintClassName);
      if (null == namespaceNum) {
        Thread.sleep(500);
        continue;
      }
      Integer tableNum = c.tableOperations().listConstraints(t1).get(constraintClassName);
      if (null == tableNum) {
        Thread.sleep(500);
        continue;
      }
      assertEquals(namespaceNum, tableNum);
      passed = true;
    }
    assertTrue("Failed to observe constraint in both table and namespace", passed);

    Mutation m1 = new Mutation("r1");
    Mutation m2 = new Mutation("r2");
    Mutation m3 = new Mutation("r3");
    m1.put("a", "b", new Value("abcde".getBytes(UTF_8)));
    m2.put("e", "f", new Value("123".getBytes(UTF_8)));
    m3.put("c", "d", new Value("zyxwv".getBytes(UTF_8)));

    passed = false;
    for (int i = 0; i < 5; i++) {
      BatchWriter bw = c.createBatchWriter(t1, new BatchWriterConfig());
      bw.addMutations(Arrays.asList(m1, m2, m3));
      try {
        bw.close();
        Thread.sleep(500);
      } catch (MutationsRejectedException e) {
        passed = true;
        assertEquals(1, e.getConstraintViolationSummaries().size());
        assertEquals(2, e.getConstraintViolationSummaries().get(0).getNumberOfViolatingMutations());
        break;
      }
    }

    assertTrue("Failed to see mutations rejected after constraint was added", passed);

    assertNotNull("Namespace constraint ID should not be null", namespaceNum);
    c.namespaceOperations().removeConstraint(namespace, namespaceNum);
    passed = false;
    for (int i = 0; i < 5; i++) {
      if (c.namespaceOperations().listConstraints(namespace).containsKey(constraintClassName)) {
        Thread.sleep(500);
        continue;
      }
      if (c.tableOperations().listConstraints(t1).containsKey(constraintClassName)) {
        Thread.sleep(500);
        continue;
      }
      passed = true;
    }
    assertTrue("Failed to verify that constraint was removed from namespace and table", passed);

    passed = false;
    for (int i = 0; i < 5; i++) {
      BatchWriter bw = c.createBatchWriter(t1, new BatchWriterConfig());
      try {
        bw.addMutations(Arrays.asList(m1, m2, m3));
        bw.close();
      } catch (MutationsRejectedException e) {
        Thread.sleep(500);
        continue;
      }
      passed = true;
    }
    assertTrue("Failed to add mutations that should be allowed", passed);
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

    try {
      c.tableOperations().rename(t1, t2);
      fail();
    } catch (AccumuloException e) {
      // this is expected, because we don't allow renames across namespaces
      assertEquals(ThriftTableOperationException.class.getName(), e.getCause().getClass().getName());
      assertEquals(TableOperation.RENAME, ((ThriftTableOperationException) e.getCause()).getOp());
      assertEquals(TableOperationExceptionType.INVALID_NAME, ((ThriftTableOperationException) e.getCause()).getType());
    }

    try {
      c.tableOperations().rename(t1, t5);
      fail();
    } catch (AccumuloException e) {
      // this is expected, because we don't allow renames across namespaces
      assertEquals(ThriftTableOperationException.class.getName(), e.getCause().getClass().getName());
      assertEquals(TableOperation.RENAME, ((ThriftTableOperationException) e.getCause()).getOp());
      assertEquals(TableOperationExceptionType.INVALID_NAME, ((ThriftTableOperationException) e.getCause()).getType());
    }

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
   * Tests new Namespace permissions as well as modifications to Table permissions because of namespaces. Checks each permission to first make sure the user
   * doesn't have permission to perform the action, then root grants them the permission and we check to make sure they could perform the action.
   */
  @Test
  public void testPermissions() throws Exception {
    ClusterUser user1 = getUser(0), user2 = getUser(1), root = getAdminUser();
    String u1 = user1.getPrincipal();
    String u2 = user2.getPrincipal();
    PasswordToken pass = (null != user1.getPassword() ? new PasswordToken(user1.getPassword()) : null);

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
    Connector user1Con = c.getInstance().getConnector(u1, user1.getToken());

    try {
      user1Con.tableOperations().create(t2);
      fail();
    } catch (AccumuloSecurityException e) {
      expectPermissionDenied(e);
    }

    loginAs(root);
    c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.CREATE_TABLE);
    loginAs(user1);
    user1Con.tableOperations().create(t2);
    loginAs(root);
    assertTrue(c.tableOperations().list().contains(t2));
    c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.CREATE_TABLE);

    loginAs(user1);
    try {
      user1Con.tableOperations().delete(t1);
      fail();
    } catch (AccumuloSecurityException e) {
      expectPermissionDenied(e);
    }

    loginAs(root);
    c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.DROP_TABLE);
    loginAs(user1);
    user1Con.tableOperations().delete(t1);
    loginAs(root);
    assertTrue(!c.tableOperations().list().contains(t1));
    c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.DROP_TABLE);

    c.tableOperations().create(t3);
    BatchWriter bw = c.createBatchWriter(t3, null);
    Mutation m = new Mutation("row");
    m.put("cf", "cq", "value");
    bw.addMutation(m);
    bw.close();

    loginAs(user1);
    Iterator<Entry<Key,Value>> i = user1Con.createScanner(t3, new Authorizations()).iterator();
    try {
      i.next();
      fail();
    } catch (RuntimeException e) {
      assertEquals(AccumuloSecurityException.class.getName(), e.getCause().getClass().getName());
      expectPermissionDenied((AccumuloSecurityException) e.getCause());
    }

    loginAs(user1);
    m = new Mutation(u1);
    m.put("cf", "cq", "turtles");
    bw = user1Con.createBatchWriter(t3, null);
    try {
      bw.addMutation(m);
      bw.close();
      fail();
    } catch (MutationsRejectedException e) {
      assertEquals(1, e.getSecurityErrorCodes().size());
      assertEquals(1, e.getSecurityErrorCodes().entrySet().iterator().next().getValue().size());
      switch (e.getSecurityErrorCodes().entrySet().iterator().next().getValue().iterator().next()) {
        case PERMISSION_DENIED:
          break;
        default:
          fail();
      }
    }

    loginAs(root);
    c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.READ);
    loginAs(user1);
    i = user1Con.createScanner(t3, new Authorizations()).iterator();
    assertTrue(i.hasNext());
    loginAs(root);
    c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.READ);
    c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.WRITE);

    loginAs(user1);
    m = new Mutation(u1);
    m.put("cf", "cq", "turtles");
    bw = user1Con.createBatchWriter(t3, null);
    bw.addMutation(m);
    bw.close();
    loginAs(root);
    c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.WRITE);

    loginAs(user1);
    try {
      user1Con.tableOperations().setProperty(t3, Property.TABLE_FILE_MAX.getKey(), "42");
      fail();
    } catch (AccumuloSecurityException e) {
      expectPermissionDenied(e);
    }

    loginAs(root);
    c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.ALTER_TABLE);
    loginAs(user1);
    user1Con.tableOperations().setProperty(t3, Property.TABLE_FILE_MAX.getKey(), "42");
    user1Con.tableOperations().removeProperty(t3, Property.TABLE_FILE_MAX.getKey());
    loginAs(root);
    c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.ALTER_TABLE);

    loginAs(user1);
    try {
      user1Con.namespaceOperations().setProperty(n1, Property.TABLE_FILE_MAX.getKey(), "55");
      fail();
    } catch (AccumuloSecurityException e) {
      expectPermissionDenied(e);
    }

    loginAs(root);
    c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.ALTER_NAMESPACE);
    loginAs(user1);
    user1Con.namespaceOperations().setProperty(n1, Property.TABLE_FILE_MAX.getKey(), "42");
    user1Con.namespaceOperations().removeProperty(n1, Property.TABLE_FILE_MAX.getKey());
    loginAs(root);
    c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.ALTER_NAMESPACE);

    loginAs(root);
    c.securityOperations().createLocalUser(u2, (root.getPassword() == null ? null : new PasswordToken(user2.getPassword())));
    loginAs(user1);
    try {
      user1Con.securityOperations().grantNamespacePermission(u2, n1, NamespacePermission.ALTER_NAMESPACE);
      fail();
    } catch (AccumuloSecurityException e) {
      expectPermissionDenied(e);
    }

    loginAs(root);
    c.securityOperations().grantNamespacePermission(u1, n1, NamespacePermission.GRANT);
    loginAs(user1);
    user1Con.securityOperations().grantNamespacePermission(u2, n1, NamespacePermission.ALTER_NAMESPACE);
    user1Con.securityOperations().revokeNamespacePermission(u2, n1, NamespacePermission.ALTER_NAMESPACE);
    loginAs(root);
    c.securityOperations().revokeNamespacePermission(u1, n1, NamespacePermission.GRANT);

    loginAs(user1);
    try {
      user1Con.namespaceOperations().create(n2);
      fail();
    } catch (AccumuloSecurityException e) {
      expectPermissionDenied(e);
    }

    loginAs(root);
    c.securityOperations().grantSystemPermission(u1, SystemPermission.CREATE_NAMESPACE);
    loginAs(user1);
    user1Con.namespaceOperations().create(n2);
    loginAs(root);
    c.securityOperations().revokeSystemPermission(u1, SystemPermission.CREATE_NAMESPACE);

    c.securityOperations().revokeNamespacePermission(u1, n2, NamespacePermission.DROP_NAMESPACE);
    loginAs(user1);
    try {
      user1Con.namespaceOperations().delete(n2);
      fail();
    } catch (AccumuloSecurityException e) {
      expectPermissionDenied(e);
    }

    loginAs(root);
    c.securityOperations().grantSystemPermission(u1, SystemPermission.DROP_NAMESPACE);
    loginAs(user1);
    user1Con.namespaceOperations().delete(n2);
    loginAs(root);
    c.securityOperations().revokeSystemPermission(u1, SystemPermission.DROP_NAMESPACE);

    loginAs(user1);
    try {
      user1Con.namespaceOperations().setProperty(n1, Property.TABLE_FILE_MAX.getKey(), "33");
      fail();
    } catch (AccumuloSecurityException e) {
      expectPermissionDenied(e);
    }

    loginAs(root);
    c.securityOperations().grantSystemPermission(u1, SystemPermission.ALTER_NAMESPACE);
    loginAs(user1);
    user1Con.namespaceOperations().setProperty(n1, Property.TABLE_FILE_MAX.getKey(), "33");
    user1Con.namespaceOperations().removeProperty(n1, Property.TABLE_FILE_MAX.getKey());
    loginAs(root);
    c.securityOperations().revokeSystemPermission(u1, SystemPermission.ALTER_NAMESPACE);
  }

  @Test
  public void verifySystemPropertyInheritance() throws Exception {
    String t1 = "1";
    String t2 = namespace + "." + t1;
    c.tableOperations().create(t1);
    c.namespaceOperations().create(namespace);
    c.tableOperations().create(t2);

    // verify iterator inheritance
    _verifySystemPropertyInheritance(t1, t2, Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.sum", "20," + SimpleFilter.class.getName(), false);

    // verify constraint inheritance
    _verifySystemPropertyInheritance(t1, t2, Property.TABLE_CONSTRAINT_PREFIX.getKey() + "42", NumericValueConstraint.class.getName(), false);

    // verify other inheritance
    _verifySystemPropertyInheritance(t1, t2, Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "dummy", "dummy", true);
  }

  private void _verifySystemPropertyInheritance(String defaultNamespaceTable, String namespaceTable, String k, String v, boolean systemNamespaceShouldInherit)
      throws Exception {
    // nobody should have any of these properties yet
    assertFalse(c.instanceOperations().getSystemConfiguration().containsValue(v));
    assertFalse(checkNamespaceHasProp(Namespace.ACCUMULO, k, v));
    assertFalse(checkTableHasProp(RootTable.NAME, k, v));
    assertFalse(checkTableHasProp(MetadataTable.NAME, k, v));
    assertFalse(checkNamespaceHasProp(Namespace.DEFAULT, k, v));
    assertFalse(checkTableHasProp(defaultNamespaceTable, k, v));
    assertFalse(checkNamespaceHasProp(namespace, k, v));
    assertFalse(checkTableHasProp(namespaceTable, k, v));

    // set the filter, verify that accumulo namespace is the only one unaffected
    c.instanceOperations().setProperty(k, v);
    // doesn't take effect immediately, needs time to propagate to tserver's ZooKeeper cache
    sleepUninterruptibly(250, TimeUnit.MILLISECONDS);
    assertTrue(c.instanceOperations().getSystemConfiguration().containsValue(v));
    assertEquals(systemNamespaceShouldInherit, checkNamespaceHasProp(Namespace.ACCUMULO, k, v));
    assertEquals(systemNamespaceShouldInherit, checkTableHasProp(RootTable.NAME, k, v));
    assertEquals(systemNamespaceShouldInherit, checkTableHasProp(MetadataTable.NAME, k, v));
    assertTrue(checkNamespaceHasProp(Namespace.DEFAULT, k, v));
    assertTrue(checkTableHasProp(defaultNamespaceTable, k, v));
    assertTrue(checkNamespaceHasProp(namespace, k, v));
    assertTrue(checkTableHasProp(namespaceTable, k, v));

    // verify it is no longer inherited
    c.instanceOperations().removeProperty(k);
    // doesn't take effect immediately, needs time to propagate to tserver's ZooKeeper cache
    sleepUninterruptibly(250, TimeUnit.MILLISECONDS);
    assertFalse(c.instanceOperations().getSystemConfiguration().containsValue(v));
    assertFalse(checkNamespaceHasProp(Namespace.ACCUMULO, k, v));
    assertFalse(checkTableHasProp(RootTable.NAME, k, v));
    assertFalse(checkTableHasProp(MetadataTable.NAME, k, v));
    assertFalse(checkNamespaceHasProp(Namespace.DEFAULT, k, v));
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
    assertTrue(namespaces.contains(Namespace.ACCUMULO));
    assertTrue(namespaces.contains(Namespace.DEFAULT));
    assertFalse(namespaces.contains(namespace));
    assertEquals(Namespace.ID.ACCUMULO.canonicalID(), map.get(Namespace.ACCUMULO));
    assertEquals(Namespace.ID.DEFAULT.canonicalID(), map.get(Namespace.DEFAULT));
    assertNull(map.get(namespace));

    c.namespaceOperations().create(namespace);
    namespaces = c.namespaceOperations().list();
    map = c.namespaceOperations().namespaceIdMap();
    assertEquals(3, namespaces.size());
    assertEquals(3, map.size());
    assertTrue(namespaces.contains(Namespace.ACCUMULO));
    assertTrue(namespaces.contains(Namespace.DEFAULT));
    assertTrue(namespaces.contains(namespace));
    assertEquals(Namespace.ID.ACCUMULO.canonicalID(), map.get(Namespace.ACCUMULO));
    assertEquals(Namespace.ID.DEFAULT.canonicalID(), map.get(Namespace.DEFAULT));
    assertNotNull(map.get(namespace));

    c.namespaceOperations().delete(namespace);
    namespaces = c.namespaceOperations().list();
    map = c.namespaceOperations().namespaceIdMap();
    assertEquals(2, namespaces.size());
    assertEquals(2, map.size());
    assertTrue(namespaces.contains(Namespace.ACCUMULO));
    assertTrue(namespaces.contains(Namespace.DEFAULT));
    assertFalse(namespaces.contains(namespace));
    assertEquals(Namespace.ID.ACCUMULO.canonicalID(), map.get(Namespace.ACCUMULO));
    assertEquals(Namespace.ID.DEFAULT.canonicalID(), map.get(Namespace.DEFAULT));
    assertNull(map.get(namespace));
  }

  @Test
  public void loadClass() throws Exception {
    assertTrue(c.namespaceOperations().testClassLoad(Namespace.DEFAULT, VersioningIterator.class.getName(), SortedKeyValueIterator.class.getName()));
    assertFalse(c.namespaceOperations().testClassLoad(Namespace.DEFAULT, "dummy", SortedKeyValueIterator.class.getName()));
    try {
      c.namespaceOperations().testClassLoad(namespace, "dummy", "dummy");
      fail();
    } catch (NamespaceNotFoundException e) {
      // expected, ignore
    }
  }

  @Test
  public void testModifyingPermissions() throws Exception {
    String tableName = namespace + ".modify";
    c.namespaceOperations().create(namespace);
    c.tableOperations().create(tableName);
    assertTrue(c.securityOperations().hasTablePermission(c.whoami(), tableName, TablePermission.READ));
    c.securityOperations().revokeTablePermission(c.whoami(), tableName, TablePermission.READ);
    assertFalse(c.securityOperations().hasTablePermission(c.whoami(), tableName, TablePermission.READ));
    c.securityOperations().grantTablePermission(c.whoami(), tableName, TablePermission.READ);
    assertTrue(c.securityOperations().hasTablePermission(c.whoami(), tableName, TablePermission.READ));
    c.tableOperations().delete(tableName);

    try {
      c.securityOperations().hasTablePermission(c.whoami(), tableName, TablePermission.READ);
      fail();
    } catch (Exception e) {
      if (!(e instanceof AccumuloSecurityException) || !((AccumuloSecurityException) e).getSecurityErrorCode().equals(SecurityErrorCode.TABLE_DOESNT_EXIST))
        throw new Exception("Has permission resulted in " + e.getClass().getName(), e);
    }

    try {
      c.securityOperations().grantTablePermission(c.whoami(), tableName, TablePermission.READ);
      fail();
    } catch (Exception e) {
      if (!(e instanceof AccumuloSecurityException) || !((AccumuloSecurityException) e).getSecurityErrorCode().equals(SecurityErrorCode.TABLE_DOESNT_EXIST))
        throw new Exception("Has permission resulted in " + e.getClass().getName(), e);
    }

    try {
      c.securityOperations().revokeTablePermission(c.whoami(), tableName, TablePermission.READ);
      fail();
    } catch (Exception e) {
      if (!(e instanceof AccumuloSecurityException) || !((AccumuloSecurityException) e).getSecurityErrorCode().equals(SecurityErrorCode.TABLE_DOESNT_EXIST))
        throw new Exception("Has permission resulted in " + e.getClass().getName(), e);
    }

    assertTrue(c.securityOperations().hasNamespacePermission(c.whoami(), namespace, NamespacePermission.READ));
    c.securityOperations().revokeNamespacePermission(c.whoami(), namespace, NamespacePermission.READ);
    assertFalse(c.securityOperations().hasNamespacePermission(c.whoami(), namespace, NamespacePermission.READ));
    c.securityOperations().grantNamespacePermission(c.whoami(), namespace, NamespacePermission.READ);
    assertTrue(c.securityOperations().hasNamespacePermission(c.whoami(), namespace, NamespacePermission.READ));

    c.namespaceOperations().delete(namespace);

    try {
      c.securityOperations().hasTablePermission(c.whoami(), tableName, TablePermission.READ);
      fail();
    } catch (Exception e) {
      if (!(e instanceof AccumuloSecurityException) || !((AccumuloSecurityException) e).getSecurityErrorCode().equals(SecurityErrorCode.TABLE_DOESNT_EXIST))
        throw new Exception("Has permission resulted in " + e.getClass().getName(), e);
    }

    try {
      c.securityOperations().grantTablePermission(c.whoami(), tableName, TablePermission.READ);
      fail();
    } catch (Exception e) {
      if (!(e instanceof AccumuloSecurityException) || !((AccumuloSecurityException) e).getSecurityErrorCode().equals(SecurityErrorCode.TABLE_DOESNT_EXIST))
        throw new Exception("Has permission resulted in " + e.getClass().getName(), e);
    }

    try {
      c.securityOperations().revokeTablePermission(c.whoami(), tableName, TablePermission.READ);
      fail();
    } catch (Exception e) {
      if (!(e instanceof AccumuloSecurityException) || !((AccumuloSecurityException) e).getSecurityErrorCode().equals(SecurityErrorCode.TABLE_DOESNT_EXIST))
        throw new Exception("Has permission resulted in " + e.getClass().getName(), e);
    }

    try {
      c.securityOperations().hasNamespacePermission(c.whoami(), namespace, NamespacePermission.READ);
      fail();
    } catch (Exception e) {
      if (!(e instanceof AccumuloSecurityException) || !((AccumuloSecurityException) e).getSecurityErrorCode().equals(SecurityErrorCode.NAMESPACE_DOESNT_EXIST))
        throw new Exception("Has permission resulted in " + e.getClass().getName(), e);
    }

    try {
      c.securityOperations().grantNamespacePermission(c.whoami(), namespace, NamespacePermission.READ);
      fail();
    } catch (Exception e) {
      if (!(e instanceof AccumuloSecurityException) || !((AccumuloSecurityException) e).getSecurityErrorCode().equals(SecurityErrorCode.NAMESPACE_DOESNT_EXIST))
        throw new Exception("Has permission resulted in " + e.getClass().getName(), e);
    }

    try {
      c.securityOperations().revokeNamespacePermission(c.whoami(), namespace, NamespacePermission.READ);
      fail();
    } catch (Exception e) {
      if (!(e instanceof AccumuloSecurityException) || !((AccumuloSecurityException) e).getSecurityErrorCode().equals(SecurityErrorCode.NAMESPACE_DOESNT_EXIST))
        throw new Exception("Has permission resulted in " + e.getClass().getName(), e);
    }

  }

  @Test
  public void verifyTableOperationsExceptions() throws Exception {
    String tableName = namespace + ".1";
    IteratorSetting setting = new IteratorSetting(200, VersioningIterator.class);
    Text a = new Text("a");
    Text z = new Text("z");
    TableOperations ops = c.tableOperations();

    // this one doesn't throw an exception, so don't fail; just check that it works
    assertFalse(ops.exists(tableName));

    // table operations that should throw an AccumuloException caused by NamespaceNotFoundException
    int numRun = 0;
    ACCUMULOEXCEPTIONS_NAMESPACENOTFOUND: for (int i = 0;; ++i)
      try {
        switch (i) {
          case 0:
            ops.create(tableName);
            fail();
            break;
          case 1:
            ops.create("a");
            ops.clone("a", tableName, true, Collections.<String,String> emptyMap(), Collections.<String> emptySet());
            fail();
            break;
          case 2:
            ops.importTable(tableName, System.getProperty("user.dir") + "/target");
            fail();
            break;
          default:
            // break out of infinite loop
            assertEquals(3, i); // check test integrity
            assertEquals(3, numRun); // check test integrity
            break ACCUMULOEXCEPTIONS_NAMESPACENOTFOUND;
        }
      } catch (Exception e) {
        numRun++;
        if (!(e instanceof AccumuloException) || !(e.getCause() instanceof NamespaceNotFoundException))
          throw new Exception("Case " + i + " resulted in " + e.getClass().getName(), e);
      }

    // table operations that should throw an AccumuloException caused by a TableNotFoundException caused by a NamespaceNotFoundException
    // these are here because we didn't declare TableNotFoundException in the API :(
    numRun = 0;
    ACCUMULOEXCEPTIONS_TABLENOTFOUND: for (int i = 0;; ++i)
      try {
        switch (i) {
          case 0:
            ops.removeConstraint(tableName, 0);
            fail();
            break;
          case 1:
            ops.removeProperty(tableName, "a");
            fail();
            break;
          case 2:
            ops.setProperty(tableName, "a", "b");
            fail();
            break;
          default:
            // break out of infinite loop
            assertEquals(3, i); // check test integrity
            assertEquals(3, numRun); // check test integrity
            break ACCUMULOEXCEPTIONS_TABLENOTFOUND;
        }
      } catch (Exception e) {
        numRun++;
        if (!(e instanceof AccumuloException) || !(e.getCause() instanceof TableNotFoundException)
            || !(e.getCause().getCause() instanceof NamespaceNotFoundException))
          throw new Exception("Case " + i + " resulted in " + e.getClass().getName(), e);
      }

    // table operations that should throw a TableNotFoundException caused by NamespaceNotFoundException
    numRun = 0;
    TABLENOTFOUNDEXCEPTIONS: for (int i = 0;; ++i)
      try {
        switch (i) {
          case 0:
            ops.addConstraint(tableName, NumericValueConstraint.class.getName());
            fail();
            break;
          case 1:
            ops.addSplits(tableName, new TreeSet<Text>());
            fail();
            break;
          case 2:
            ops.attachIterator(tableName, setting);
            fail();
            break;
          case 3:
            ops.cancelCompaction(tableName);
            fail();
            break;
          case 4:
            ops.checkIteratorConflicts(tableName, setting, EnumSet.allOf(IteratorScope.class));
            fail();
            break;
          case 5:
            ops.clearLocatorCache(tableName);
            fail();
            break;
          case 6:
            ops.clone(tableName, "2", true, Collections.<String,String> emptyMap(), Collections.<String> emptySet());
            fail();
            break;
          case 7:
            ops.compact(tableName, a, z, true, true);
            fail();
            break;
          case 8:
            ops.delete(tableName);
            fail();
            break;
          case 9:
            ops.deleteRows(tableName, a, z);
            fail();
            break;
          case 10:
            ops.splitRangeByTablets(tableName, new Range(), 10);
            fail();
            break;
          case 11:
            ops.exportTable(tableName, namespace + "_dir");
            fail();
            break;
          case 12:
            ops.flush(tableName, a, z, true);
            fail();
            break;
          case 13:
            ops.getDiskUsage(Collections.singleton(tableName));
            fail();
            break;
          case 14:
            ops.getIteratorSetting(tableName, "a", IteratorScope.scan);
            fail();
            break;
          case 15:
            ops.getLocalityGroups(tableName);
            fail();
            break;
          case 16:
            ops.getMaxRow(tableName, Authorizations.EMPTY, a, true, z, true);
            fail();
            break;
          case 17:
            ops.getProperties(tableName);
            fail();
            break;
          case 18:
            ops.importDirectory(tableName, "", "", false);
            fail();
            break;
          case 19:
            ops.testClassLoad(tableName, VersioningIterator.class.getName(), SortedKeyValueIterator.class.getName());
            fail();
            break;
          case 20:
            ops.listConstraints(tableName);
            fail();
            break;
          case 21:
            ops.listIterators(tableName);
            fail();
            break;
          case 22:
            ops.listSplits(tableName);
            fail();
            break;
          case 23:
            ops.merge(tableName, a, z);
            fail();
            break;
          case 24:
            ops.offline(tableName, true);
            fail();
            break;
          case 25:
            ops.online(tableName, true);
            fail();
            break;
          case 26:
            ops.removeIterator(tableName, "a", EnumSet.of(IteratorScope.scan));
            fail();
            break;
          case 27:
            ops.rename(tableName, tableName + "2");
            fail();
            break;
          case 28:
            ops.setLocalityGroups(tableName, Collections.<String,Set<Text>> emptyMap());
            fail();
            break;
          default:
            // break out of infinite loop
            assertEquals(29, i); // check test integrity
            assertEquals(29, numRun); // check test integrity
            break TABLENOTFOUNDEXCEPTIONS;
        }
      } catch (Exception e) {
        numRun++;
        if (!(e instanceof TableNotFoundException) || !(e.getCause() instanceof NamespaceNotFoundException))
          throw new Exception("Case " + i + " resulted in " + e.getClass().getName(), e);
      }
  }

  @Test
  public void verifyNamespaceOperationsExceptions() throws Exception {
    IteratorSetting setting = new IteratorSetting(200, VersioningIterator.class);
    NamespaceOperations ops = c.namespaceOperations();

    // this one doesn't throw an exception, so don't fail; just check that it works
    assertFalse(ops.exists(namespace));

    // namespace operations that should throw a NamespaceNotFoundException
    int numRun = 0;
    NAMESPACENOTFOUND: for (int i = 0;; ++i)
      try {
        switch (i) {
          case 0:
            ops.addConstraint(namespace, NumericValueConstraint.class.getName());
            fail();
            break;
          case 1:
            ops.attachIterator(namespace, setting);
            fail();
            break;
          case 2:
            ops.checkIteratorConflicts(namespace, setting, EnumSet.of(IteratorScope.scan));
            fail();
            break;
          case 3:
            ops.delete(namespace);
            fail();
            break;
          case 4:
            ops.getIteratorSetting(namespace, "thing", IteratorScope.scan);
            fail();
            break;
          case 5:
            ops.getProperties(namespace);
            fail();
            break;
          case 6:
            ops.listConstraints(namespace);
            fail();
            break;
          case 7:
            ops.listIterators(namespace);
            fail();
            break;
          case 8:
            ops.removeConstraint(namespace, 1);
            fail();
            break;
          case 9:
            ops.removeIterator(namespace, "thing", EnumSet.allOf(IteratorScope.class));
            fail();
            break;
          case 10:
            ops.removeProperty(namespace, "a");
            fail();
            break;
          case 11:
            ops.rename(namespace, namespace + "2");
            fail();
            break;
          case 12:
            ops.setProperty(namespace, "k", "v");
            fail();
            break;
          case 13:
            ops.testClassLoad(namespace, VersioningIterator.class.getName(), SortedKeyValueIterator.class.getName());
            fail();
            break;
          default:
            // break out of infinite loop
            assertEquals(14, i); // check test integrity
            assertEquals(14, numRun); // check test integrity
            break NAMESPACENOTFOUND;
        }
      } catch (Exception e) {
        numRun++;
        if (!(e instanceof NamespaceNotFoundException))
          throw new Exception("Case " + i + " resulted in " + e.getClass().getName(), e);
      }

    // namespace operations that should throw a NamespaceExistsException
    numRun = 0;
    NAMESPACEEXISTS: for (int i = 0;; ++i)
      try {
        switch (i) {
          case 0:
            ops.create(namespace + "0");
            ops.create(namespace + "0"); // should fail here
            fail();
            break;
          case 1:
            ops.create(namespace + i + "_1");
            ops.create(namespace + i + "_2");
            ops.rename(namespace + i + "_1", namespace + i + "_2"); // should fail here
            fail();
            break;
          case 2:
            ops.create(Namespace.DEFAULT);
            fail();
            break;
          case 3:
            ops.create(Namespace.ACCUMULO);
            fail();
            break;
          case 4:
            ops.create(namespace + i + "_1");
            ops.rename(namespace + i + "_1", Namespace.DEFAULT); // should fail here
            fail();
            break;
          case 5:
            ops.create(namespace + i + "_1");
            ops.rename(namespace + i + "_1", Namespace.ACCUMULO); // should fail here
            fail();
            break;
          default:
            // break out of infinite loop
            assertEquals(6, i); // check test integrity
            assertEquals(6, numRun); // check test integrity
            break NAMESPACEEXISTS;
        }
      } catch (Exception e) {
        numRun++;
        if (!(e instanceof NamespaceExistsException))
          throw new Exception("Case " + i + " resulted in " + e.getClass().getName(), e);
      }
  }

  private boolean checkTableHasProp(String t, String propKey, String propVal) {
    return checkHasProperty(t, propKey, propVal, true);
  }

  private boolean checkNamespaceHasProp(String n, String propKey, String propVal) {
    return checkHasProperty(n, propKey, propVal, false);
  }

  private boolean checkHasProperty(String name, String propKey, String propVal, boolean nameIsTable) {
    try {
      Iterable<Entry<String,String>> iterable = nameIsTable ? c.tableOperations().getProperties(name) : c.namespaceOperations().getProperties(name);
      for (Entry<String,String> e : iterable)
        if (propKey.equals(e.getKey()))
          return propVal.equals(e.getValue());
      return false;
    } catch (Exception e) {
      fail();
      return false;
    }
  }

  public static class SimpleFilter extends Filter {
    @Override
    public boolean accept(Key k, Value v) {
      if (k.getColumnFamily().toString().equals("a"))
        return false;
      return true;
    }
  }

  private void expectPermissionDenied(AccumuloSecurityException sec) {
    assertEquals(sec.getSecurityErrorCode().getClass(), SecurityErrorCode.class);
    switch (sec.getSecurityErrorCode()) {
      case PERMISSION_DENIED:
        break;
      default:
        fail();
    }
  }

}
