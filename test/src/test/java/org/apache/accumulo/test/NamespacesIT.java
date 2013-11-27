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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.examples.simple.constraints.NumericValueConstraint;
import org.apache.accumulo.test.functional.SimpleMacIT;
import org.junit.Test;

public class NamespacesIT extends SimpleMacIT {

  Random random = new Random();

  /**
   * This test creates a table without specifying a namespace. In this case, it puts the table into the default namespace.
   */
  @Test
  public void testDefaultNamespace() throws Exception {
    String tableName = "test";
    Connector c = getConnector();

    assertTrue(c.namespaceOperations().exists(Constants.DEFAULT_NAMESPACE));
    c.tableOperations().create(tableName);
    assertTrue(c.tableOperations().exists(tableName));
  }

  /**
   * This test creates a new namespace "testing" and a table "testing.table1" which puts "table1" into the "testing" namespace. Then we create "testing.table2"
   * which creates "table2" and puts it into "testing" as well. Then we make sure that you can't delete a namespace with tables in it, and then we delete the
   * tables and delete the namespace.
   */
  @Test
  public void testCreateAndDeleteNamespace() throws Exception {
    String namespace = "testing";
    String tableName1 = namespace + ".table1";
    String tableName2 = namespace + ".table2";

    Connector c = getConnector();

    c.namespaceOperations().create(namespace);
    assertTrue(c.namespaceOperations().exists(namespace));

    c.tableOperations().create(tableName1);
    assertTrue(c.tableOperations().exists(tableName1));

    c.tableOperations().create(tableName2);
    assertTrue(c.tableOperations().exists(tableName2));

    // deleting
    try {
      // can't delete a namespace with tables in it
      c.namespaceOperations().delete(namespace);
      fail();
    } catch (NamespaceNotEmptyException e) {
      // ignore, supposed to happen
    }
    assertTrue(c.namespaceOperations().exists(namespace));
    assertTrue(c.tableOperations().exists(tableName1));
    assertTrue(c.tableOperations().exists(tableName2));

    c.tableOperations().delete(tableName2);
    assertTrue(!c.tableOperations().exists(tableName2));
    assertTrue(c.namespaceOperations().exists(namespace));

    c.tableOperations().delete(tableName1);
    assertTrue(!c.tableOperations().exists(tableName1));
    c.namespaceOperations().delete(namespace);
    assertTrue(!c.namespaceOperations().exists(namespace));
  }

  /**
   * This test creates a namespace, modifies it's properties, and checks to make sure that those properties are applied to its tables. To do something on a
   * namespace-wide level, use NamespaceOperations.
   * 
   * Checks to make sure namespace-level properties are overridden by table-level properties.
   * 
   * Checks to see if the default namespace's properties work as well.
   */

  @Test
  public void testNamespaceProperties() throws Exception {
    String namespace = "propchange";
    String tableName1 = namespace + ".table1";
    String tableName2 = namespace + ".table2";

    String propKey = Property.TABLE_SCAN_MAXMEM.getKey();
    String propVal = "42K";

    Connector c = getConnector();

    c.namespaceOperations().create(namespace);
    c.tableOperations().create(tableName1);
    c.namespaceOperations().setProperty(namespace, propKey, propVal);

    // check the namespace has the property
    assertTrue(checkNamespaceHasProp(c, namespace, propKey, propVal));

    // check that the table gets it from the namespace
    assertTrue(checkTableHasProp(c, tableName1, propKey, propVal));

    // test a second table to be sure the first wasn't magical
    // (also, changed the order, the namespace has the property already)
    c.tableOperations().create(tableName2);
    assertTrue(checkTableHasProp(c, tableName2, propKey, propVal));

    // test that table properties override namespace properties
    String propKey2 = Property.TABLE_FILE_MAX.getKey();
    String propVal2 = "42";
    String tablePropVal = "13";

    c.tableOperations().setProperty(tableName2, propKey2, tablePropVal);
    c.namespaceOperations().setProperty("propchange", propKey2, propVal2);

    assertTrue(checkTableHasProp(c, tableName2, propKey2, tablePropVal));

    // now check that you can change the default namespace's properties
    propVal = "13K";
    String tableName = "some_table";
    c.tableOperations().create(tableName);
    c.namespaceOperations().setProperty(Constants.DEFAULT_NAMESPACE, propKey, propVal);

    assertTrue(checkTableHasProp(c, tableName, propKey, propVal));

    // test the properties server-side by configuring an iterator.
    // should not show anything with column-family = 'a'
    String tableName3 = namespace + ".table3";
    c.tableOperations().create(tableName3);

    IteratorSetting setting = new IteratorSetting(250, "thing", SimpleFilter.class.getName());
    c.namespaceOperations().attachIterator(namespace, setting);

    BatchWriter bw = c.createBatchWriter(tableName3, new BatchWriterConfig());
    Mutation m = new Mutation("r");
    m.put("a", "b", new Value("abcde".getBytes()));
    bw.addMutation(m);
    bw.flush();
    bw.close();

    Scanner s = c.createScanner(tableName3, Authorizations.EMPTY);
    assertTrue(!s.iterator().hasNext());
  }

  /**
   * This test renames and clones two separate table into different namespaces. different namespace.
   * 
   */
  @Test
  public void testCloneTableToNewNamespace() throws Exception {
    Connector c = getConnector();

    String[] uniqueNames = getTableNames(2);
    String namespace1 = uniqueNames[0];
    String namespace2 = uniqueNames[1];
    String tableName1 = namespace1 + ".table1";
    String tableName2 = namespace2 + ".table2";

    c.namespaceOperations().create(namespace1);
    c.tableOperations().create(tableName1);
    assertTrue(c.tableOperations().exists(tableName1));

    c.namespaceOperations().create(namespace2);
    c.tableOperations().clone(tableName1, tableName2, false, null, null);

    assertTrue(c.tableOperations().exists(tableName1));
    assertTrue(c.tableOperations().exists(tableName2));
  }

  /**
   * This test renames a namespace and ensures that its tables are still correct
   */
  @Test
  public void testNamespaceRename() throws Exception {
    String namespace1 = "n1";
    String namespace2 = "n2";
    String table = "t";

    Connector c = getConnector();
    Instance instance = c.getInstance();

    c.namespaceOperations().create(namespace1);
    c.tableOperations().create(namespace1 + "." + table);

    c.namespaceOperations().rename(namespace1, namespace2);

    assertTrue(!c.namespaceOperations().exists(namespace1));
    assertTrue(c.namespaceOperations().exists(namespace2));
    assertTrue(c.tableOperations().exists(namespace2 + "." + table));
    String tid = Tables.getTableId(instance, namespace2 + "." + table);
    String tnid = Tables.getNamespace(instance, tid);
    String tnamespace = Namespaces.getNamespaceName(instance, tnid);
    assertTrue(namespace2.equals(tnamespace));
  }

  /**
   * This test clones a table to a different namespace and ensures it's properties are correct
   */
  @Test
  public void testCloneTableProperties() throws Exception {
    String[] uniqueNames = getTableNames(2);
    String n1 = uniqueNames[0];
    String n2 = uniqueNames[1];
    String t1 = n1 + ".table";
    String t2 = n2 + ".table";

    String propKey = Property.TABLE_FILE_MAX.getKey();
    String propVal1 = "55";
    String propVal2 = "66";

    Connector c = getConnector();

    c.namespaceOperations().create(n1);
    c.tableOperations().create(t1);

    c.tableOperations().removeProperty(t1, Property.TABLE_FILE_MAX.getKey());
    c.namespaceOperations().setProperty(n1, propKey, propVal1);

    assertTrue(checkTableHasProp(c, t1, propKey, propVal1));

    c.namespaceOperations().create(n2);
    c.namespaceOperations().setProperty(n2, propKey, propVal2);
    c.tableOperations().clone(t1, t2, true, null, null);
    c.tableOperations().removeProperty(t2, propKey);

    assertTrue(checkTableHasProp(c, t2, propKey, propVal2));

    c.tableOperations().delete(t1);
    c.tableOperations().delete(t2);
    c.namespaceOperations().delete(n1);
    c.namespaceOperations().delete(n2);
  }

  /**
   * This tests adding iterators to a namespace, listing them, and removing them as well as adding and removing constraints
   */
  @Test
  public void testNamespaceIteratorsAndConstraints() throws Exception {
    Connector c = getConnector();

    String namespace = "iterator";
    String tableName = namespace + ".table";
    String iter = "thing";

    c.namespaceOperations().create(namespace);
    c.tableOperations().create(tableName);

    IteratorSetting setting = new IteratorSetting(250, iter, SimpleFilter.class.getName());
    HashSet<IteratorScope> scope = new HashSet<IteratorScope>();
    scope.add(IteratorScope.scan);
    c.namespaceOperations().attachIterator(namespace, setting, EnumSet.copyOf(scope));

    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("r");
    m.put("a", "b", new Value("abcde".getBytes(Constants.UTF8)));
    bw.addMutation(m);
    bw.flush();

    Scanner s = c.createScanner(tableName, Authorizations.EMPTY);
    assertTrue(!s.iterator().hasNext());

    assertTrue(c.namespaceOperations().listIterators(namespace).containsKey(iter));
    c.namespaceOperations().removeIterator(namespace, iter, EnumSet.copyOf(scope));

    c.namespaceOperations().addConstraint(namespace, NumericValueConstraint.class.getName());
    // doesn't take effect immediately, needs time to propagate
    UtilWaitThread.sleep(250);

    m = new Mutation("rowy");
    m.put("a", "b", new Value("abcde".getBytes(Constants.UTF8)));
    try {
      bw.addMutation(m);
      bw.flush();
      bw.close();
      fail();
    } catch (MutationsRejectedException e) {
      // supposed to be thrown
    }
    int num = c.namespaceOperations().listConstraints(namespace).get(NumericValueConstraint.class.getName());
    c.namespaceOperations().removeConstraint(namespace, num);
  }

  /**
   * Tests disallowed rename across namespaces
   */
  @Test
  public void testRenameTable() throws Exception {
    Connector c = getConnector();

    String[] uniqueNames = getTableNames(4);
    String namespace1 = uniqueNames[0];
    String namespace2 = uniqueNames[1];
    String tableName1 = namespace1 + "." + uniqueNames[2];
    String tableName2 = namespace2 + "." + uniqueNames[3];
    String tableName3 = namespace1 + "." + uniqueNames[3];

    c.namespaceOperations().create(namespace1);
    c.namespaceOperations().create(namespace2);
    c.tableOperations().create(tableName1);

    try {
      c.tableOperations().rename(tableName1, tableName2);
      fail();
    } catch (AccumuloException e) {
      // this is expected, because we don't allow renames across namespaces
    }

    c.tableOperations().rename(tableName1, tableName3);
  }

  /**
   * Tests new Namespace permissions as well as modifications to Table permissions because of namespaces. Checks each permission to first make sure the user
   * doesn't have permission to perform the action, then root grants them the permission and we check to make sure they could perform the action.
   */
  @Test
  public void testPermissions() throws Exception {
    Connector c = getConnector();

    String[] uniqueNames = getTableNames(8);
    String user1 = uniqueNames[0];
    String user2 = uniqueNames[1];
    PasswordToken pass = new PasswordToken(uniqueNames[2]);
    String n1 = uniqueNames[3];
    String n2 = uniqueNames[4];
    String t1 = uniqueNames[5];
    String t2 = uniqueNames[6];
    String t3 = uniqueNames[7];

    c.namespaceOperations().create(n1);
    c.tableOperations().create(n1 + "." + t1);

    c.securityOperations().createLocalUser(user1, pass);

    Connector user1Con = getConnector().getInstance().getConnector(user1, pass);

    try {
      user1Con.tableOperations().create(n1 + "." + t2);
      fail();
    } catch (AccumuloSecurityException e) {
      // supposed to happen
    }

    c.securityOperations().grantNamespacePermission(user1, n1, NamespacePermission.CREATE_TABLE);
    user1Con.tableOperations().create(n1 + "." + t2);
    assertTrue(c.tableOperations().list().contains(n1 + "." + t2));
    c.securityOperations().revokeNamespacePermission(user1, n1, NamespacePermission.CREATE_TABLE);

    try {
      user1Con.tableOperations().delete(n1 + "." + t1);
      fail();
    } catch (AccumuloSecurityException e) {
      // should happen
    }

    c.securityOperations().grantNamespacePermission(user1, n1, NamespacePermission.DROP_TABLE);
    user1Con.tableOperations().delete(n1 + "." + t1);
    assertTrue(!c.tableOperations().list().contains(n1 + "." + t1));
    c.securityOperations().revokeNamespacePermission(user1, n1, NamespacePermission.DROP_TABLE);

    c.tableOperations().create(n1 + "." + t3);
    BatchWriter bw = c.createBatchWriter(n1 + "." + t3, null);
    Mutation m = new Mutation("row");
    m.put("cf", "cq", "value");
    bw.addMutation(m);
    bw.close();

    Iterator<Entry<Key,Value>> i = user1Con.createScanner(n1 + "." + t3, new Authorizations()).iterator();
    try {
      i.next();
      fail();
    } catch (RuntimeException e) {
      // yup
    }

    m = new Mutation(user1);
    m.put("cf", "cq", "turtles");
    bw = user1Con.createBatchWriter(n1 + "." + t3, null);
    try {
      bw.addMutation(m);
      bw.close();
      fail();
    } catch (MutationsRejectedException e) {
      // good
    }

    c.securityOperations().grantNamespacePermission(user1, n1, NamespacePermission.READ);
    i = user1Con.createScanner(n1 + "." + t3, new Authorizations()).iterator();
    assertTrue(i.hasNext());
    c.securityOperations().revokeNamespacePermission(user1, n1, NamespacePermission.READ);

    c.securityOperations().grantNamespacePermission(user1, n1, NamespacePermission.WRITE);
    m = new Mutation(user1);
    m.put("cf", "cq", "turtles");
    bw = user1Con.createBatchWriter(n1 + "." + t3, null);
    bw.addMutation(m);
    bw.close();
    c.securityOperations().revokeNamespacePermission(user1, n1, NamespacePermission.WRITE);

    try {
      user1Con.tableOperations().setProperty(n1 + "." + t3, Property.TABLE_FILE_MAX.getKey(), "42");
      fail();
    } catch (AccumuloSecurityException e) {}

    c.securityOperations().grantNamespacePermission(user1, n1, NamespacePermission.ALTER_TABLE);
    user1Con.tableOperations().setProperty(n1 + "." + t3, Property.TABLE_FILE_MAX.getKey(), "42");
    user1Con.tableOperations().removeProperty(n1 + "." + t3, Property.TABLE_FILE_MAX.getKey());
    c.securityOperations().revokeNamespacePermission(user1, n1, NamespacePermission.ALTER_TABLE);

    try {
      user1Con.namespaceOperations().setProperty(n1, Property.TABLE_FILE_MAX.getKey(), "55");
      fail();
    } catch (AccumuloSecurityException e) {}

    c.securityOperations().grantNamespacePermission(user1, n1, NamespacePermission.ALTER_NAMESPACE);
    user1Con.namespaceOperations().setProperty(n1, Property.TABLE_FILE_MAX.getKey(), "42");
    user1Con.namespaceOperations().removeProperty(n1, Property.TABLE_FILE_MAX.getKey());
    c.securityOperations().revokeNamespacePermission(user1, n1, NamespacePermission.ALTER_NAMESPACE);

    c.securityOperations().createLocalUser(user2, pass);
    try {
      user1Con.securityOperations().grantNamespacePermission(user2, n1, NamespacePermission.ALTER_NAMESPACE);
      fail();
    } catch (AccumuloSecurityException e) {}

    c.securityOperations().grantNamespacePermission(user1, n1, NamespacePermission.GRANT);
    user1Con.securityOperations().grantNamespacePermission(user2, n1, NamespacePermission.ALTER_NAMESPACE);
    user1Con.securityOperations().revokeNamespacePermission(user2, n1, NamespacePermission.ALTER_NAMESPACE);
    c.securityOperations().revokeNamespacePermission(user1, n1, NamespacePermission.GRANT);

    try {
      user1Con.namespaceOperations().create(n2);
      fail();
    } catch (AccumuloSecurityException e) {}

    c.securityOperations().grantSystemPermission(user1, SystemPermission.CREATE_NAMESPACE);
    user1Con.namespaceOperations().create(n2);
    c.securityOperations().revokeSystemPermission(user1, SystemPermission.CREATE_NAMESPACE);

    try {
      user1Con.namespaceOperations().delete(n2);
      fail();
    } catch (AccumuloSecurityException e) {}

    c.securityOperations().grantSystemPermission(user1, SystemPermission.DROP_NAMESPACE);
    user1Con.namespaceOperations().delete(n2);
    c.securityOperations().revokeSystemPermission(user1, SystemPermission.DROP_NAMESPACE);

    try {
      user1Con.namespaceOperations().setProperty(n1, Property.TABLE_FILE_MAX.getKey(), "33");
      fail();
    } catch (AccumuloSecurityException e) {}

    c.securityOperations().grantSystemPermission(user1, SystemPermission.ALTER_NAMESPACE);
    user1Con.namespaceOperations().setProperty(n1, Property.TABLE_FILE_MAX.getKey(), "33");
    user1Con.namespaceOperations().removeProperty(n1, Property.TABLE_FILE_MAX.getKey());
    c.securityOperations().revokeSystemPermission(user1, SystemPermission.ALTER_NAMESPACE);
  }

  /**
   * This test makes sure that system-level iterators and constraints are ignored by the system namespace so that the metadata and root tables aren't affected
   */
  @Test
  public void excludeSystemIterConst() throws Exception {
    Connector c = getConnector();

    c.instanceOperations().setProperty("table.iterator.scan.sum", "20," + SimpleFilter.class.getName());
    assertTrue(c.instanceOperations().getSystemConfiguration().containsValue("20," + SimpleFilter.class.getName()));

    assertTrue(checkNamespaceHasProp(c, Constants.DEFAULT_NAMESPACE, "table.iterator.scan.sum", "20," + SimpleFilter.class.getName()));
    assertTrue(!checkNamespaceHasProp(c, Constants.SYSTEM_NAMESPACE, "table.iterator.scan.sum", "20," + SimpleFilter.class.getName()));
    c.instanceOperations().removeProperty("table.iterator.scan.sum");

    c.instanceOperations().setProperty("table.constraint.42", NumericValueConstraint.class.getName());
    assertTrue(c.instanceOperations().getSystemConfiguration().containsValue(NumericValueConstraint.class.getName()));

    assertTrue(checkNamespaceHasProp(c, Constants.DEFAULT_NAMESPACE, "table.constraint.42", NumericValueConstraint.class.getName()));
    assertTrue(!checkNamespaceHasProp(c, Constants.SYSTEM_NAMESPACE, "table.constraint.42", NumericValueConstraint.class.getName()));
    c.instanceOperations().removeProperty("table.constraint.42");
  }

  private boolean checkTableHasProp(Connector c, String t, String propKey, String propVal) throws AccumuloException, TableNotFoundException {
    for (Entry<String,String> e : c.tableOperations().getProperties(t)) {
      if (e.getKey().equals(propKey) && e.getValue().equals(propVal)) {
        return true;
      }
    }
    return false;
  }

  private boolean checkNamespaceHasProp(Connector c, String n, String propKey, String propVal) throws AccumuloException, NamespaceNotFoundException,
      AccumuloSecurityException {
    for (Entry<String,String> e : c.namespaceOperations().getProperties(n)) {
      if (e.getKey().equals(propKey) && e.getValue().equals(propVal)) {
        return true;
      }
    }
    return false;
  }

  public static class SimpleFilter extends Filter {
    @Override
    public boolean accept(Key k, Value v) {
      if (k.getColumnFamily().toString().equals("a"))
        return false;
      return true;
    }
  }
}
