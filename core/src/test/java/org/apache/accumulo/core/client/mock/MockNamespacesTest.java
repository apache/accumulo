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

package org.apache.accumulo.core.client.mock;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

@Deprecated
public class MockNamespacesTest {

  @Rule
  public TestName test = new TestName();

  private Connector conn;

  @Before
  public void setupInstance() throws Exception {
    Instance inst = new MockInstance(test.getMethodName());
    conn = inst.getConnector("user", new PasswordToken("pass"));
  }

  /**
   * This test creates a table without specifying a namespace. In this case, it puts the table into the default namespace.
   */
  @Test
  public void testDefaultNamespace() throws Exception {
    String tableName = "test";

    assertTrue(conn.namespaceOperations().exists(Namespace.DEFAULT));
    conn.tableOperations().create(tableName);
    assertTrue(conn.tableOperations().exists(tableName));
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

    conn.namespaceOperations().create(namespace);
    assertTrue(conn.namespaceOperations().exists(namespace));

    conn.tableOperations().create(tableName1);
    assertTrue(conn.tableOperations().exists(tableName1));

    conn.tableOperations().create(tableName2);
    assertTrue(conn.tableOperations().exists(tableName2));

    // deleting
    try {
      // can't delete a namespace with tables in it
      conn.namespaceOperations().delete(namespace);
      fail();
    } catch (NamespaceNotEmptyException e) {
      // ignore, supposed to happen
    }
    assertTrue(conn.namespaceOperations().exists(namespace));
    assertTrue(conn.tableOperations().exists(tableName1));
    assertTrue(conn.tableOperations().exists(tableName2));

    conn.tableOperations().delete(tableName2);
    assertTrue(!conn.tableOperations().exists(tableName2));
    assertTrue(conn.namespaceOperations().exists(namespace));

    conn.tableOperations().delete(tableName1);
    assertTrue(!conn.tableOperations().exists(tableName1));
    conn.namespaceOperations().delete(namespace);
    assertTrue(!conn.namespaceOperations().exists(namespace));
  }

  /**
   * This test creates a namespace, modifies it's properties, and checks to make sure that those properties are applied to its tables. To do something on a
   * namespace-wide level, use {@link NamespaceOperations}.
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

    conn.namespaceOperations().create(namespace);
    conn.tableOperations().create(tableName1);
    conn.namespaceOperations().setProperty(namespace, propKey, propVal);

    // check the namespace has the property
    assertTrue(checkNamespaceHasProp(conn, namespace, propKey, propVal));

    // check that the table gets it from the namespace
    assertTrue(checkTableHasProp(conn, tableName1, propKey, propVal));

    // test a second table to be sure the first wasn't magical
    // (also, changed the order, the namespace has the property already)
    conn.tableOperations().create(tableName2);
    assertTrue(checkTableHasProp(conn, tableName2, propKey, propVal));

    // test that table properties override namespace properties
    String propKey2 = Property.TABLE_FILE_MAX.getKey();
    String propVal2 = "42";
    String tablePropVal = "13";

    conn.tableOperations().setProperty(tableName2, propKey2, tablePropVal);
    conn.namespaceOperations().setProperty("propchange", propKey2, propVal2);

    assertTrue(checkTableHasProp(conn, tableName2, propKey2, tablePropVal));

    // now check that you can change the default namespace's properties
    propVal = "13K";
    String tableName = "some_table";
    conn.tableOperations().create(tableName);
    conn.namespaceOperations().setProperty(Namespace.DEFAULT, propKey, propVal);

    assertTrue(checkTableHasProp(conn, tableName, propKey, propVal));

    // test the properties server-side by configuring an iterator.
    // should not show anything with column-family = 'a'
    String tableName3 = namespace + ".table3";
    conn.tableOperations().create(tableName3);

    IteratorSetting setting = new IteratorSetting(250, "thing", SimpleFilter.class.getName());
    conn.namespaceOperations().attachIterator(namespace, setting);

    BatchWriter bw = conn.createBatchWriter(tableName3, new BatchWriterConfig());
    Mutation m = new Mutation("r");
    m.put("a", "b", new Value("abcde".getBytes()));
    bw.addMutation(m);
    bw.flush();
    bw.close();

    // Scanner s = c.createScanner(tableName3, Authorizations.EMPTY);
    // do scanners work correctly in mock?
    // assertTrue(!s.iterator().hasNext());
  }

  /**
   * This test renames and clones two separate table into different namespaces. different namespace.
   */
  @Test
  public void testRenameAndCloneTableToNewNamespace() throws Exception {
    String namespace1 = "renamed";
    String namespace2 = "cloned";
    String tableName = "table";
    String tableName1 = "renamed.table1";
    // String tableName2 = "cloned.table2";

    conn.tableOperations().create(tableName);
    conn.namespaceOperations().create(namespace1);
    conn.namespaceOperations().create(namespace2);

    conn.tableOperations().rename(tableName, tableName1);

    assertTrue(conn.tableOperations().exists(tableName1));
    assertTrue(!conn.tableOperations().exists(tableName));

    // TODO implement clone in mock
    // c.tableOperations().clone(tableName1, tableName2, false, null, null);
    // assertTrue(c.tableOperations().exists(tableName1)); assertTrue(c.tableOperations().exists(tableName2));
  }

  /**
   * This test renames a namespace and ensures that its tables are still correct
   */
  @Test
  public void testNamespaceRename() throws Exception {
    String namespace1 = "n1";
    String namespace2 = "n2";
    String table = "t";

    conn.namespaceOperations().create(namespace1);
    conn.tableOperations().create(namespace1 + "." + table);

    conn.namespaceOperations().rename(namespace1, namespace2);

    assertTrue(!conn.namespaceOperations().exists(namespace1));
    assertTrue(conn.namespaceOperations().exists(namespace2));
    assertTrue(!conn.tableOperations().exists(namespace1 + "." + table));
    assertTrue(conn.tableOperations().exists(namespace2 + "." + table));
  }

  /**
   * This tests adding iterators to a namespace, listing them, and removing them
   */
  @Test
  public void testNamespaceIterators() throws Exception {
    String namespace = "iterator";
    String tableName = namespace + ".table";
    String iter = "thing";

    conn.namespaceOperations().create(namespace);
    conn.tableOperations().create(tableName);

    IteratorSetting setting = new IteratorSetting(250, iter, SimpleFilter.class.getName());
    HashSet<IteratorScope> scope = new HashSet<>();
    scope.add(IteratorScope.scan);
    conn.namespaceOperations().attachIterator(namespace, setting, EnumSet.copyOf(scope));

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("r");
    m.put("a", "b", new Value("abcde".getBytes(UTF_8)));
    bw.addMutation(m);
    bw.flush();

    Scanner s = conn.createScanner(tableName, Authorizations.EMPTY);
    System.out.println(s.iterator().next());
    // do scanners work correctly in mock?
    // assertTrue(!s.iterator().hasNext());

    assertTrue(conn.namespaceOperations().listIterators(namespace).containsKey(iter));
    conn.namespaceOperations().removeIterator(namespace, iter, EnumSet.copyOf(scope));
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
