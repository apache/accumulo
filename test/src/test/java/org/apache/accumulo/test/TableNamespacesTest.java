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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNamespaceNotEmptyException;
import org.apache.accumulo.core.client.TableNamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.TableNamespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.examples.simple.constraints.NumericValueConstraint;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TableNamespacesTest {
  
  Random random = new Random();
  public static TemporaryFolder folder = new TemporaryFolder();
  static private MiniAccumuloCluster accumulo;
  static private String secret = "secret";
  
  @BeforeClass
  static public void setUp() throws Exception {
    folder.create();
    accumulo = new MiniAccumuloCluster(folder.getRoot(), secret);
    accumulo.start();
  }
  
  @AfterClass
  static public void tearDown() throws Exception {
    accumulo.stop();
    folder.delete();
  }
  
  /**
   * This test creates a table without specifying a namespace. In this case, it puts the table into the default namespace.
   * 
   * @throws Exception
   */
  @Test
  public void testDefaultNamespace() throws Exception {
    String tableName = "test";
    Connector c = accumulo.getConnector("root", secret);
    
    assertTrue(c.tableNamespaceOperations().exists(Constants.DEFAULT_TABLE_NAMESPACE));
    c.tableOperations().create(tableName);
    assertTrue(c.tableOperations().exists(tableName));
  }
  
  /**
   * This test creates a new namespace "testing" and a table "testing.table1" which puts "table1" into the "testing" namespace. Then we create "testing.table2"
   * which creates "table2" and puts it into "testing" as well. Then we make sure that you can't delete a namespace with tables in it, and then we delete the
   * tables and delete the namespace.
   * 
   * @throws Exception
   */
  @Test
  public void testCreateAndDeleteNamespace() throws Exception {
    String namespace = "testing";
    String tableName1 = namespace + ".table1";
    String tableName2 = namespace + ".table2";
    
    Connector c = accumulo.getConnector("root", secret);
    
    c.tableNamespaceOperations().create(namespace);
    assertTrue(c.tableNamespaceOperations().exists(namespace));
    
    c.tableOperations().create(tableName1);
    assertTrue(c.tableOperations().exists(tableName1));
    
    c.tableOperations().create(tableName2);
    assertTrue(c.tableOperations().exists(tableName2));
    
    // deleting
    try {
      // can't delete a namespace with tables in it
      c.tableNamespaceOperations().delete(namespace);
      fail();
    } catch (TableNamespaceNotEmptyException e) {
      // ignore, supposed to happen
    }
    assertTrue(c.tableNamespaceOperations().exists(namespace));
    assertTrue(c.tableOperations().exists(tableName1));
    assertTrue(c.tableOperations().exists(tableName2));
    
    c.tableOperations().delete(tableName2);
    assertTrue(!c.tableOperations().exists(tableName2));
    assertTrue(c.tableNamespaceOperations().exists(namespace));
    
    c.tableOperations().delete(tableName1);
    assertTrue(!c.tableOperations().exists(tableName1));
    c.tableNamespaceOperations().delete(namespace);
    assertTrue(!c.tableNamespaceOperations().exists(namespace));
  }
  
  /**
   * This test creates a namespace, modifies it's properties, and checks to make sure that those properties are applied to its tables. To do something on a
   * namespace-wide level, use TableNamespaceOperations.
   * 
   * Checks to make sure namespace-level properties are overridden by table-level properties.
   * 
   * Checks to see if the default namespace's properties work as well.
   * 
   * @throws Exception
   */
  
  @Test
  public void testNamespaceProperties() throws Exception {
    String namespace = "propchange";
    String tableName1 = namespace + ".table1";
    String tableName2 = namespace + ".table2";
    
    String propKey = Property.TABLE_SCAN_MAXMEM.getKey();
    String propVal = "42K";
    
    Connector c = accumulo.getConnector("root", secret);
    
    c.tableNamespaceOperations().create(namespace);
    c.tableOperations().create(tableName1);
    c.tableNamespaceOperations().setProperty(namespace, propKey, propVal);
    
    // check the namespace has the property
    assertTrue(checkTableNamespaceHasProp(c, namespace, propKey, propVal));
    
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
    c.tableNamespaceOperations().setProperty("propchange", propKey2, propVal2);
    
    assertTrue(checkTableHasProp(c, tableName2, propKey2, tablePropVal));
    
    // now check that you can change the default namespace's properties
    propVal = "13K";
    String tableName = "some_table";
    c.tableOperations().create(tableName);
    c.tableNamespaceOperations().setProperty(Constants.DEFAULT_TABLE_NAMESPACE, propKey, propVal);
    
    assertTrue(checkTableHasProp(c, tableName, propKey, propVal));
    
    // test the properties server-side by configuring an iterator.
    // should not show anything with column-family = 'a'
    String tableName3 = namespace + ".table3";
    c.tableOperations().create(tableName3);
    
    IteratorSetting setting = new IteratorSetting(250, "thing", SimpleFilter.class.getName());
    c.tableNamespaceOperations().attachIterator(namespace, setting);
    
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
   * This test creates a new user and a namespace. It checks to make sure the user can't modify anything in the namespace at first, then it grants the user
   * permissions and makes sure that they can modify the namespace. Then it also checks if the user has the correct permissions on tables both already existing
   * in the namespace and ones they create.
   * 
   * @throws Exception
   */
  @Test
  public void testNamespacePermissions() throws Exception {
    // TODO make the test once namespace-level permissions are implemented. (ACCUMULO-1479)
  }
  
  /**
   * This test renames and clones two separate table into different namespaces. different namespace.
   * 
   * @throws Exception
   */
  @Test
  public void testRenameAndCloneTableToNewNamespace() throws Exception {
    String namespace1 = "renamed";
    String namespace2 = "cloned";
    String tableName = "table";
    String tableName1 = "renamed.table1";
    String tableName2 = "cloned.table2";
    
    Connector c = accumulo.getConnector("root", secret);
    
    c.tableOperations().create(tableName);
    c.tableNamespaceOperations().create(namespace1);
    c.tableNamespaceOperations().create(namespace2);
    
    c.tableOperations().rename(tableName, tableName1);
    
    assertTrue(c.tableOperations().exists(tableName1));
    assertTrue(!c.tableOperations().exists(tableName));
    
    c.tableOperations().clone(tableName1, tableName2, false, null, null);
    
    assertTrue(c.tableOperations().exists(tableName1));
    assertTrue(c.tableOperations().exists(tableName2));
    return;
  }
  
  /**
   * This test renames a table namespace and ensures that its tables are still correct
   */
  @Test
  public void testNamespaceRename() throws Exception {
    String namespace1 = "n1";
    String namespace2 = "n2";
    String table = "t";
    
    Connector c = accumulo.getConnector("root", secret);
    Instance instance = c.getInstance();
    
    c.tableNamespaceOperations().create(namespace1);
    c.tableOperations().create(namespace1 + "." + table);
    
    c.tableNamespaceOperations().rename(namespace1, namespace2);
    
    assertTrue(!c.tableNamespaceOperations().exists(namespace1));
    assertTrue(c.tableNamespaceOperations().exists(namespace2));
    assertTrue(c.tableOperations().exists(namespace2 + "." + table));
    String tid = Tables.getTableId(instance, namespace2 + "." + table);
    String tnid = Tables.getNamespace(instance, tid);
    String tnamespace = TableNamespaces.getNamespaceName(instance, tnid);
    assertTrue(namespace2.equals(tnamespace));
  }
  
  /**
   * This test clones a table to a different namespace and ensures it's properties are correct
   */
  @Test
  public void testCloneTableProperties() throws Exception {
    String n1 = "namespace1";
    String n2 = "namespace2";
    String t1 = n1 + ".table";
    String t2 = n2 + ".table";
    
    String propKey = Property.TABLE_FILE_MAX.getKey();
    String propVal1 = "55";
    String propVal2 = "66";
    
    Connector c = accumulo.getConnector("root", secret);
    
    c.tableNamespaceOperations().create(n1);
    c.tableOperations().create(t1);
    
    c.tableOperations().removeProperty(t1, Property.TABLE_FILE_MAX.getKey());
    c.tableNamespaceOperations().setProperty(n1, propKey, propVal1);
    
    assertTrue(checkTableHasProp(c, t1, propKey, propVal1));
    
    c.tableNamespaceOperations().create(n2);
    c.tableNamespaceOperations().setProperty(n2, propKey, propVal2);
    c.tableOperations().clone(t1, t2, true, null, null);
    c.tableOperations().removeProperty(t2, propKey);
    
    assertTrue(checkTableHasProp(c, t2, propKey, propVal2));
    
    c.tableNamespaceOperations().delete(n1, true);
    c.tableNamespaceOperations().delete(n2, true);
  }
  
  /**
   * This test clones namespaces. First checks to see that the properties were correctly copied over, then checks to see that the correct properties were set
   * when given that option and that table properties copy successfully.
   */
  @Test
  public void testCloneNamespace() throws Exception {
    String n1 = "nspace1";
    String n2 = "nspace2";
    String n3 = "nspace3";
    String t = ".table";
    
    String propKey1 = Property.TABLE_FILE_MAX.getKey();
    String propKey2 = Property.TABLE_SCAN_MAXMEM.getKey();
    String propVal1 = "55";
    String propVal2 = "66";
    String propVal3 = "77K";
    
    Connector c = accumulo.getConnector("root", secret);
    c.tableNamespaceOperations().create(n1);
    c.tableOperations().create(n1 + t);
    
    c.tableNamespaceOperations().setProperty(n1, propKey1, propVal1);
    c.tableOperations().setProperty(n1 + t, propKey1, propVal2);
    c.tableNamespaceOperations().setProperty(n1, propKey2, propVal3);
    
    c.tableNamespaceOperations().clone(n1, n2, false, null, null, false);
    assertTrue(c.tableNamespaceOperations().exists(n2));
    assertTrue(checkTableNamespaceHasProp(c, n2, propKey1, propVal1));
    assertTrue(checkTableHasProp(c, n2 + t, propKey1, propVal2));
    assertTrue(checkTableNamespaceHasProp(c, n2, propKey2, propVal3));
    
    Map<String,String> propsToSet = new HashMap<String,String>();
    propsToSet.put(propKey1, propVal1);
    c.tableNamespaceOperations().clone(n1, n3, true, propsToSet, null, true);
    
    assertTrue(checkTableNamespaceHasProp(c, n3, propKey1, propVal1));
    assertTrue(checkTableHasProp(c, n3 + t, propKey1, propVal2));
    assertTrue(checkTableNamespaceHasProp(c, n3, propKey2, propVal3));
    assertTrue(!checkTableHasProp(c, n3 + t, propKey2, propVal3));
  }
  
  /**
   * This tests adding iterators to a namespace, listing them, and removing them as well as adding and removing constraints
   */
  @Test
  public void testNamespaceIterators() throws Exception {
    Connector c = accumulo.getConnector("root", secret);
    
    String namespace = "iterator";
    String tableName = namespace + ".table";
    String iter = "thing";
    
    c.tableNamespaceOperations().create(namespace);
    c.tableOperations().create(tableName);
    
    IteratorSetting setting = new IteratorSetting(250, iter, SimpleFilter.class.getName());
    HashSet<IteratorScope> scope = new HashSet<IteratorScope>();
    scope.add(IteratorScope.scan);
    c.tableNamespaceOperations().attachIterator(namespace, setting, EnumSet.copyOf(scope));
    
    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("r");
    m.put("a", "b", new Value("abcde".getBytes(Constants.UTF8)));
    bw.addMutation(m);
    bw.flush();
    
    Scanner s = c.createScanner(tableName, Authorizations.EMPTY);
    assertTrue(!s.iterator().hasNext());
    
    assertTrue(c.tableNamespaceOperations().listIterators(namespace).containsKey(iter));
    c.tableNamespaceOperations().removeIterator(namespace, iter, EnumSet.copyOf(scope));
    
    c.tableNamespaceOperations().addConstraint(namespace, NumericValueConstraint.class.getName());
    // doesn't take effect immediately, needs time to propagate
    UtilWaitThread.sleep(250);
    
    m = new Mutation("rowy");
    m.put("a", "b", new Value("abcde".getBytes(Constants.UTF8)));
    try {
      bw.addMutation(m);
      bw.flush();
      fail();
    } catch (MutationsRejectedException e) {
      // supposed to be thrown
    }
    bw.close();
    
    int num = c.tableNamespaceOperations().listConstraints(namespace).get(NumericValueConstraint.class.getName());
    c.tableNamespaceOperations().removeConstraint(namespace, num);
  }
  
  private boolean checkTableHasProp(Connector c, String t, String propKey, String propVal) throws AccumuloException, TableNotFoundException {
    for (Entry<String,String> e : c.tableOperations().getProperties(t)) {
      if (e.getKey().equals(propKey) && e.getValue().equals(propVal)) {
        return true;
      }
    }
    return false;
  }
  
  private boolean checkTableNamespaceHasProp(Connector c, String n, String propKey, String propVal) throws AccumuloException, TableNamespaceNotFoundException {
    for (Entry<String,String> e : c.tableNamespaceOperations().getProperties(n)) {
      if (e.getKey().equals(propKey) && e.getValue().equals(propVal)) {
        return true;
      }
    }
    return false;
  }
  
  public static class SimpleFilter extends Filter {
    public boolean accept(Key k, Value v) {
      if (k.getColumnFamily().toString().equals("a"))
        return false;
      return true;
    }
  }
}
