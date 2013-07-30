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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNamespaceExistsException;
import org.apache.accumulo.core.client.TableNamespaceNotEmptyException;
import org.apache.accumulo.core.client.TableNamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.admin.TableNamespaceOperationsHelper;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.security.Credentials;
import org.apache.commons.lang.NotImplementedException;

public class MockTableNamespaceOperations extends TableNamespaceOperationsHelper {

  final private MockAccumulo acu;
  final private String username;

  MockTableNamespaceOperations(MockAccumulo acu, String username) {
    this.acu = acu;
    this.username = username;
  }

  @Override
  public SortedSet<String> list() {
    return new TreeSet<String>(acu.namespaces.keySet());
  }

  @Override
  public boolean exists(String tableName) {
    return acu.namespaces.containsKey(tableName);
  }

  @Override
  public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableNamespaceExistsException {
    create(tableName, true, TimeType.MILLIS);
  }

  @Override
  public void create(String tableName, boolean versioningIter) throws AccumuloException, AccumuloSecurityException, TableNamespaceExistsException {
    create(tableName, versioningIter, TimeType.MILLIS);
  }

  @Override
  public void create(String namespace, boolean versioningIter, TimeType timeType) throws AccumuloException, AccumuloSecurityException,
      TableNamespaceExistsException {
    if (!namespace.matches(Constants.VALID_TABLE_NAMESPACE_REGEX)) {
      throw new IllegalArgumentException();
    }

    if (exists(namespace))
      throw new TableNamespaceExistsException(namespace, namespace, "");

    if (!exists(namespace)) {
      acu.createNamespace(username, namespace);
    }
    acu.createTable(username, namespace, versioningIter, timeType);
  }

  @Override
  public void delete(String namespace) throws AccumuloException, AccumuloSecurityException, TableNamespaceNotFoundException, TableNamespaceNotEmptyException {
    delete(namespace, false);
  }

  @Override
  public void delete(String namespace, boolean deleteTables) throws AccumuloException, AccumuloSecurityException, TableNamespaceNotFoundException,
      TableNamespaceNotEmptyException {
    if (!exists(namespace))
      throw new TableNamespaceNotFoundException(namespace, namespace, "");

    MockTableNamespace n = acu.namespaces.get(namespace);
    if (!deleteTables) {
      if (n.getTables(acu).size() > 0) {
        throw new TableNamespaceNotEmptyException(null, namespace, null);
      }
    } else {
      for (String t : n.getTables(acu)) {
        try {
          new MockTableOperations(acu, username).delete(t);
        } catch (TableNotFoundException e) {
          System.err.println("Table (" + e.getTableName() + ") not found while deleting namespace (" + namespace + ")");
        }
      }
    }
    acu.namespaces.remove(namespace);
  }

  @Override
  public void rename(String oldNamespaceName, String newNamespaceName) throws AccumuloSecurityException, TableNamespaceNotFoundException, AccumuloException,
      TableNamespaceExistsException {
    if (!exists(oldNamespaceName))
      throw new TableNamespaceNotFoundException(oldNamespaceName, oldNamespaceName, "");
    if (exists(newNamespaceName))
      throw new TableNamespaceExistsException(newNamespaceName, newNamespaceName, "");

    MockTableNamespace n = acu.namespaces.get(oldNamespaceName);
    for (String t : n.getTables(acu)) {
      String tt = newNamespaceName + "." + Tables.extractTableName(t);
      acu.tables.put(tt, acu.tables.remove(t));
    }
    acu.namespaces.put(newNamespaceName, acu.namespaces.remove(oldNamespaceName));
  }

  @Override
  public void setProperty(String namespace, String property, String value) throws AccumuloException, AccumuloSecurityException {
    acu.namespaces.get(namespace).settings.put(property, value);
  }

  @Override
  public void removeProperty(String namespace, String property) throws AccumuloException, AccumuloSecurityException {
    acu.namespaces.get(namespace).settings.remove(property);
  }

  @Override
  public Iterable<Entry<String,String>> getProperties(String namespace) throws TableNamespaceNotFoundException {
    if (!exists(namespace)) {
      throw new TableNamespaceNotFoundException(namespace, namespace, "");
    }

    return acu.namespaces.get(namespace).settings.entrySet();
  }

  @Override
  public void offline(String namespace) throws AccumuloSecurityException, AccumuloException, TableNamespaceNotFoundException {
    if (!exists(namespace))
      throw new TableNamespaceNotFoundException(namespace, namespace, "");
  }

  @Override
  public void online(String namespace) throws AccumuloSecurityException, AccumuloException, TableNamespaceNotFoundException {
    if (!exists(namespace))
      throw new TableNamespaceNotFoundException(namespace, namespace, "");
  }

  @Override
  public Map<String,String> namespaceIdMap() {
    Map<String,String> result = new HashMap<String,String>();
    for (String table : acu.tables.keySet()) {
      result.put(table, table);
    }
    return result;
  }

  @Override
  public List<DiskUsage> getDiskUsage(String namespace) throws AccumuloException, AccumuloSecurityException {

    List<DiskUsage> diskUsages = new ArrayList<DiskUsage>();
    diskUsages.add(new DiskUsage(new TreeSet<String>(acu.namespaces.get(namespace).getTables(acu)), 0l));

    return diskUsages;
  }

  @Override
  public void clone(String srcName, String newName, boolean flush, Map<String,String> propertiesToSet, Set<String> propertiesToExclude, Boolean copyTableProps)
      throws AccumuloSecurityException, AccumuloException, TableNamespaceNotFoundException, TableNamespaceExistsException {
    // TODO Implement clone in Mock
    throw new NotImplementedException();
  }
}
