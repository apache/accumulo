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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.impl.NamespaceOperationsHelper;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated since 1.8.0; use MiniAccumuloCluster or a standard mock framework instead.
 */
@Deprecated
class MockNamespaceOperations extends NamespaceOperationsHelper {

  private static final Logger log = LoggerFactory.getLogger(MockNamespaceOperations.class);

  final private MockAccumulo acu;
  final private String username;

  MockNamespaceOperations(MockAccumulo acu, String username) {
    this.acu = acu;
    this.username = username;
  }

  @Override
  public SortedSet<String> list() {
    return new TreeSet<>(acu.namespaces.keySet());
  }

  @Override
  public boolean exists(String namespace) {
    return acu.namespaces.containsKey(namespace);
  }

  @Override
  public void create(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceExistsException {
    if (!namespace.matches(Namespaces.VALID_NAME_REGEX))
      throw new IllegalArgumentException();

    if (exists(namespace))
      throw new NamespaceExistsException(namespace, namespace, "");
    else
      acu.createNamespace(username, namespace);
  }

  @Override
  public void delete(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException, NamespaceNotEmptyException {
    if (acu.namespaces.get(namespace).getTables(acu).size() > 0) {
      throw new NamespaceNotEmptyException(null, namespace, null);
    }
    acu.namespaces.remove(namespace);
  }

  @Override
  public void rename(String oldNamespaceName, String newNamespaceName) throws AccumuloSecurityException, NamespaceNotFoundException, AccumuloException,
      NamespaceExistsException {
    if (!exists(oldNamespaceName))
      throw new NamespaceNotFoundException(oldNamespaceName, oldNamespaceName, "");
    if (exists(newNamespaceName))
      throw new NamespaceExistsException(newNamespaceName, newNamespaceName, "");

    MockNamespace n = acu.namespaces.get(oldNamespaceName);
    for (String t : n.getTables(acu)) {
      String tt = newNamespaceName + "." + Tables.qualify(t).getSecond();
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
  public Iterable<Entry<String,String>> getProperties(String namespace) throws NamespaceNotFoundException {
    if (!exists(namespace)) {
      throw new NamespaceNotFoundException(namespace, namespace, "");
    }

    return acu.namespaces.get(namespace).settings.entrySet();
  }

  @Override
  public Map<String,String> namespaceIdMap() {
    Map<String,String> result = new HashMap<>();
    for (String table : acu.tables.keySet()) {
      result.put(table, table);
    }
    return result;
  }

  @Override
  public boolean testClassLoad(String namespace, String className, String asTypeName) throws AccumuloException, AccumuloSecurityException,
      NamespaceNotFoundException {

    try {
      AccumuloVFSClassLoader.loadClass(className, Class.forName(asTypeName));
    } catch (ClassNotFoundException e) {
      log.warn("Could not load class '" + className + "' with type name '" + asTypeName + "' in testClassLoad()", e);
      return false;
    }
    return true;
  }
}
