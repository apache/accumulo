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
package org.apache.accumulo.core.client.admin;

import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

/**
 * Provides an API for administering namespaces
 *
 * All tables exist in a namespace. The default namespace has no name, and is used if an explicit namespace is not specified. Fully qualified table names look
 * like "namespaceName.tableName". Tables in the default namespace are fully qualified simply as "tableName".
 *
 * @since 1.6.0
 */
public interface NamespaceOperations {

  /**
   * Returns the name of the system reserved namespace
   *
   * @return the name of the system namespace
   * @since 1.6.0
   */
  String systemNamespace();

  /**
   * Returns the name of the default namespace
   *
   * @return the name of the default namespace
   * @since 1.6.0
   */
  String defaultNamespace();

  /**
   * Retrieve a list of namespaces in Accumulo.
   *
   * @return List of namespaces in accumulo
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @since 1.6.0
   */
  SortedSet<String> list() throws AccumuloException, AccumuloSecurityException;

  /**
   * A method to check if a namespace exists in Accumulo.
   *
   * @param namespace
   *          the name of the namespace
   * @return true if the namespace exists
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @since 1.6.0
   */
  boolean exists(String namespace) throws AccumuloException, AccumuloSecurityException;

  /**
   * Create an empty namespace with no initial configuration. Valid names for a namespace contain letters, numbers, and the underscore character.
   *
   * @param namespace
   *          the name of the namespace
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceExistsException
   *           if the specified namespace already exists
   * @since 1.6.0
   */
  void create(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceExistsException;

  /**
   * Delete an empty namespace
   *
   * @param namespace
   *          the name of the namespace
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @throws NamespaceNotEmptyException
   *           if the namespaces still contains tables
   * @since 1.6.0
   */
  void delete(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException, NamespaceNotEmptyException;

  /**
   * Rename a namespace
   *
   * @param oldNamespaceName
   *          the old namespace name
   * @param newNamespaceName
   *          the new namespace name
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the old namespace does not exist
   * @throws NamespaceExistsException
   *           if the new namespace already exists
   * @since 1.6.0
   */
  void rename(String oldNamespaceName, String newNamespaceName) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException,
      NamespaceExistsException;

  /**
   * Sets a property on a namespace which applies to all tables in the namespace. Note that it may take a few seconds to propagate the change everywhere.
   *
   * @param namespace
   *          the name of the namespace
   * @param property
   *          the name of a per-table property
   * @param value
   *          the value to set a per-table property to
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  void setProperty(String namespace, String property, String value) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Removes a property from a namespace. Note that it may take a few seconds to propagate the change everywhere.
   *
   * @param namespace
   *          the name of the namespace
   * @param property
   *          the name of a per-table property
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  void removeProperty(String namespace, String property) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Gets properties of a namespace, which are inherited by tables in this namespace. Note that recently changed properties may not be available immediately.
   *
   * @param namespace
   *          the name of the namespace
   * @return all properties visible by this namespace (system and per-table properties). Note that recently changed properties may not be visible immediately.
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  Iterable<Entry<String,String>> getProperties(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Get a mapping of namespace name to internal namespace id.
   *
   * @return the map from namespace name to internal namespace id
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @since 1.6.0
   */
  Map<String,String> namespaceIdMap() throws AccumuloException, AccumuloSecurityException;

  /**
   * Add an iterator to a namespace on all scopes.
   *
   * @param namespace
   *          the name of the namespace
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  void attachIterator(String namespace, IteratorSetting setting) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Add an iterator to a namespace on the given scopes.
   *
   * @param namespace
   *          the name of the namespace
   * @param setting
   *          object specifying the properties of the iterator
   * @param scopes
   *          the set of scopes the iterator should apply to
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  void attachIterator(String namespace, IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloException, AccumuloSecurityException,
      NamespaceNotFoundException;

  /**
   * Remove an iterator from a namespace by name.
   *
   * @param namespace
   *          the name of the namespace
   * @param name
   *          the name of the iterator
   * @param scopes
   *          the scopes of the iterator
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  void removeIterator(String namespace, String name, EnumSet<IteratorScope> scopes) throws AccumuloException, AccumuloSecurityException,
      NamespaceNotFoundException;

  /**
   * Get the settings for an iterator.
   *
   * @param namespace
   *          the name of the namespace
   * @param name
   *          the name of the iterator
   * @param scope
   *          the scope of the iterator
   * @return the settings for this iterator
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  IteratorSetting getIteratorSetting(String namespace, String name, IteratorScope scope) throws AccumuloException, AccumuloSecurityException,
      NamespaceNotFoundException;

  /**
   * Get a list of iterators for this namespace.
   *
   * @param namespace
   *          the name of the namespace
   * @return a set of iterator names
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  Map<String,EnumSet<IteratorScope>> listIterators(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Check whether a given iterator configuration conflicts with existing configuration; in particular, determine if the name or priority are already in use for
   * the specified scopes. If so, an IllegalArgumentException is thrown, wrapped in an AccumuloException.
   *
   * @param namespace
   *          the name of the namespace
   * @param setting
   *          object specifying the properties of the iterator
   * @param scopes
   *          the scopes of the iterator
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  void checkIteratorConflicts(String namespace, IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloException, AccumuloSecurityException,
      NamespaceNotFoundException;

  /**
   * Add a new constraint to a namespace.
   *
   * @param namespace
   *          the name of the namespace
   * @param constraintClassName
   *          the full name of the constraint class
   * @return the unique id number assigned to the constraint
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  int addConstraint(String namespace, String constraintClassName) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Remove a constraint from a namespace.
   *
   * @param namespace
   *          the name of the namespace
   * @param id
   *          the unique id number assigned to the constraint
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  void removeConstraint(String namespace, int id) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * List constraints on a namespace with their assigned numbers.
   *
   * @param namespace
   *          the name of the namespace
   * @return a map from constraint class name to assigned number
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  Map<String,Integer> listConstraints(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Test to see if the instance can load the given class as the given type. This check uses the table classpath property if it is set.
   *
   * @param namespace
   *          the name of the namespace
   * @param className
   *          the class to try to load
   * @param asTypeName
   *          the interface or superclass the given class is attempted to load as
   * @return true if the instance can load the given class as the given type, false otherwise
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the specified namespace doesn't exist
   * @since 1.6.0
   */
  boolean testClassLoad(String namespace, String className, String asTypeName) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException;
}
