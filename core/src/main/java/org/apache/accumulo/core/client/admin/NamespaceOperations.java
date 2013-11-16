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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

/**
 * Provides a class for administering namespaces
 * 
 */

public interface NamespaceOperations {

  /**
   * Retrieve a list of namespaces in Accumulo.
   * 
   * @return List of namespaces in accumulo
   */
  public SortedSet<String> list();

  /**
   * A method to check if a namespace exists in Accumulo.
   * 
   * @param namespace
   *          the name of the namespace
   * @return true if the namespace exists
   */
  public boolean exists(String namespace);

  /**
   * Create a namespace with no special configuration
   * 
   * @param namespace
   *          the name of the namespace
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceExistsException
   *           if the namespace already exists
   */
  public void create(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceExistsException;

  /**
   * @param namespace
   *          the name of the namespace
   * @param limitVersion
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceExistsException
   *           if the namespace already exists
   */
  public void create(String namespace, boolean limitVersion) throws AccumuloException, AccumuloSecurityException, NamespaceExistsException;

  /**
   * @param namespace
   *          the name of the namespace
   * @param versioningIter
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   * @param timeType
   *          specifies logical or real-time based time recording for entries in the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceExistsException
   *           if the namespace already exists
   */
  public void create(String namespace, boolean versioningIter, TimeType timeType) throws AccumuloException, AccumuloSecurityException, NamespaceExistsException;

  /**
   * Delete a namespace if it is empty
   * 
   * @param namespace
   *          the name of the namespace
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the namespace does not exist
   * @throws NamespaceNotEmptyException
   *           if the namespaces still contains tables
   * @throws TableNotFoundException
   *           if table not found while deleting
   */
  public void delete(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException, NamespaceNotEmptyException,
      TableNotFoundException;

  /**
   * Delete a namespace
   * 
   * @param namespace
   *          the name of the namespace
   * @param deleteTables
   *          boolean, if true deletes all the tables in the namespace in addition to deleting the namespace.
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the namespace does not exist
   * @throws NamespaceNotEmptyException
   *           if the namespaces still contains tables
   * @throws TableNotFoundException
   *           if table not found while deleting
   */
  public void delete(String namespace, boolean deleteTables) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException,
      NamespaceNotEmptyException, TableNotFoundException;

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
   */
  public void rename(String oldNamespaceName, String newNamespaceName) throws AccumuloSecurityException, NamespaceNotFoundException, AccumuloException,
      NamespaceExistsException;

  /**
   * Sets a property on a namespace which applies to all tables in the namespace. Note that it may take a short period of time (a second) to propagate the
   * change everywhere.
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
   */
  public void setProperty(String namespace, String property, String value) throws AccumuloException, AccumuloSecurityException;

  /**
   * Removes a property from a namespace. Note that it may take a short period of time (a second) to propagate the change everywhere.
   * 
   * @param namespace
   *          the name of the namespace
   * @param property
   *          the name of a per-table property
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  public void removeProperty(String namespace, String property) throws AccumuloException, AccumuloSecurityException;

  /**
   * Gets properties of a namespace. Note that recently changed properties may not be available immediately.
   * 
   * @param namespace
   *          the name of the namespace
   * @return all properties visible by this table (system and per-table properties). Note that recently changed properties may not be visible immediately.
   * @throws NamespaceNotFoundException
   *           if the namespace does not exist
   */
  public Iterable<Entry<String,String>> getProperties(String namespace) throws AccumuloException, NamespaceNotFoundException;

  /**
   * 
   * @param namespace
   *          the namespace to take offline
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws NamespaceNotFoundException
   *           if the namespace does not exist
   */
  public void offline(String namespace) throws AccumuloSecurityException, AccumuloException, NamespaceNotFoundException;

  /**
   * 
   * @param namespace
   *          the namespace to take online
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws NamespaceNotFoundException
   *           if the namespace does not exist
   */
  public void online(String namespace) throws AccumuloSecurityException, AccumuloException, NamespaceNotFoundException;

  /**
   * Get a mapping of namespace name to internal namespace id.
   * 
   * @return the map from namespace name to internal namespace id
   */
  public Map<String,String> namespaceIdMap();

  /**
   * Gets the number of bytes being used in the files for the set of tables in this namespace
   * 
   * @param namespace
   *          the namespace to get the set of tables from
   * 
   * @return a list of disk usage objects containing linked table names and sizes
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   */
  public List<DiskUsage> getDiskUsage(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Add an iterator to a namespace on all scopes.
   * 
   * @param namespace
   *          the name of the namespace
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the namespace
   * @throws AccumuloException
   * @throws NamespaceNotFoundException
   *           throw if the namespace no longer exists
   * @throws IllegalArgumentException
   *           if the setting conflicts with any existing iterators
   */
  public void attachIterator(String namespace, IteratorSetting setting) throws AccumuloSecurityException, AccumuloException, NamespaceNotFoundException;

  /**
   * Add an iterator to a namespace on the given scopes.
   * 
   * @param namespace
   *          the name of the namespace
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the namespace
   * @throws AccumuloException
   * @throws NamespaceNotFoundException
   *           throw if the namespace no longer exists
   * @throws IllegalArgumentException
   *           if the setting conflicts with any existing iterators
   */
  public void attachIterator(String namespace, IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException,
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
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the namespace
   * @throws AccumuloException
   * @throws NamespaceNotFoundException
   *           thrown if the namespace no longer exists
   */
  public void removeIterator(String namespace, String name, EnumSet<IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException,
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
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the namespace
   * @throws AccumuloException
   * @throws NamespaceNotFoundException
   *           thrown if the namespace no longer exists
   */
  public IteratorSetting getIteratorSetting(String namespace, String name, IteratorScope scope) throws AccumuloSecurityException, AccumuloException,
      NumberFormatException, NamespaceNotFoundException;

  /**
   * Get a list of iterators for this namespace.
   * 
   * @param namespace
   *          the name of the namespace
   * @return a set of iterator names
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the namespace
   * @throws AccumuloException
   * @throws NamespaceNotFoundException
   *           thrown if the namespace no longer exists
   */
  public Map<String,EnumSet<IteratorScope>> listIterators(String namespace) throws AccumuloSecurityException, AccumuloException, NamespaceNotFoundException;

  /**
   * Check whether a given iterator configuration conflicts with existing configuration; in particular, determine if the name or priority are already in use for
   * the specified scopes.
   * 
   * @param namespace
   *          the name of the namespace
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloException
   * @throws NamespaceNotFoundException
   *           thrown if the namespace no longer exists
   * @throws IllegalStateException
   *           if the setting conflicts with any existing iterators
   */
  public void checkIteratorConflicts(String namespace, IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloException,
      NamespaceNotFoundException;

  /**
   * Add a new constraint to a namespace.
   * 
   * @param namespace
   *          the name of the namespace
   * @param constraintClassName
   *          the full name of the constraint class
   * @return the unique number assigned to the constraint
   * @throws AccumuloException
   *           thrown if the constraint has already been added to the table or if there are errors in the configuration of existing constraints
   * @throws AccumuloSecurityException
   *           thrown if the user doesn't have permission to add the constraint
   * @throws NamespaceNotFoundException
   *           thrown if the namespace no longer exists
   */
  public int addConstraint(String namespace, String constraintClassName) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Remove a constraint from a namespace.
   * 
   * @param namespace
   *          the name of the namespace
   * @param number
   *          the unique number assigned to the constraint
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   *           thrown if the user doesn't have permission to remove the constraint
   */
  public void removeConstraint(String namespace, int number) throws AccumuloException, AccumuloSecurityException;

  /**
   * List constraints on a namespace with their assigned numbers.
   * 
   * @param namespace
   *          the name of the namespace
   * @return a map from constraint class name to assigned number
   * @throws AccumuloException
   *           thrown if there are errors in the configuration of existing constraints
   * @throws NamespaceNotFoundException
   *           thrown if the namespace no longer exists
   */
  public Map<String,Integer> listConstraints(String namespace) throws AccumuloException, NamespaceNotFoundException;

  /**
   * Test to see if the instance can load the given class as the given type. This check uses the table classpath property if it is set.
   * 
   * @return true if the instance can load the given class as the given type, false otherwise
   */
  boolean testClassLoad(String namespace, String className, String asTypeName) throws NamespaceNotFoundException, AccumuloException, AccumuloSecurityException;
}
