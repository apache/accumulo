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
import java.util.Set;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNamespaceExistsException;
import org.apache.accumulo.core.client.TableNamespaceNotEmptyException;
import org.apache.accumulo.core.client.TableNamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

/**
 * Provides a class for administering table namespaces
 * 
 */

public interface TableNamespaceOperations {
  
  /**
   * Retrieve a list of table namespaces in Accumulo.
   * 
   * @return List of table namespaces in accumulo
   */
  public SortedSet<String> list();
  
  /**
   * A method to check if a table namespace exists in Accumulo.
   * 
   * @param namespace
   *          the name of the table namespace
   * @return true if the table namespace exists
   */
  public boolean exists(String namespace);
  
  /**
   * Create a table namespace with no special configuration
   * 
   * @param namespace
   *          the name of the table namespace
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNamespaceExistsException
   *           if the table namespace already exists
   */
  public void create(String namespace) throws AccumuloException, AccumuloSecurityException, TableNamespaceExistsException;
  
  /**
   * @param namespace
   *          the name of the table namespace
   * @param limitVersion
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNamespaceExistsException
   *           if the table namespace already exists
   */
  public void create(String namespace, boolean limitVersion) throws AccumuloException, AccumuloSecurityException, TableNamespaceExistsException;
  
  /**
   * @param namespace
   *          the name of the table namespace
   * @param versioningIter
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   * @param timeType
   *          specifies logical or real-time based time recording for entries in the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNamespaceExistsException
   *           if the table namespace already exists
   */
  public void create(String namespace, boolean versioningIter, TimeType timeType) throws AccumuloException, AccumuloSecurityException,
      TableNamespaceExistsException;
  
  /**
   * Delete a table namespace if it is empty
   * 
   * @param namespace
   *          the name of the table namespace
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNamespaceNotFoundException
   *           if the table namespace does not exist
   * @throws TableNamespaceNotEmptyException
   *           if the table namespaces still contains tables
   * @throws TableNotFoundException 
   *           if table not found while deleting
   */
  public void delete(String namespace) throws AccumuloException, AccumuloSecurityException, TableNamespaceNotFoundException, TableNamespaceNotEmptyException, TableNotFoundException;
  
  /**
   * Delete a table namespace
   * 
   * @param namespace
   *          the name of the table namespace
   * @param deleteTables
   *          boolean, if true deletes all the tables in the namespace in addition to deleting the namespace.
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNamespaceNotFoundException
   *           if the table namespace does not exist
   * @throws TableNamespaceNotEmptyException
   *           if the table namespaces still contains tables
   * @throws TableNotFoundException 
   *           if table not found while deleting
   */
  public void delete(String namespace, boolean deleteTables) throws AccumuloException, AccumuloSecurityException, TableNamespaceNotFoundException,
      TableNamespaceNotEmptyException, TableNotFoundException;
  
  /**
   * Rename a table namespace
   * 
   * @param oldNamespaceName
   *          the old table namespace name
   * @param newNamespaceName
   *          the new table namespace name
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNamespaceNotFoundException
   *           if the old table namespace does not exist
   * @throws TableNamespaceExistsException
   *           if the new table namespace already exists
   */
  public void rename(String oldNamespaceName, String newNamespaceName) throws AccumuloSecurityException, TableNamespaceNotFoundException, AccumuloException,
      TableNamespaceExistsException;
  
  /**
   * Sets a property on a table namespace. Note that it may take a short period of time (a second) to propagate the change everywhere.
   * 
   * @param namespace
   *          the name of the table namespace
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
   * Removes a property from a table namespace. Note that it may take a short period of time (a second) to propagate the change everywhere.
   * 
   * @param namespace
   *          the name of the table namespace
   * @param property
   *          the name of a per-table property
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  public void removeProperty(String namespace, String property) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Gets properties of a table namespace. Note that recently changed properties may not be available immediately.
   * 
   * @param namespace
   *          the name of the table namespace
   * @return all properties visible by this table (system and per-table properties). Note that recently changed properties may not be visible immediately.
   * @throws TableNamespaceNotFoundException
   *           if the table does not exist
   */
  public Iterable<Entry<String,String>> getProperties(String namespace) throws AccumuloException, TableNamespaceNotFoundException;
  
  /**
   * 
   * @param namespace
   *          the table namespace to take offline
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws TableNamespaceNotFoundException
   */
  public void offline(String namespace) throws AccumuloSecurityException, AccumuloException, TableNamespaceNotFoundException;
  
  /**
   * 
   * @param namespace
   *          the table namespace to take online
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws TableNamespaceNotFoundException
   */
  public void online(String namespace) throws AccumuloSecurityException, AccumuloException, TableNamespaceNotFoundException;
  
  /**
   * Get a mapping of table namespace name to internal table namespace id.
   * 
   * @return the map from table namespace name to internal table namespace id
   */
  public Map<String,String> namespaceIdMap();
  
  /**
   * Gets the number of bytes being used in the files for the set of tables in this namespace
   * 
   * @param namespace
   *          the table namespace to get the set of tables from
   * 
   * @return a list of disk usage objects containing linked table names and sizes
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   */
  public List<DiskUsage> getDiskUsage(String namespace) throws AccumuloException, AccumuloSecurityException, TableNamespaceNotFoundException;
  
  /**
   * Clone a all the tables in a table namespace to a new table namespace. Optionally copy all their properties as well.
   * 
   * @param srcName
   *          The table namespace to clone
   * @param newName
   *          The new table namespace to clone to
   * @param flush
   *          Whether to flush each table before cloning
   * @param propertiesToSet
   *          Which table namespace properties to set
   * @param propertiesToExclude
   *          Which table namespace properties to exclude
   * @param copyTableProps
   *          Whether to copy each table's properties
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws TableNamespaceNotFoundException
   *           If the old table namespace doesn't exist
   * @throws TableNamespaceExistsException
   *           If the new table namespace already exists
   */
  public void clone(String srcName, String newName, boolean flush, Map<String,String> propertiesToSet, Set<String> propertiesToExclude, Boolean copyTableProps)
      throws AccumuloSecurityException, AccumuloException, TableNamespaceNotFoundException, TableNamespaceExistsException;
  
  /**
   * Add an iterator to a table namespace on all scopes.
   * 
   * @param tableNamespace
   *          the name of the table namespace
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the table
   * @throws AccumuloException
   * @throws TableNamespaceNotFoundException
   *           throw if the table namespace no longer exists
   * @throws IllegalArgumentException
   *           if the setting conflicts with any existing iterators
   */
  public void attachIterator(String tableNamespace, IteratorSetting setting) throws AccumuloSecurityException, AccumuloException,
      TableNamespaceNotFoundException;
  
  /**
   * Add an iterator to a table namespace on the given scopes.
   * 
   * @param tableNamespace
   *          the name of the table namespace
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the table namespace
   * @throws AccumuloException
   * @throws TableNamespaceNotFoundException
   *           throw if the table namespace no longer exists
   * @throws IllegalArgumentException
   *           if the setting conflicts with any existing iterators
   */
  public void attachIterator(String tableNamespace, IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloSecurityException,
      AccumuloException, TableNamespaceNotFoundException;
  
  /**
   * Remove an iterator from a table namespace by name.
   * 
   * @param tableNamespace
   *          the name of the table namespace
   * @param name
   *          the name of the iterator
   * @param scopes
   *          the scopes of the iterator
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the table namespace
   * @throws AccumuloException
   * @throws TableNamespaceNotFoundException
   *           thrown if the table namespace no longer exists
   */
  public void removeIterator(String tableNamespace, String name, EnumSet<IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException,
      TableNamespaceNotFoundException;
  
  /**
   * Get the settings for an iterator.
   * 
   * @param tableNamespace
   *          the name of the table namespace
   * @param name
   *          the name of the iterator
   * @param scope
   *          the scope of the iterator
   * @return the settings for this iterator
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the table namespace
   * @throws AccumuloException
   * @throws TableNamespaceNotFoundException
   *           thrown if the table namespace no longer exists
   */
  public IteratorSetting getIteratorSetting(String tableNamespace, String name, IteratorScope scope) throws AccumuloSecurityException, AccumuloException,
      NumberFormatException, TableNamespaceNotFoundException;
  
  /**
   * Get a list of iterators for this table namespace.
   * 
   * @param tableNamespace
   *          the name of the table namespace
   * @return a set of iterator names
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the table namespace
   * @throws AccumuloException
   * @throws TableNamespaceNotFoundException
   *           thrown if the table namespace no longer exists
   */
  public Map<String,EnumSet<IteratorScope>> listIterators(String tableNamespace) throws AccumuloSecurityException, AccumuloException,
      TableNamespaceNotFoundException;
  
  /**
   * Check whether a given iterator configuration conflicts with existing configuration; in particular, determine if the name or priority are already in use for
   * the specified scopes.
   * 
   * @param tableNamespace
   *          the name of the table namespace
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloException
   * @throws TableNamespaceNotFoundException
   *           thrown if the table namespace no longer exists
   * @throws IllegalStateException
   *           if the setting conflicts with any existing iterators
   */
  public void checkIteratorConflicts(String tableNamespace, IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloException,
      TableNamespaceNotFoundException;
  
  /**
   * Add a new constraint to a table namespace.
   * 
   * @param tableNamespace
   *          the name of the table namespace
   * @param constraintClassName
   *          the full name of the constraint class
   * @return the unique number assigned to the constraint
   * @throws AccumuloException
   *           thrown if the constraint has already been added to the table or if there are errors in the configuration of existing constraints
   * @throws AccumuloSecurityException
   *           thrown if the user doesn't have permission to add the constraint
   * @throws TableNamespaceNotFoundException
   *           thrown if the table namespace no longer exists
   */
  public int addConstraint(String tableNamespace, String constraintClassName) throws AccumuloException, AccumuloSecurityException,
      TableNamespaceNotFoundException;
  
  /**
   * Remove a constraint from a table namespace.
   * 
   * @param tableNamespace
   *          the name of the table namespace
   * @param number
   *          the unique number assigned to the constraint
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   *           thrown if the user doesn't have permission to remove the constraint
   */
  public void removeConstraint(String tableNamespace, int number) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * List constraints on a table namespace with their assigned numbers.
   * 
   * @param tableNamespace
   *          the name of the table namespace
   * @return a map from constraint class name to assigned number
   * @throws AccumuloException
   *           thrown if there are errors in the configuration of existing constraints
   * @throws TableNamespaceNotFoundException
   *           thrown if the table namespace no longer exists
   */
  public Map<String,Integer> listConstraints(String tableNamespace) throws AccumuloException, TableNamespaceNotFoundException;

  
  /**
   * Test to see if the instance can load the given class as the given type. This check uses the table classpath property if it is set.
   * 
   * @return true if the instance can load the given class as the given type, false otherwise
   */
  boolean testClassLoad(String namespace, String className, String asTypeName) throws TableNamespaceNotFoundException, AccumuloException,
      AccumuloSecurityException;
}
