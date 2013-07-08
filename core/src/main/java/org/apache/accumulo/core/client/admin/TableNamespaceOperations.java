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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNamespaceExistsException;
import org.apache.accumulo.core.client.TableNamespaceNotEmptyException;
import org.apache.accumulo.core.client.TableNamespaceNotFoundException;

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
  public void create(String namespace, boolean versioningIter, TimeType timeType) throws AccumuloException, AccumuloSecurityException, TableNamespaceExistsException;
  
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
   */
  public void delete(String namespace) throws AccumuloException, AccumuloSecurityException, TableNamespaceNotFoundException, TableNamespaceNotEmptyException;
  
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
   */
  public void delete(String namespace, boolean deleteTables) throws AccumuloException, AccumuloSecurityException, TableNamespaceNotFoundException, TableNamespaceNotEmptyException;
  
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
   * Removes a property from a table namespace.  Note that it may take a short period of time (a second) to propagate the change everywhere.
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
   * Gets properties of a table namespace.  Note that recently changed properties may not be available immediately.
   * 
   * @param namespace
   *          the name of the table namespace
   * @return all properties visible by this table (system and per-table properties).  Note that recently changed 
   *         properties may not be visible immediately. 
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
   * @return  a list of disk usage objects containing linked table names and sizes
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   */
  public List<DiskUsage> getDiskUsage(String namespace) throws AccumuloException, AccumuloSecurityException, TableNamespaceNotFoundException;
}
