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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.aggregation.conf.AggregatorConfiguration;
import org.apache.accumulo.core.util.BulkImportHelper.AssignmentStats;
import org.apache.hadoop.io.Text;

/**
 * Provides a class for administering tables
 * 
 */
public class TableOperations {
  final private TableOperations impl;
  
  protected TableOperations() {
    impl = null;
  }
  
  /**
   * Retrieve a list of tables in Accumulo.
   * 
   * @return List of tables in accumulo
   */
  public SortedSet<String> list() {
    return impl.list();
  }
  
  /**
   * A method to check if a table exists in Accumulo.
   * 
   * @param tableName
   *          the name of the table
   * @return true if the table exists
   */
  public boolean exists(String tableName) {
    return impl.exists(tableName);
  }
  
  /**
   * Create a table with no special configuration
   * 
   * @param tableName
   *          the name of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableExistsException
   *           if the table already exists
   */
  public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    impl.create(tableName);
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param partitionKeys
   *          a sorted set of row key values to pre-split the table on
   * @param aggregators
   *          a list of configured aggregators to apply to the table immediately
   * @param timeType
   *          specifies logical or real-time based time recording for entries in the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableExistsException
   *           if the table already exists
   */
  public void create(String tableName, TimeType timeType) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    impl.create(tableName, timeType);
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param aggregators
   *          List of aggregators to add
   * @throws AccumuloSecurityException
   *           if insufficient permissions to do action
   * @throws TableNotFoundException
   *           if table does not exist
   * @throws AccumuloException
   *           if a general error occurs
   */
  public void addAggregators(String tableName, List<AggregatorConfiguration> aggregators) throws AccumuloSecurityException, TableNotFoundException,
      AccumuloException {}
  
  /**
   * @param tableName
   *          the name of the table
   * @param partitionKeys
   *          a sorted set of row key values to pre-split the table on
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public void addSplits(String tableName, SortedSet<Text> partitionKeys) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    impl.addSplits(tableName, partitionKeys);
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @return the split points (end-row names) for the table's current split profile
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public Collection<Text> getSplits(String tableName) throws TableNotFoundException {
    return impl.getSplits(tableName);
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param maxSplits
   *          specifies the maximum number of splits to return
   * @return the split points (end-row names) for the table's current split profile, grouped into fewer splits so as not to exceed maxSplits
   * @throws TableNotFoundException
   */
  public Collection<Text> getSplits(String tableName, int maxSplits) throws TableNotFoundException {
    return impl.getSplits(tableName, maxSplits);
  }
  
  /**
   * Delete a table
   * 
   * @param tableName
   *          the name of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public void delete(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    impl.delete(tableName);
  }
  
  /**
   * Rename a table
   * 
   * @param oldTableName
   *          the old table name
   * @param newTableName
   *          the new table name
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the old table name does not exist
   * @throws TableExistsException
   *           if the new table name already exists
   */
  public void rename(String oldTableName, String newTableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
      TableExistsException {
    impl.rename(oldTableName, newTableName);
  }
  
  /**
   * Flush a table
   * 
   * @param tableName
   *          the name of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  public void flush(String tableName) throws AccumuloException, AccumuloSecurityException {
    impl.flush(tableName);
  }
  
  /**
   * Sets a property on a table
   * 
   * @param tableName
   *          the name of the table
   * @param property
   *          the name of a per-table property
   * @param value
   *          the value to set a per-table property to
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  public void setProperty(String tableName, String property, String value) throws AccumuloException, AccumuloSecurityException {
    impl.setProperty(tableName, property, value);
  }
  
  /**
   * Removes a property from a table
   * 
   * @param tableName
   *          the name of the table
   * @param property
   *          the name of a per-table property
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  public void removeProperty(String tableName, String property) throws AccumuloException, AccumuloSecurityException {
    impl.removeProperty(tableName, property);
  }
  
  /**
   * Gets properties of a table
   * 
   * @param tableName
   *          the name of the table
   * @return all properties visible by this table (system and per-table properties)
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public Iterable<Entry<String,String>> getProperties(String tableName) throws TableNotFoundException {
    return impl.getProperties(tableName);
  }
  
  /**
   * Sets a tables locality groups. A tables locality groups can be changed at any time.
   * 
   * @param tableName
   *          the name of the table
   * @param groups
   *          mapping of locality group names to column families in the locality group
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public void setLocalityGroups(String tableName, Map<String,Set<Text>> groups) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    impl.setLocalityGroups(tableName, groups);
  }
  
  /**
   * 
   * Gets the locality groups currently set for a table.
   * 
   * @param tableName
   *          the name of the table
   * @return mapping of locality group names to column families in the locality group
   * @throws AccumuloException
   *           if a general error occurs
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public Map<String,Set<Text>> getLocalityGroups(String tableName) throws AccumuloException, TableNotFoundException {
    return impl.getLocalityGroups(tableName);
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param range
   *          a range to split
   * @param maxSplits
   *          the maximum number of splits
   * @return the range, split into smaller ranges that fall on boundaries of the table's split points as evenly as possible
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    return impl.splitRangeByTablets(tableName, range, maxSplits);
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param dir
   *          the HDFS directory to find files for importing
   * @param failureDir
   *          the HDFS directory to place files that failed to be imported
   * @param numThreads
   *          the number of threads to use to process the files
   * @param numAssignThreads
   *          the number of threads to use when assigning the files
   * @param disableGC
   *          prevents the garbage collector from cleaning up files that were bulk imported
   * @return the statistics for the operation
   * @throws IOException
   *           when there is an error reading/writing to HDFS
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   */
  public AssignmentStats importDirectory(String tableName, String dir, String failureDir, int numThreads, int numAssignThreads, boolean disableGC)
      throws IOException, AccumuloException, AccumuloSecurityException {
    return impl.importDirectory(tableName, dir, failureDir, numThreads, numAssignThreads, disableGC);
  }
  
  /**
   * 
   * @param tableName
   *          the table to take offline
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   */
  public void offline(String tableName) throws AccumuloSecurityException, AccumuloException {
    impl.offline(tableName);
  }
  
  /**
   * 
   * @param tableName
   *          the table to take online
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   */
  public void online(String tableName) throws AccumuloSecurityException, AccumuloException {
    impl.online(tableName);
  }
  
  /**
   * Clears the tablet locator cache for a specified table
   * 
   * @param tableName
   *          the name of the table
   * @throws TableNotFoundException
   *           if table does not exist
   */
  public void clearLocatorCache(String tableName) throws TableNotFoundException {
    impl.clearLocatorCache(tableName);
  }
  
  /**
   * Get a mapping of table name to internal table id.
   * 
   * @return the map from table name to internal table id
   */
  public Map<String,String> tableIdMap() {
    return impl.tableIdMap();
  }
}
