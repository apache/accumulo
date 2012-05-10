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
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * Provides a class for administering tables
 * 
 */

public interface TableOperations {
  
  public SortedSet<String> list();
  
  /**
   * A method to check if a table exists in Accumulo.
   * 
   * @param tableName
   *          the name of the table
   * @return true if the table exists
   */
  public boolean exists(String tableName);
  
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
  public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException;
  
  /**
   * @param tableName
   *          the name of the table
   * @param limitVersion
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableExistsException
   *           if the table already exists
   */
  public void create(String tableName, boolean limitVersion) throws AccumuloException, AccumuloSecurityException, TableExistsException;
  
  /**
   * @param tableName
   *          the name of the table
   * @param versioningIter
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   * @param timeType
   *          specifies logical or real-time based time recording for entries in the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableExistsException
   *           if the table already exists
   */
  public void create(String tableName, boolean versioningIter, TimeType timeType) throws AccumuloException, AccumuloSecurityException, TableExistsException;
  
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
  public void addSplits(String tableName, SortedSet<Text> partitionKeys) throws TableNotFoundException, AccumuloException, AccumuloSecurityException;
  
  /**
   * @param tableName
   *          the name of the table
   * @return the split points (end-row names) for the table's current split profile
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public Collection<Text> getSplits(String tableName) throws TableNotFoundException;
  
  /**
   * @param tableName
   *          the name of the table
   * @param maxSplits
   *          specifies the maximum number of splits to return
   * @return the split points (end-row names) for the table's current split profile, grouped into fewer splits so as not to exceed maxSplits
   * @throws TableNotFoundException
   */
  public Collection<Text> getSplits(String tableName, int maxSplits) throws TableNotFoundException;
  
  /**
   * Finds the max row within a given range. To find the max row in a table, pass null for start and end row.
   * 
   * @param tableName
   * @param auths
   *          find the max row that can seen with these auths
   * @param startRow
   *          row to start looking at, null means -Infinity
   * @param startInclusive
   *          determines if the start row is included
   * @param endRow
   *          row to stop looking at, null means Infinity
   * @param endInclusive
   *          determines if the end row is included
   * 
   * @return The max row in the range, or null if there is no visible data in the range.
   * 
   * @throws AccumuloSecurityException
   * @throws AccumuloException
   * @throws TableNotFoundException
   */
  public Text getMaxRow(String tableName, Authorizations auths, Text startRow, boolean startInclusive, Text endRow, boolean endInclusive)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException;
  
  /**
   * Merge tablets between (start, end]
   * 
   * @param tableName
   *          the table to merge
   * @param start
   *          first tablet to be merged contains the row after this row, null means the first tablet
   * @param end
   *          last tablet to be merged contains this row, null means the last tablet
   */
  public void merge(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
  
  /**
   * Delete rows between (start, end]
   * 
   * @param tableName
   *          the table to merge
   * @param start
   *          delete rows after this, null means the first row of the table
   * @param end
   *          last row to be deleted, inclusive, null means the last row of the table
   */
  public void deleteRows(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
  
  /**
   * Starts a full major compaction of the tablets in the range (start, end]. The compaction is preformed even for tablets that have only one file.
   * 
   * @param tableName
   *          the table to compact
   * @param start
   *          first tablet to be compacted contains the row after this row, null means the first tablet in table
   * @param end
   *          last tablet to be merged contains this row, null means the last tablet in table
   * @param flush
   *          when true, table memory is flushed before compaction starts
   * @param wait
   *          when true, the call will not return until compactions are finished
   */
  public void compact(String tableName, Text start, Text end, boolean flush, boolean wait) throws AccumuloSecurityException, TableNotFoundException,
      AccumuloException;
  
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
  public void delete(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
  
  /**
   * Clone a table from an existing table. The cloned table will have the same data as the source table it was created from. After cloning, the two tables can
   * mutate independently. Initially the cloned table should not use any extra space, however as the source table and cloned table major compact extra space
   * will be used by the clone.
   * 
   * Initially the cloned table is only readable and writable by the user who created it.
   * 
   * @param srcTableName
   *          the table to clone
   * @param newTableName
   *          the name of the clone
   * @param flush
   *          determines if memory is flushed in the source table before cloning.
   * @param propertiesToSet
   *          the sources tables properties are copied, this allows overriding of those properties
   * @param propertiesToExclude
   *          do not copy these properties from the source table, just revert to system defaults
   */
  
  public void clone(String srcTableName, String newTableName, boolean flush, Map<String,String> propertiesToSet, Set<String> propertiesToExclude)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException;
  
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
      TableExistsException;
  
  /**
   * Initiate a flush of a tables data that is in memory
   * 
   * @param tableName
   *          the name of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * 
   * @deprecated As of release 1.4, replaced by {@link #flush(String, Text, Text, boolean)}
   */
  public void flush(String tableName) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Flush a tables data that is currently in memory.
   * 
   * @param tableName
   *          the name of the table
   * @param wait
   *          if true the call will not return until all data present in memory when the call was is flushed if false will initiate a flush of data in memory,
   *          but will not wait for it to complete
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   */
  public void flush(String tableName, Text start, Text end, boolean wait) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
  
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
  public void setProperty(String tableName, String property, String value) throws AccumuloException, AccumuloSecurityException;
  
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
  public void removeProperty(String tableName, String property) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Gets properties of a table
   * 
   * @param tableName
   *          the name of the table
   * @return all properties visible by this table (system and per-table properties)
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public Iterable<Entry<String,String>> getProperties(String tableName) throws AccumuloException, TableNotFoundException;
  
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
  public void setLocalityGroups(String tableName, Map<String,Set<Text>> groups) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
  
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
  public Map<String,Set<Text>> getLocalityGroups(String tableName) throws AccumuloException, TableNotFoundException;
  
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
      TableNotFoundException;
  
  /**
   * Bulk import all the files in a directory into a table.
   * 
   * @param tableName
   *          the name of the table
   * @param dir
   *          the HDFS directory to find files for importing
   * @param failureDir
   *          the HDFS directory to place files that failed to be imported, must exist and be empty
   * @throws IOException
   *           when there is an error reading/writing to HDFS
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws TableNotFoundException
   *           when the table no longer exists
   * 
   */
  public void importDirectory(String tableName, String dir, String failureDir, boolean setTime) throws TableNotFoundException, IOException, AccumuloException,
      AccumuloSecurityException;
  
  /**
   * 
   * @param tableName
   *          the table to take offline
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws TableNotFoundException
   */
  public void offline(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException;
  
  /**
   * 
   * @param tableName
   *          the table to take online
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws TableNotFoundException
   */
  public void online(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException;
  
  /**
   * Clears the tablet locator cache for a specified table
   * 
   * @param tableName
   *          the name of the table
   * @throws TableNotFoundException
   *           if table does not exist
   */
  public void clearLocatorCache(String tableName) throws TableNotFoundException;
  
  /**
   * Get a mapping of table name to internal table id.
   * 
   * @return the map from table name to internal table id
   */
  public Map<String,String> tableIdMap();
  
  /**
   * Add an iterator to a table on all scopes.
   * 
   * @param tableName
   *          the name of the table
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the table
   * @throws AccumuloException
   * @throws TableNotFoundException
   *           throw if the table no longer exists
   * @throws IllegalArgumentException
   *           if the setting conflicts with any existing iterators
   */
  public void attachIterator(String tableName, IteratorSetting setting) throws AccumuloSecurityException, AccumuloException, TableNotFoundException;
  
  /**
   * Add an iterator to a table on the given scopes.
   * 
   * @param tableName
   *          the name of the table
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the table
   * @throws AccumuloException
   * @throws TableNotFoundException
   *           throw if the table no longer exists
   * @throws IllegalArgumentException
   *           if the setting conflicts with any existing iterators
   */
  public void attachIterator(String tableName, IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException,
      TableNotFoundException;
  
  /**
   * Remove an iterator from a table by name.
   * 
   * @param tableName
   *          the name of the table
   * @param name
   *          the name of the iterator
   * @param scopes
   *          the scopes of the iterator
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the table
   * @throws AccumuloException
   * @throws TableNotFoundException
   *           throw if the table no longer exists
   */
  public void removeIterator(String tableName, String name, EnumSet<IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException,
      TableNotFoundException;
  
  /**
   * Get the settings for an iterator.
   * 
   * @param tableName
   *          the name of the table
   * @param name
   *          the name of the iterator
   * @param scope
   *          the scope of the iterator
   * @return the settings for this iterator
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the table
   * @throws AccumuloException
   * @throws TableNotFoundException
   *           throw if the table no longer exists
   */
  public IteratorSetting getIteratorSetting(String tableName, String name, IteratorScope scope) throws AccumuloSecurityException, AccumuloException,
      TableNotFoundException;
  
  /**
   * Get a list of iterators for this table.
   * 
   * @param tableName
   *          the name of the table
   * @return a set of iterator names
   * @throws AccumuloSecurityException
   * @throws AccumuloException
   * @throws TableNotFoundException
   */
  public Map<String,EnumSet<IteratorScope>> listIterators(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException;
  
  /**
   * Check whether a given iterator configuration conflicts with existing configuration; in particular, determine if the name or priority are already in use for
   * the specified scopes.
   * 
   * @param tableName
   *          the name of the table
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloException
   * @throws TableNotFoundException
   * @throws IllegalStateException
   *           if the setting conflicts with any existing iterators
   */
  public void checkIteratorConflicts(String tableName, IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloException, TableNotFoundException;
  
  /**
   * Add a new constraint to a table.
   * 
   * @param tableName
   *          the name of the table
   * @param constraintClassName
   *          the full name of the constraint class
   * @return the unique number assigned to the constraint
   * @throws AccumuloException
   *           thrown if the constraint has already been added to the table or if there are errors in the configuration of existing constraints
   * @throws AccumuloSecurityException
   *           thrown if the user doesn't have permission to add the constraint
   * @throws TableNotFoundException
   */
  public int addConstraint(String tableName, String constraintClassName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
  
  /**
   * Remove a constraint from a table.
   * 
   * @param tableName
   *          the name of the table
   * @param number
   *          the unique number assigned to the constraint
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   *           thrown if the user doesn't have permission to remove the constraint
   */
  public void removeConstraint(String tableName, int number) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * List constraints on a table with their assigned numbers.
   * 
   * @param tableName
   *          the name of the table
   * @return a map from constraint class name to assigned number
   * @throws AccumuloException
   *           thrown if there are errors in the configuration of existing constraints
   * @throws TableNotFoundException
   */
  public Map<String,Integer> listConstraints(String tableName) throws AccumuloException, TableNotFoundException;
}
