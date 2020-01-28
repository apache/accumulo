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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * Provides a class for administering tables
 *
 */

public interface TableOperations {

  /**
   * Retrieve a list of tables in Accumulo.
   *
   * @return List of tables in accumulo
   */
  SortedSet<String> list();

  /**
   * A method to check if a table exists in Accumulo.
   *
   * @param tableName
   *          the name of the table
   * @return true if the table exists
   */
  boolean exists(String tableName);

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
  void create(String tableName)
      throws AccumuloException, AccumuloSecurityException, TableExistsException;

  /**
   * @param tableName
   *          the name of the table
   * @param limitVersion
   *          Enables/disables the versioning iterator, which will limit the number of Key versions
   *          kept.
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableExistsException
   *           if the table already exists
   * @deprecated since 1.7.0; use {@link #create(String, NewTableConfiguration)} instead.
   */
  @Deprecated
  void create(String tableName, boolean limitVersion)
      throws AccumuloException, AccumuloSecurityException, TableExistsException;

  /**
   * @param tableName
   *          the name of the table
   * @param versioningIter
   *          Enables/disables the versioning iterator, which will limit the number of Key versions
   *          kept.
   * @param timeType
   *          specifies logical or real-time based time recording for entries in the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableExistsException
   *           if the table already exists
   * @deprecated since 1.7.0; use {@link #create(String, NewTableConfiguration)} instead.
   */
  @Deprecated
  void create(String tableName, boolean versioningIter, TimeType timeType)
      throws AccumuloException, AccumuloSecurityException, TableExistsException;

  /**
   * @param tableName
   *          the name of the table
   * @param ntc
   *          specifies the new table's configuration variable, which are: 1. enable/disable the
   *          versioning iterator, which will limit the number of Key versions kept; 2. specifies
   *          logical or real-time based time recording for entries in the table; 3. user defined
   *          properties to be merged into the initial properties of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableExistsException
   *           if the table already exists
   * @since 1.7.0
   */
  void create(String tableName, NewTableConfiguration ntc)
      throws AccumuloSecurityException, AccumuloException, TableExistsException;

  /**
   * Imports a table exported via exportTable and copied via hadoop distcp.
   *
   * @param tableName
   *          Name of a table to create and import into.
   * @param importDir
   *          Directory that contains the files copied by distcp from exportTable
   * @since 1.5.0
   */
  void importTable(String tableName, String importDir)
      throws TableExistsException, AccumuloException, AccumuloSecurityException;

  /**
   * Exports a table. The tables data is not exported, just table metadata and a list of files to
   * distcp. The table being exported must be offline and stay offline for the duration of distcp.
   * To avoid losing access to a table it can be cloned and the clone taken offline for export.
   *
   * <p>
   * See docs/examples/README.export
   *
   * @param tableName
   *          Name of the table to export.
   * @param exportDir
   *          An empty directory in HDFS where files containing table metadata and list of files to
   *          distcp will be placed.
   * @since 1.5.0
   */
  void exportTable(String tableName, String exportDir)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException;

  /**
   * Ensures that tablets are split along a set of keys.
   * <p>
   * Note that while the documentation for Text specifies that its bytestream should be UTF-8, the
   * encoding is not enforced by operations that work with byte arrays.
   * <p>
   * For example, you can create 256 evenly-sliced splits via the following code sample even though
   * the given byte sequences are not valid UTF-8.
   *
   * <pre>
   * TableOperations tableOps = connector.tableOperations();
   * TreeSet&lt;Text&gt; splits = new TreeSet&lt;Text&gt;();
   * for (int i = 0; i &lt; 256; i++) {
   *   byte[] bytes = {(byte) i};
   *   splits.add(new Text(bytes));
   * }
   * tableOps.addSplits(TABLE_NAME, splits);
   * </pre>
   *
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
  void addSplits(String tableName, SortedSet<Text> partitionKeys)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException;

  /**
   * @param tableName
   *          the name of the table
   * @return the split points (end-row names) for the table's current split profile
   * @throws TableNotFoundException
   *           if the table does not exist
   * @deprecated since 1.5.0; use {@link #listSplits(String)} instead.
   */
  @Deprecated
  Collection<Text> getSplits(String tableName) throws TableNotFoundException;

  /**
   * @param tableName
   *          the name of the table
   * @return the split points (end-row names) for the table's current split profile
   * @throws TableNotFoundException
   *           if the table does not exist
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @since 1.5.0
   */
  Collection<Text> listSplits(String tableName)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException;

  /**
   * @param tableName
   *          the name of the table
   * @param maxSplits
   *          specifies the maximum number of splits to return
   * @return the split points (end-row names) for the table's current split profile, grouped into
   *         fewer splits so as not to exceed maxSplits
   * @deprecated since 1.5.0; use {@link #listSplits(String, int)} instead.
   */
  @Deprecated
  Collection<Text> getSplits(String tableName, int maxSplits) throws TableNotFoundException;

  /**
   * @param tableName
   *          the name of the table
   * @param maxSplits
   *          specifies the maximum number of splits to return
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @return the split points (end-row names) for the table's current split profile, grouped into
   *         fewer splits so as not to exceed maxSplits
   * @since 1.5.0
   */
  Collection<Text> listSplits(String tableName, int maxSplits)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException;

  /**
   * Locates the tablet servers and tablets that would service a collections of ranges. If a range
   * covers multiple tablets, it will occur multiple times in the returned map.
   *
   * @param ranges
   *          The input ranges that should be mapped to tablet servers and tablets.
   *
   * @throws TableOfflineException
   *           if the table is offline or goes offline during the operation
   * @since 1.8.0
   */
  Locations locate(String tableName, Collection<Range> ranges)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * Finds the max row within a given range. To find the max row in a table, pass null for start and
   * end row.
   *
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
   */
  Text getMaxRow(String tableName, Authorizations auths, Text startRow, boolean startInclusive,
      Text endRow, boolean endInclusive)
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
  void merge(String tableName, Text start, Text end)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

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
  void deleteRows(String tableName, Text start, Text end)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * Starts a full major compaction of the tablets in the range (start, end]. The compaction is
   * preformed even for tablets that have only one file.
   *
   * @param tableName
   *          the table to compact
   * @param start
   *          first tablet to be compacted contains the row after this row, null means the first
   *          tablet in table
   * @param end
   *          last tablet to be compacted contains this row, null means the last tablet in table
   * @param flush
   *          when true, table memory is flushed before compaction starts
   * @param wait
   *          when true, the call will not return until compactions are finished
   */
  void compact(String tableName, Text start, Text end, boolean flush, boolean wait)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException;

  /**
   * Starts a full major compaction of the tablets in the range (start, end]. The compaction is
   * preformed even for tablets that have only one file.
   *
   * @param tableName
   *          the table to compact
   * @param start
   *          first tablet to be compacted contains the row after this row, null means the first
   *          tablet in table
   * @param end
   *          last tablet to be compacted contains this row, null means the last tablet in table
   * @param iterators
   *          A set of iterators that will be applied to each tablet compacted. If two or more
   *          concurrent calls to compact pass iterators, then only one will succeed and the others
   *          will fail.
   * @param flush
   *          when true, table memory is flushed before compaction starts
   * @param wait
   *          when true, the call will not return until compactions are finished
   * @since 1.5.0
   */
  void compact(String tableName, Text start, Text end, List<IteratorSetting> iterators,
      boolean flush, boolean wait)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException;

  /**
   * Starts a full major compaction of the tablets in the range (start, end]. If the config does not
   * specify a compaction strategy, then all files in a tablet are compacted. The compaction is
   * performed even for tablets that have only one file.
   *
   * <p>
   * Only one compact call at a time can pass iterators and/or a compaction strategy. If two threads
   * call compaction with iterators and/or a copmaction strategy, then one will fail.
   *
   * @param tableName
   *          the table to compact
   * @param config
   *          the configuration to use
   *
   * @since 1.7.0
   */
  void compact(String tableName, CompactionConfig config)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException;

  /**
   * Cancels a user initiated major compaction of a table initiated with
   * {@link #compact(String, Text, Text, boolean, boolean)} or
   * {@link #compact(String, Text, Text, List, boolean, boolean)}. Compactions of tablets that are
   * currently running may finish, but new compactions of tablets will not start.
   *
   * @param tableName
   *          the name of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws TableNotFoundException
   *           if the table does not exist
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @since 1.5.0
   */
  void cancelCompaction(String tableName)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException;

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
  void delete(String tableName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * Clone a table from an existing table. The cloned table will have the same data as the source
   * table it was created from. After cloning, the two tables can mutate independently. Initially
   * the cloned table should not use any extra space, however as the source table and cloned table
   * major compact extra space will be used by the clone.
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

  void clone(String srcTableName, String newTableName, boolean flush,
      Map<String,String> propertiesToSet, Set<String> propertiesToExclude) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException, TableExistsException;

  /**
   * Clone a table from an existing table. The cloned table will have the same data as the source
   * table it was created from. After cloning, the two tables can mutate independently. Initially
   * the cloned table should not use any extra space, however as the source table and cloned table
   * major compact extra space will be used by the clone.
   *
   * Initially the cloned table is only readable and writable by the user who created it.
   *
   * @param srcTableName
   *          the table to clone
   * @param newTableName
   *          the name of the clone
   * @param config
   *          the clone command configuration
   * @since 1.10 and 2.1
   */
  void clone(String srcTableName, String newTableName, CloneConfiguration config)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TableExistsException;

  /**
   * Rename a table
   *
   * @param oldTableName
   *          the old table name
   * @param newTableName
   *          the new table name, which must be in the same namespace as the oldTableName
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the old table name does not exist
   * @throws TableExistsException
   *           if the new table name already exists
   */
  void rename(String oldTableName, String newTableName) throws AccumuloSecurityException,
      TableNotFoundException, AccumuloException, TableExistsException;

  /**
   * Initiate a flush of a table's data that is in memory
   *
   * @param tableName
   *          the name of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   *
   * @deprecated since 1.4; use {@link #flush(String, Text, Text, boolean)} instead
   */
  @Deprecated
  void flush(String tableName) throws AccumuloException, AccumuloSecurityException;

  /**
   * Flush a table's data that is currently in memory.
   *
   * @param tableName
   *          the name of the table
   * @param wait
   *          if true the call will not return until all data present in memory when the call was is
   *          flushed if false will initiate a flush of data in memory, but will not wait for it to
   *          complete
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  void flush(String tableName, Text start, Text end, boolean wait)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * Sets a property on a table. This operation is asynchronous and eventually consistent. Not all
   * tablets in a table will acknowledge this new value immediately nor at the same time. Within a
   * few seconds without another change, all tablets in a table should see the updated value. The
   * clone table feature can be used if consistency is required.
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
  void setProperty(String tableName, String property, String value)
      throws AccumuloException, AccumuloSecurityException;

  /**
   * Removes a property from a table. This operation is asynchronous and eventually consistent. Not
   * all tablets in a table will acknowledge this altered value immediately nor at the same time.
   * Within a few seconds without another change, all tablets in a table should see the altered
   * value. The clone table feature can be used if consistency is required.
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
  void removeProperty(String tableName, String property)
      throws AccumuloException, AccumuloSecurityException;

  /**
   * Gets properties of a table. This operation is asynchronous and eventually consistent. It is not
   * guaranteed that all tablets in a table will return the same values. Within a few seconds
   * without another change, all tablets in a table should be consistent. The clone table feature
   * can be used if consistency is required.
   *
   * @param tableName
   *          the name of the table
   * @return all properties visible by this table (system and per-table properties). Note that
   *         recently changed properties may not be visible immediately.
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  Iterable<Entry<String,String>> getProperties(String tableName)
      throws AccumuloException, TableNotFoundException;

  /**
   * Sets a table's locality groups. A table's locality groups can be changed at any time.
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
  void setLocalityGroups(String tableName, Map<String,Set<Text>> groups)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

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
  Map<String,Set<Text>> getLocalityGroups(String tableName)
      throws AccumuloException, TableNotFoundException;

  /**
   * @param tableName
   *          the name of the table
   * @param range
   *          a range to split
   * @param maxSplits
   *          the maximum number of splits
   * @return the range, split into smaller ranges that fall on boundaries of the table's split
   *         points as evenly as possible
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * Bulk import all the files in a directory into a table. Files can be created using
   * {@link AccumuloFileOutputFormat} and {@link RFile#newWriter()}
   *
   * @param tableName
   *          the name of the table
   * @param dir
   *          the HDFS directory to find files for importing
   * @param failureDir
   *          the HDFS directory to place files that failed to be imported, must exist and be empty
   * @param setTime
   *          override the time values in the input files, and use the current time for all
   *          mutations
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
  void importDirectory(String tableName, String dir, String failureDir, boolean setTime)
      throws TableNotFoundException, IOException, AccumuloException, AccumuloSecurityException;

  /**
   * Initiates taking a table offline, but does not wait for action to complete
   *
   * @param tableName
   *          the table to take offline
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   */
  void offline(String tableName)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

  /**
   *
   * @param tableName
   *          the table to take offline
   * @param wait
   *          if true, then will not return until table is offline
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @since 1.6.0
   */
  void offline(String tableName, boolean wait)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

  /**
   * Initiates bringing a table online, but does not wait for action to complete
   *
   * @param tableName
   *          the table to take online
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   */
  void online(String tableName)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

  /**
   *
   * @param tableName
   *          the table to take online
   * @param wait
   *          if true, then will not return until table is online
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @since 1.6.0
   */
  void online(String tableName, boolean wait)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

  /**
   * Clears the tablet locator cache for a specified table
   *
   * @param tableName
   *          the name of the table
   * @throws TableNotFoundException
   *           if table does not exist
   */
  void clearLocatorCache(String tableName) throws TableNotFoundException;

  /**
   * Get a mapping of table name to internal table id.
   *
   * @return the map from table name to internal table id
   */
  Map<String,String> tableIdMap();

  /**
   * Add an iterator to a table on all scopes.
   *
   * @param tableName
   *          the name of the table
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the table
   * @throws TableNotFoundException
   *           throw if the table no longer exists
   * @throws IllegalArgumentException
   *           if the setting conflicts with any existing iterators
   */
  void attachIterator(String tableName, IteratorSetting setting)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

  /**
   * Add an iterator to a table on the given scopes.
   *
   * @param tableName
   *          the name of the table
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloSecurityException
   *           thrown if the user does not have the ability to set properties on the table
   * @throws TableNotFoundException
   *           throw if the table no longer exists
   * @throws IllegalArgumentException
   *           if the setting conflicts with any existing iterators
   */
  void attachIterator(String tableName, IteratorSetting setting, EnumSet<IteratorScope> scopes)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

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
   * @throws TableNotFoundException
   *           throw if the table no longer exists
   */
  void removeIterator(String tableName, String name, EnumSet<IteratorScope> scopes)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

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
   * @throws TableNotFoundException
   *           throw if the table no longer exists
   */
  IteratorSetting getIteratorSetting(String tableName, String name, IteratorScope scope)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

  /**
   * Get a list of iterators for this table.
   *
   * @param tableName
   *          the name of the table
   * @return a set of iterator names
   */
  Map<String,EnumSet<IteratorScope>> listIterators(String tableName)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

  /**
   * Check whether a given iterator configuration conflicts with existing configuration; in
   * particular, determine if the name or priority are already in use for the specified scopes.
   *
   * @param tableName
   *          the name of the table
   * @param setting
   *          object specifying the properties of the iterator
   */
  void checkIteratorConflicts(String tableName, IteratorSetting setting,
      EnumSet<IteratorScope> scopes) throws AccumuloException, TableNotFoundException;

  /**
   * Add a new constraint to a table.
   *
   * @param tableName
   *          the name of the table
   * @param constraintClassName
   *          the full name of the constraint class
   * @return the unique number assigned to the constraint
   * @throws AccumuloException
   *           thrown if the constraint has already been added to the table or if there are errors
   *           in the configuration of existing constraints
   * @throws AccumuloSecurityException
   *           thrown if the user doesn't have permission to add the constraint
   * @since 1.5.0
   */
  int addConstraint(String tableName, String constraintClassName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * Remove a constraint from a table.
   *
   * @param tableName
   *          the name of the table
   * @param number
   *          the unique number assigned to the constraint
   * @throws AccumuloSecurityException
   *           thrown if the user doesn't have permission to remove the constraint
   * @since 1.5.0
   */
  void removeConstraint(String tableName, int number)
      throws AccumuloException, AccumuloSecurityException;

  /**
   * List constraints on a table with their assigned numbers.
   *
   * @param tableName
   *          the name of the table
   * @return a map from constraint class name to assigned number
   * @throws AccumuloException
   *           thrown if there are errors in the configuration of existing constraints
   * @since 1.5.0
   */
  Map<String,Integer> listConstraints(String tableName)
      throws AccumuloException, TableNotFoundException;

  /**
   * Gets the number of bytes being used in the files for a set of tables
   *
   * @param tables
   *          a set of tables
   * @return a list of disk usage objects containing linked table names and sizes
   * @since 1.6.0
   */
  List<DiskUsage> getDiskUsage(Set<String> tables)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * Test to see if the instance can load the given class as the given type. This check uses the
   * table classpath if it is set.
   *
   * @return true if the instance can load the given class as the given type, false otherwise
   *
   * @since 1.5.0
   */
  boolean testClassLoad(String tableName, final String className, final String asTypeName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * Set or update the sampler configuration for a table. If the table has existing sampler
   * configuration, those properties will be cleared before setting the new table properties.
   *
   * @param tableName
   *          the name of the table
   * @since 1.8.0
   */
  void setSamplerConfiguration(String tableName, SamplerConfiguration samplerConfiguration)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException;

  /**
   * Clear all sampling configuration properties on the table.
   *
   * @param tableName
   *          the name of the table
   * @since 1.8.0
   */
  void clearSamplerConfiguration(String tableName)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException;

  /**
   * Reads the sampling configuration properties for a table.
   *
   * @param tableName
   *          the name of the table
   * @since 1.8.0
   */
  SamplerConfiguration getSamplerConfiguration(String tableName)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException;
}
