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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.FindMax;
import org.apache.accumulo.core.client.admin.TableOperationsHelper;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.conf.PerColumnIteratorConfig;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.BulkImportHelper.AssignmentStats;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

@SuppressWarnings("deprecation")
public class MockTableOperations extends TableOperationsHelper {
  
  final private MockAccumulo acu;
  final private String username;
  
  MockTableOperations(MockAccumulo acu, String username) {
    this.acu = acu;
    this.username = username;
  }
  
  @Override
  public SortedSet<String> list() {
    return new TreeSet<String>(acu.tables.keySet());
  }
  
  @Override
  public boolean exists(String tableName) {
    return acu.tables.containsKey(tableName);
  }
  
  @Override
  public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    if (!tableName.matches(Constants.VALID_TABLE_NAME_REGEX)) {
      throw new IllegalArgumentException();
    }
    create(tableName, true, TimeType.MILLIS);
  }
  
  @Override
  public void create(String tableName, boolean versioningIter) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    create(tableName, versioningIter, TimeType.MILLIS);
  }
  
  @Override
  public void create(String tableName, boolean versioningIter, TimeType timeType) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    if (!tableName.matches(Constants.VALID_TABLE_NAME_REGEX)) {
      throw new IllegalArgumentException();
    }
    if (exists(tableName))
      throw new TableExistsException(tableName, tableName, "");
    acu.createTable(username, tableName, versioningIter, timeType);
  }
  
  /**
   * @deprecated since 1.4 {@link #attachIterator(String, IteratorSetting)}
   */
  @Override
  public void addAggregators(String tableName, List<? extends PerColumnIteratorConfig> aggregators) throws AccumuloSecurityException, TableNotFoundException,
      AccumuloException {
    if (!exists(tableName))
      throw new TableNotFoundException(tableName, tableName, "");
    acu.addAggregators(tableName, aggregators);
  }
  
  @Override
  public void addSplits(String tableName, SortedSet<Text> partitionKeys) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    throw new NotImplementedException();
  }
  
  @Override
  public Collection<Text> getSplits(String tableName) throws TableNotFoundException {
    if (!exists(tableName))
      throw new TableNotFoundException(tableName, tableName, "");
    return Collections.emptyList();
  }
  
  @Override
  public Collection<Text> getSplits(String tableName, int maxSplits) throws TableNotFoundException {
    return getSplits(tableName);
  }
  
  @Override
  public void delete(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    if (!exists(tableName))
      throw new TableNotFoundException(tableName, tableName, "");
    acu.tables.remove(tableName);
  }
  
  @Override
  public void rename(String oldTableName, String newTableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
      TableExistsException {
    if (!exists(oldTableName))
      throw new TableNotFoundException(oldTableName, oldTableName, "");
    if (exists(newTableName))
      throw new TableExistsException(newTableName, newTableName, "");
    MockTable t = acu.tables.remove(oldTableName);
    acu.tables.put(newTableName, t);
  }
  
  @Override
  public void flush(String tableName) throws AccumuloException, AccumuloSecurityException {}
  
  @Override
  public void setProperty(String tableName, String property, String value) throws AccumuloException, AccumuloSecurityException {
    acu.tables.get(tableName).settings.put(property, value);
  }
  
  @Override
  public void removeProperty(String tableName, String property) throws AccumuloException, AccumuloSecurityException {
    acu.tables.get(tableName).settings.remove(property);
  }
  
  @Override
  public Iterable<Entry<String,String>> getProperties(String tableName) throws TableNotFoundException {
    if (!exists(tableName))
      throw new TableNotFoundException(tableName, tableName, "");
    return acu.tables.get(tableName).settings.entrySet();
  }
  
  @Override
  public void setLocalityGroups(String tableName, Map<String,Set<Text>> groups) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    throw new NotImplementedException();
  }
  
  @Override
  public Map<String,Set<Text>> getLocalityGroups(String tableName) throws AccumuloException, TableNotFoundException {
    throw new NotImplementedException();
  }
  
  @Override
  public Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    return Collections.singleton(range);
  }
  
  @Override
  public AssignmentStats importDirectory(String tableName, String dir, String failureDir, int numThreads, int numAssignThreads, boolean disableGC)
      throws IOException, AccumuloException, AccumuloSecurityException {
    throw new NotImplementedException();
  }
  
  @Override
  public void importDirectory(String tableName, String dir, String failureDir, boolean setTime) throws IOException, AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    long time = System.currentTimeMillis();
    MockTable table = acu.tables.get(tableName);
    if (table == null) {
      throw new TableNotFoundException(null, tableName,
          "The table was not found");
    }
    Path importPath = new Path(dir);
    Path failurePath = new Path(failureDir);

    Configuration conf = new Configuration();
    FileSystem importFs = importPath.getFileSystem(conf);
    FileSystem failureFs = importPath.getFileSystem(conf);
    /*
     * check preconditions
     */
    // directories are directories
    if (importFs.isFile(importPath)) {
      throw new IOException("Import path must be a directory.");
    }
    if (failureFs.isFile(failurePath)) {
      throw new IOException("Failure path must be a directory.");
    }
    // failures are writable
    Path createPath = failurePath.suffix("/.createFile");
    FSDataOutputStream createStream = null;
    try {
      createStream = failureFs.create(createPath);
    } catch (IOException e) {
      throw new IOException("Error path is not writable.");
    } finally {
      if (createStream != null) {
        createStream.close();
      }
    }
    failureFs.delete(createPath, false);
    // failures are empty
    FileStatus[] failureChildStats = failureFs.listStatus(failurePath);
    if (failureChildStats.length > 0) {
      throw new IOException("Error path must be empty.");
    }
    /*
     * Begin the import - iterate the files in the path
     */
    for (FileStatus importStatus : importFs.listStatus(importPath)) {
      try {
        FileSKVIterator importIterator = FileOperations.getInstance()
            .openReader(importStatus.getPath().toString(), true, importFs,
                conf, AccumuloConfiguration.getDefaultConfiguration());
        while (importIterator.hasTop()) {
          Key key = importIterator.getTopKey();
          Value value = importIterator.getTopValue();
          if (setTime) {
            key.setTimestamp(time);
          }
          Mutation mutation = new Mutation(key.getRow());
          if (!key.isDeleted()) {
            mutation.put(key.getColumnFamily(), key.getColumnQualifier(),
                new ColumnVisibility(key.getColumnVisibilityData().toArray()),
                key.getTimestamp(), value);
          } else {
            mutation.putDelete(key.getColumnFamily(), key.getColumnQualifier(),
                new ColumnVisibility(key.getColumnVisibilityData().toArray()),
                key.getTimestamp());
          }
          table.addMutation(mutation);
          importIterator.next();
        }
      } catch (Exception e) {
        FSDataOutputStream failureWriter = null;
        DataInputStream failureReader = null;
        try {
          failureWriter = failureFs.create(failurePath.suffix("/"
              + importStatus.getPath().getName()));
          failureReader = importFs.open(importStatus.getPath());
          int read = 0;
          byte[] buffer = new byte[1024];
          while (-1 != (read = failureReader.read(buffer))) {
            failureWriter.write(buffer, 0, read);
          }
        } finally {
          if (failureReader != null)
            failureReader.close();
          if (failureWriter != null)
            failureWriter.close();
        }
      }
      importFs.delete(importStatus.getPath(), true);
    }
  }
  
  @Override
  public void offline(String tableName) throws AccumuloSecurityException, AccumuloException {
    throw new NotImplementedException();
  }
  
  @Override
  public void online(String tableName) throws AccumuloSecurityException, AccumuloException {}
  
  @Override
  public void clearLocatorCache(String tableName) throws TableNotFoundException {
    throw new NotImplementedException();
  }
  
  @Override
  public Map<String,String> tableIdMap() {
    Map<String,String> result = new HashMap<String,String>();
    for (String table : acu.tables.keySet()) {
      result.put(table, table);
    }
    return result;
  }
  
  @Override
  public void merge(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    throw new NotImplementedException();
  }
  
  @Override
  public void deleteRows(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    throw new NotImplementedException();
  }
  
  @Override
  public void compact(String tableName, Text start, Text end, boolean flush, boolean wait) throws AccumuloSecurityException, TableNotFoundException,
      AccumuloException {
    throw new NotImplementedException();
  }
  
  @Override
  public void clone(String srcTableName, String newTableName, boolean flush, Map<String,String> propertiesToSet, Set<String> propertiesToExclude)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
    throw new NotImplementedException();
  }
  
  @Override
  public void flush(String tableName, Text start, Text end, boolean wait) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    throw new NotImplementedException();
  }
  
  @Override
  public Text getMaxRow(String tableName, Authorizations auths, Text startRow, boolean startInclusive, Text endRow, boolean endInclusive)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    MockTable table = acu.tables.get(tableName);
    if (table == null)
      throw new TableNotFoundException(tableName, tableName, "no such table");
    
    return FindMax.findMax(new MockScanner(table, auths), startRow, startInclusive, endRow, endInclusive);
  }
}
