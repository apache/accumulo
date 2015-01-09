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
package org.apache.accumulo.core.client.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

class OfflineIterator implements Iterator<Entry<Key,Value>> {

  static class OfflineIteratorEnvironment implements IteratorEnvironment {
    @Override
    public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
      throw new NotImplementedException();
    }

    @Override
    public AccumuloConfiguration getConfig() {
      return AccumuloConfiguration.getDefaultConfiguration();
    }

    @Override
    public IteratorScope getIteratorScope() {
      return IteratorScope.scan;
    }

    @Override
    public boolean isFullMajorCompaction() {
      return false;
    }

    private ArrayList<SortedKeyValueIterator<Key,Value>> topLevelIterators = new ArrayList<SortedKeyValueIterator<Key,Value>>();

    @Override
    public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
      topLevelIterators.add(iter);
    }

    SortedKeyValueIterator<Key,Value> getTopLevelIterator(SortedKeyValueIterator<Key,Value> iter) {
      if (topLevelIterators.isEmpty())
        return iter;
      ArrayList<SortedKeyValueIterator<Key,Value>> allIters = new ArrayList<SortedKeyValueIterator<Key,Value>>(topLevelIterators);
      allIters.add(iter);
      return new MultiIterator(allIters, false);
    }
  }

  private SortedKeyValueIterator<Key,Value> iter;
  private Range range;
  private KeyExtent currentExtent;
  private Connector conn;
  private String tableId;
  private Authorizations authorizations;
  private Instance instance;
  private ScannerOptions options;
  private ArrayList<SortedKeyValueIterator<Key,Value>> readers;
  private AccumuloConfiguration config;

  public OfflineIterator(ScannerOptions options, Instance instance, Credentials credentials, Authorizations authorizations, Text table, Range range) {
    this.options = new ScannerOptions(options);
    this.instance = instance;
    this.range = range;

    if (this.options.fetchedColumns.size() > 0) {
      this.range = range.bound(this.options.fetchedColumns.first(), this.options.fetchedColumns.last());
    }

    this.tableId = table.toString();
    this.authorizations = authorizations;
    this.readers = new ArrayList<SortedKeyValueIterator<Key,Value>>();

    try {
      conn = instance.getConnector(credentials.getPrincipal(), credentials.getToken());
      config = new ConfigurationCopy(conn.instanceOperations().getSiteConfiguration());
      nextTablet();

      while (iter != null && !iter.hasTop())
        nextTablet();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNext() {
    return iter != null && iter.hasTop();
  }

  @Override
  public Entry<Key,Value> next() {
    try {
      byte[] v = iter.getTopValue().get();
      // copy just like tablet server does, do this before calling next
      KeyValue ret = new KeyValue(new Key(iter.getTopKey()), Arrays.copyOf(v, v.length));

      iter.next();

      while (iter != null && !iter.hasTop())
        nextTablet();

      return ret;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void nextTablet() throws TableNotFoundException, AccumuloException, IOException {

    Range nextRange = null;

    if (currentExtent == null) {
      Text startRow;

      if (range.getStartKey() != null)
        startRow = range.getStartKey().getRow();
      else
        startRow = new Text();

      nextRange = new Range(new KeyExtent(new Text(tableId), startRow, null).getMetadataEntry(), true, null, false);
    } else {

      if (currentExtent.getEndRow() == null) {
        iter = null;
        return;
      }

      if (range.afterEndKey(new Key(currentExtent.getEndRow()).followingKey(PartialKey.ROW))) {
        iter = null;
        return;
      }

      nextRange = new Range(currentExtent.getMetadataEntry(), false, null, false);
    }

    List<String> relFiles = new ArrayList<String>();

    Pair<KeyExtent,String> eloc = getTabletFiles(nextRange, relFiles);

    while (eloc.getSecond() != null) {
      if (Tables.getTableState(instance, tableId) != TableState.OFFLINE) {
        Tables.clearCache(instance);
        if (Tables.getTableState(instance, tableId) != TableState.OFFLINE) {
          throw new AccumuloException("Table is online " + tableId + " cannot scan tablet in offline mode " + eloc.getFirst());
        }
      }

      UtilWaitThread.sleep(250);

      eloc = getTabletFiles(nextRange, relFiles);
    }

    KeyExtent extent = eloc.getFirst();

    if (!extent.getTableId().toString().equals(tableId)) {
      throw new AccumuloException(" did not find tablets for table " + tableId + " " + extent);
    }

    if (currentExtent != null && !extent.isPreviousExtent(currentExtent))
      throw new AccumuloException(" " + currentExtent + " is not previous extent " + extent);

    // Old property is only used to resolve relative paths into absolute paths. For systems upgraded
    // with relative paths, it's assumed that correct instance.dfs.{uri,dir} is still correct in the configuration
    @SuppressWarnings("deprecation")
    String tablesDir = config.get(Property.INSTANCE_DFS_DIR) + Constants.HDFS_TABLES_DIR;

    List<String> absFiles = new ArrayList<String>();
    for (String relPath : relFiles) {
      if (relPath.contains(":")) {
        absFiles.add(relPath);
      } else {
        // handle old-style relative paths
        if (relPath.startsWith("..")) {
          absFiles.add(tablesDir + relPath.substring(2));
        } else {
          absFiles.add(tablesDir + "/" + tableId + relPath);
        }
      }
    }

    iter = createIterator(extent, absFiles);
    iter.seek(range, LocalityGroupUtil.families(options.fetchedColumns), options.fetchedColumns.size() == 0 ? false : true);
    currentExtent = extent;

  }

  private Pair<KeyExtent,String> getTabletFiles(Range nextRange, List<String> relFiles) throws TableNotFoundException {
    Scanner scanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setBatchSize(100);
    scanner.setRange(nextRange);

    RowIterator rowIter = new RowIterator(scanner);
    Iterator<Entry<Key,Value>> row = rowIter.next();

    KeyExtent extent = null;
    String location = null;

    while (row.hasNext()) {
      Entry<Key,Value> entry = row.next();
      Key key = entry.getKey();

      if (key.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
        relFiles.add(key.getColumnQualifier().toString());
      }

      if (key.getColumnFamily().equals(TabletsSection.CurrentLocationColumnFamily.NAME)
          || key.getColumnFamily().equals(TabletsSection.FutureLocationColumnFamily.NAME)) {
        location = entry.getValue().toString();
      }

      if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)) {
        extent = new KeyExtent(key.getRow(), entry.getValue());
      }

    }
    return new Pair<KeyExtent,String>(extent, location);
  }

  private SortedKeyValueIterator<Key,Value> createIterator(KeyExtent extent, List<String> absFiles) throws TableNotFoundException, AccumuloException,
      IOException {

    // TODO share code w/ tablet - ACCUMULO-1303
    AccumuloConfiguration acuTableConf = AccumuloConfiguration.getTableConfiguration(conn, tableId);

    Configuration conf = CachedConfiguration.getInstance();

    for (SortedKeyValueIterator<Key,Value> reader : readers) {
      ((FileSKVIterator) reader).close();
    }

    readers.clear();

    // TODO need to close files - ACCUMULO-1303
    for (String file : absFiles) {
      FileSystem fs = VolumeConfiguration.getVolume(file, conf, config).getFileSystem();
      FileSKVIterator reader = FileOperations.getInstance().openReader(file, false, fs, conf, acuTableConf, null, null);
      readers.add(reader);
    }

    MultiIterator multiIter = new MultiIterator(readers, extent);

    OfflineIteratorEnvironment iterEnv = new OfflineIteratorEnvironment();

    DeletingIterator delIter = new DeletingIterator(multiIter, false);

    ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(delIter);

    ColumnQualifierFilter colFilter = new ColumnQualifierFilter(cfsi, new HashSet<Column>(options.fetchedColumns));

    byte[] defaultSecurityLabel;

    ColumnVisibility cv = new ColumnVisibility(acuTableConf.get(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY));
    defaultSecurityLabel = cv.getExpression();

    VisibilityFilter visFilter = new VisibilityFilter(colFilter, authorizations, defaultSecurityLabel);

    return iterEnv.getTopLevelIterator(IteratorUtil.loadIterators(IteratorScope.scan, visFilter, extent, acuTableConf, options.serverSideIteratorList,
        options.serverSideIteratorOptions, iterEnv, false));
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}

/**
 *
 */
public class OfflineScanner extends ScannerOptions implements Scanner {

  private int batchSize;
  private int timeOut;
  private Range range;

  private Instance instance;
  private Credentials credentials;
  private Authorizations authorizations;
  private Text tableId;

  public OfflineScanner(Instance instance, Credentials credentials, String tableId, Authorizations authorizations) {
    ArgumentChecker.notNull(instance, credentials, tableId, authorizations);
    this.instance = instance;
    this.credentials = credentials;
    this.tableId = new Text(tableId);
    this.range = new Range((Key) null, (Key) null);

    this.authorizations = authorizations;

    this.batchSize = Constants.SCAN_BATCH_SIZE;
    this.timeOut = Integer.MAX_VALUE;
  }

  @Deprecated
  @Override
  public void setTimeOut(int timeOut) {
    this.timeOut = timeOut;
  }

  @Deprecated
  @Override
  public int getTimeOut() {
    return timeOut;
  }

  @Override
  public void setRange(Range range) {
    this.range = range;
  }

  @Override
  public Range getRange() {
    return range;
  }

  @Override
  public void setBatchSize(int size) {
    this.batchSize = size;
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public void enableIsolation() {

  }

  @Override
  public void disableIsolation() {

  }

  @Override
  public Iterator<Entry<Key,Value>> iterator() {
    return new OfflineIterator(this, instance, credentials, authorizations, tableId, range);
  }

  @Override
  public long getReadaheadThreshold() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setReadaheadThreshold(long batches) {
    throw new UnsupportedOperationException();
  }

}
