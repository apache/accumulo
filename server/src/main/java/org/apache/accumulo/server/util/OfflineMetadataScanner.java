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
package org.apache.accumulo.server.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.util.MetadataTable.LogEntry;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class OfflineMetadataScanner extends ScannerOptions implements Scanner {
  
  private Set<String> allFiles = new HashSet<String>();
  private Range range = new Range();
  private final FileSystem fs;
  private final AccumuloConfiguration conf;
  
  private List<SortedKeyValueIterator<Key,Value>> openMapFiles(Collection<String> files, FileSystem fs, AccumuloConfiguration conf) throws IOException {
    List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<SortedKeyValueIterator<Key,Value>>();
    for (String file : files) {
      FileSKVIterator reader = FileOperations.getInstance().openReader(file, true, fs, fs.getConf(), conf);
      readers.add(reader);
    }
    return readers;
  }
  
  private SortedKeyValueIterator<Key,Value> createSystemIter(Range r, List<SortedKeyValueIterator<Key,Value>> readers, HashSet<Column> columns)
      throws IOException {
    MultiIterator multiIterator = new MultiIterator(readers, false);
    DeletingIterator delIter = new DeletingIterator(multiIterator, false);
    ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(delIter);
    ColumnQualifierFilter colFilter = new ColumnQualifierFilter(cfsi, columns);
    VisibilityFilter visFilter = new VisibilityFilter(colFilter, Constants.NO_AUTHS, new byte[0]);
    
    visFilter.seek(r, LocalityGroupUtil.EMPTY_CF_SET, false);
    
    VersioningIterator vi = new VersioningIterator();
    Map<String,String> opts = new HashMap<String,String>();
    opts.put("maxVersions", "1");
    vi.init(visFilter, opts, null);
    
    return vi;
  }
  
  private static class MyEntry implements Map.Entry<Key,Value> {
    
    private Key k;
    private Value v;
    
    MyEntry(Key k, Value v) {
      this.k = k;
      this.v = v;
    }
    
    @Override
    public Key getKey() {
      return k;
    }
    
    @Override
    public Value getValue() {
      return v;
    }
    
    @Override
    public Value setValue(Value value) {
      throw new UnsupportedOperationException();
    }
    
  }
  
  public OfflineMetadataScanner(AccumuloConfiguration conf, FileSystem fs) throws IOException {
    super();
    this.fs = fs;
    this.conf = conf;
    List<LogEntry> rwal;
    try {
      rwal = MetadataTable.getLogEntries(null, Constants.ROOT_TABLET_EXTENT);
    } catch (Exception e) {
      throw new RuntimeException("Failed to check if root tablet has write ahead log entries", e);
    }
    
    if (rwal.size() > 0) {
      throw new RuntimeException("Root tablet has write ahead logs, can not scan offline");
    }
    
    FileStatus[] rootFiles = fs.listStatus(new Path(ServerConstants.getRootTabletDir()));
    
    for (FileStatus rootFile : rootFiles) {
      allFiles.add(rootFile.getPath().toString());
    }
    
    List<SortedKeyValueIterator<Key,Value>> readers = openMapFiles(allFiles, fs, conf);
    
    HashSet<Column> columns = new HashSet<Column>();
    columns.add(new Column(TextUtil.getBytes(Constants.METADATA_DATAFILE_COLUMN_FAMILY), null, null));
    columns.add(new Column(TextUtil.getBytes(Constants.METADATA_LOG_COLUMN_FAMILY), null, null));
    
    SortedKeyValueIterator<Key,Value> ssi = createSystemIter(new Range(), readers, columns);
    
    int walogs = 0;
    
    while (ssi.hasTop()) {
      if (ssi.getTopKey().compareColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY) == 0) {
        allFiles.add(ServerConstants.getMetadataTableDir() + "/" + ssi.getTopKey().getColumnQualifier().toString());
      } else {
        walogs++;
      }
      ssi.next();
    }
    
    closeReaders(readers);
    
    if (walogs > 0) {
      throw new RuntimeException("Metadata tablets have write ahead logs, can not scan offline");
    }
  }
  
  private void closeReaders(List<SortedKeyValueIterator<Key,Value>> readers) throws IOException {
    for (SortedKeyValueIterator<Key,Value> reader : readers) {
      ((FileSKVIterator) reader).close();
    }
  }
  
  @Override
  public int getBatchSize() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public Range getRange() {
    return range;
  }
  
  @Override
  public int getTimeOut() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public Iterator<Entry<Key,Value>> iterator() {
    
    final SortedKeyValueIterator<Key,Value> ssi;
    final List<SortedKeyValueIterator<Key,Value>> readers;
    try {
      readers = openMapFiles(allFiles, fs, conf);
      ssi = createSystemIter(range, readers, new HashSet<Column>(getFetchedColumns()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    return new Iterator<Entry<Key,Value>>() {
      
      @Override
      public boolean hasNext() {
        return ssi.hasTop() && range.contains(ssi.getTopKey());
      }
      
      @Override
      public Entry<Key,Value> next() {
        if (!ssi.hasTop()) {
          throw new NoSuchElementException();
        }
        
        MyEntry e = new MyEntry(new Key(ssi.getTopKey()), new Value(ssi.getTopValue()));
        try {
          ssi.next();
        } catch (IOException e1) {
          throw new RuntimeException(e1);
        }
        return e;
      }
      
      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
    };
  }
  
  @Override
  public void setBatchSize(int size) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void setRange(Range range) {
    this.range = range;
  }
  
  @Override
  public void setTimeOut(int timeOut) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void enableIsolation() {
    // should always give an isolated view since it is scanning immutable files
  }
  
  @Override
  public void disableIsolation() {
    
  }
  
  public static void main(String[] args) throws IOException {
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    ServerConfiguration conf = new ServerConfiguration(HdfsZooInstance.getInstance());
    OfflineMetadataScanner scanner = new OfflineMetadataScanner(conf.getConfiguration(), fs);
    scanner.setRange(Constants.METADATA_KEYSPACE);
    for (Entry<Key,Value> entry : scanner)
      System.out.println(entry.getKey() + " " + entry.getValue());
  }
  
}
