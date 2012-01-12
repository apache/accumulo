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
package org.apache.accumulo.core.file.map;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileCFSkippingIterator;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.SequenceFileIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;

public class MapFileOperations extends FileOperations {
  
  public static class RangeIterator implements FileSKVIterator {
    
    SortedKeyValueIterator<Key,Value> reader;
    private Range range;
    private boolean hasTop;
    
    public RangeIterator(SortedKeyValueIterator<Key,Value> reader) {
      this.reader = reader;
    }
    
    @Override
    public void close() throws IOException {
      ((FileSKVIterator) reader).close();
    }
    
    @Override
    public Key getFirstKey() throws IOException {
      return ((FileSKVIterator) reader).getFirstKey();
    }
    
    @Override
    public Key getLastKey() throws IOException {
      return ((FileSKVIterator) reader).getLastKey();
    }
    
    @Override
    public DataInputStream getMetaStore(String name) throws IOException {
      return ((FileSKVIterator) reader).getMetaStore(name);
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      return new RangeIterator(reader.deepCopy(env));
    }
    
    @Override
    public Key getTopKey() {
      if (!hasTop)
        throw new IllegalStateException();
      return reader.getTopKey();
    }
    
    @Override
    public Value getTopValue() {
      if (!hasTop)
        throw new IllegalStateException();
      return reader.getTopValue();
    }
    
    @Override
    public boolean hasTop() {
      return hasTop;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void next() throws IOException {
      if (!hasTop)
        throw new IllegalStateException();
      reader.next();
      hasTop = reader.hasTop() && !range.afterEndKey(reader.getTopKey());
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
      reader.seek(range, columnFamilies, inclusive);
      this.range = range;
      
      hasTop = reader.hasTop() && !range.afterEndKey(reader.getTopKey());
      
      while (hasTop() && range.beforeStartKey(getTopKey())) {
        next();
      }
    }
    
    @Override
    public void closeDeepCopies() throws IOException {
      ((FileSKVIterator) reader).closeDeepCopies();
    }
    
    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      ((FileSKVIterator) reader).setInterruptFlag(flag);
    }
  }
  
  @Override
  public FileSKVIterator openReader(String file, boolean seekToBeginning, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    FileSKVIterator iter = new FileCFSkippingIterator(new RangeIterator(MapFileUtil.openMapFile(acuconf, fs, file, conf)));
    
    if (seekToBeginning)
      iter.seek(new Range(new Key(), null), new ArrayList<ByteSequence>(), false);
    
    return iter;
  }
  
  /**
   * @deprecated since 1.4
   * @use org.apache.hadoop.io.MapFile.Writer
   */
  public FileSKVWriter openWriter(final String file, final FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    final MyMapFile.Writer mfw = MapFileUtil.openMapFileWriter(acuconf, conf, fs, file);
    return new FileSKVWriter() {
      
      boolean secondCall = false;
      
      @Override
      public void append(Key key, Value value) throws IOException {
        mfw.append(new Key(key), value);
      }
      
      @Override
      public void close() throws IOException {
        mfw.close();
      }
      
      @Override
      public DataOutputStream createMetaStore(String name) throws IOException {
        return fs.create(new Path(file, name), false);
      }
      
      @Override
      public void startDefaultLocalityGroup() throws IOException {
        if (secondCall)
          throw new IllegalStateException("Start default locality group called twice");
        
        secondCall = true;
      }
      
      @Override
      public void startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies) throws IOException {
        throw new UnsupportedOperationException();
      }
      
      @Override
      public boolean supportsLocalityGroups() {
        return false;
      }
    };
  }
  
  @Override
  public FileSKVIterator openIndex(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    return new SequenceFileIterator(MapFileUtil.openIndex(conf, fs, new Path(file)), false);
  }
  
  @Override
  public long getFileSize(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    return fs.getFileStatus(new Path(file + "/" + MapFile.DATA_FILE_NAME)).getLen();
  }
  
  @SuppressWarnings("deprecation")
  @Override
  public FileSKVIterator openReader(String file, Range range, Set<ByteSequence> columnFamilies, boolean inclusive, FileSystem fs, Configuration conf,
      AccumuloConfiguration tableConf) throws IOException {
    MyMapFile.Reader mfIter = MapFileUtil.openMapFile(tableConf, fs, file, conf);
    
    FileSKVIterator iter = new RangeIterator(mfIter);
    
    if (columnFamilies.size() != 0 || inclusive) {
      iter = new FileCFSkippingIterator(iter);
    }
    
    iter.seek(range, columnFamilies, inclusive);
    mfIter.dropIndex();
    
    return iter;
  }
  
  @Override
  public FileSKVIterator openReader(String file, Range range, Set<ByteSequence> columnFamilies, boolean inclusive, FileSystem fs, Configuration conf,
      AccumuloConfiguration tableConf, BlockCache dataCache, BlockCache indexCache) throws IOException {
    
    return openReader(file, range, columnFamilies, inclusive, fs, conf, tableConf);
  }
  
  @Override
  public FileSKVIterator openReader(String file, boolean seekToBeginning, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf,
      BlockCache dataCache, BlockCache indexCache) throws IOException {
    
    return openReader(file, seekToBeginning, fs, conf, acuconf);
  }
  
  @Override
  public FileSKVIterator openIndex(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf, BlockCache dCache, BlockCache iCache)
      throws IOException {
    
    return openIndex(file, fs, conf, acuconf);
  }
}
