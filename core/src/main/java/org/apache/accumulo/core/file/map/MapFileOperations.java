/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.file.map;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.MapFileIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SequenceFileIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
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
      if (!hasTop) {
        throw new IllegalStateException();
      }
      return reader.getTopKey();
    }

    @Override
    public Value getTopValue() {
      if (!hasTop) {
        throw new IllegalStateException();
      }
      return reader.getTopValue();
    }

    @Override
    public boolean hasTop() {
      return hasTop;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void next() throws IOException {
      if (!hasTop) {
        throw new IllegalStateException();
      }
      reader.next();
      hasTop = reader.hasTop() && !range.afterEndKey(reader.getTopKey());
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
        throws IOException {
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

    @Override
    public FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig) {
      return ((FileSKVIterator) reader).getSample(sampleConfig);
    }

    @Override
    public void setCacheProvider(CacheProvider cacheProvider) {}
  }

  @Override
  protected FileSKVIterator openReader(FileOptions options) throws IOException {
    FileSKVIterator iter = new RangeIterator(new MapFileIterator(options.getFileSystem(),
        options.getFilename(), options.getConfiguration()));
    if (options.isSeekToBeginning()) {
      iter.seek(new Range(new Key(), null), new ArrayList<>(), false);
    }
    return iter;
  }

  @Override
  protected FileSKVWriter openWriter(FileOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected FileSKVIterator openIndex(FileOptions options) throws IOException {
    return new SequenceFileIterator(MapFileUtil.openIndex(options.getConfiguration(),
        options.getFileSystem(), new Path(options.getFilename())), false);
  }

  @Override
  protected long getFileSize(FileOptions options) throws IOException {
    return options.getFileSystem()
        .getFileStatus(new Path(options.getFilename() + "/" + MapFile.DATA_FILE_NAME)).getLen();
  }

  @Override
  protected FileSKVIterator openScanReader(FileOptions options) throws IOException {
    MapFileIterator mfIter = new MapFileIterator(options.getFileSystem(), options.getFilename(),
        options.getConfiguration());

    FileSKVIterator iter = new RangeIterator(mfIter);
    iter.seek(options.getRange(), options.getColumnFamilies(), options.isRangeInclusive());

    return iter;
  }
}
