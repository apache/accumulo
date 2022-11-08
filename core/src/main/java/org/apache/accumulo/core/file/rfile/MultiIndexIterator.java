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
package org.apache.accumulo.core.file.rfile;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.IndexEntry;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.HeapIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;

class MultiIndexIterator extends HeapIterator implements FileSKVIterator {

  private RFile.Reader source;

  MultiIndexIterator(RFile.Reader source, List<Iterator<IndexEntry>> indexes) {
    super(indexes.size());

    this.source = source;

    for (Iterator<IndexEntry> index : indexes) {
      addSource(new IndexIterator(index));
    }
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    source.close();
  }

  @Override
  public void closeDeepCopies() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Key getFirstKey() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Key getLastKey() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataInputStream getMetaStore(String name) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCacheProvider(CacheProvider cacheProvider) {
    source.setCacheProvider(cacheProvider);
  }
}
