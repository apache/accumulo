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
package org.apache.accumulo.core.iteratorsImpl.system;

import java.io.DataInputStream;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class MapFileIterator implements FileSKVIterator {

  private static final String MSG = "Map files are not supported";

  public MapFileIterator(FileSystem fs, String dir, Configuration conf) {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public boolean hasTop() {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public void next() {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public Key getTopKey() {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public Value getTopValue() {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public Key getFirstKey() {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public Key getLastKey() {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public DataInputStream getMetaStore(String name) {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public void closeDeepCopies() {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig) {
    throw new UnsupportedOperationException(MSG);
  }

  @Override
  public void setCacheProvider(CacheProvider cacheProvider) {
    throw new UnsupportedOperationException(MSG);
  }
}
