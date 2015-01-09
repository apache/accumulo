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
package org.apache.accumulo.core.iterators.system;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.NoSuchMetaStoreException;
import org.apache.accumulo.core.file.map.MapFileUtil;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.log4j.Logger;

public class MapFileIterator implements FileSKVIterator {
  private static final Logger log = Logger.getLogger(MapFileIterator.class);

  private Reader reader;
  private Value topValue;
  private Key topKey;
  private AtomicBoolean interruptFlag;
  private int interruptCheckCount = 0;
  private FileSystem fs;
  private String dirName;

  public MapFileIterator(AccumuloConfiguration acuconf, FileSystem fs, String dir, Configuration conf) throws IOException {
    this.reader = MapFileUtil.openMapFile(acuconf, fs, dir, conf);
    this.fs = fs;
    this.dirName = dir;
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    this.interruptFlag = flag;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasTop() {
    return topKey != null;
  }

  @Override
  public void next() throws IOException {
    if (interruptFlag != null && interruptCheckCount++ % 100 == 0 && interruptFlag.get())
      throw new IterationInterruptedException();

    reader.next(topKey, topValue);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    if (columnFamilies.size() != 0 || inclusive) {
      throw new IllegalArgumentException("I do not know how to filter column families");
    }

    if (range == null)
      throw new IllegalArgumentException("Cannot seek to null range");

    if (interruptFlag != null && interruptFlag.get())
      throw new IterationInterruptedException();

    Key key = range.getStartKey();
    if (key == null) {
      key = new Key();
    }

    reader.seek(key);

    while (hasTop() && range.beforeStartKey(getTopKey())) {
      next();
    }
  }

  @Override
  public Key getTopKey() {
    return topKey;
  }

  @Override
  public Value getTopValue() {
    return topValue;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    try {
      SortedKeyValueIterator<Key,Value> other = env.reserveMapFileReader(dirName);
      ((InterruptibleIterator) other).setInterruptFlag(interruptFlag);
      log.debug("deep copying MapFile: " + this + " -> " + other);
      return other;
    } catch (IOException e) {
      log.error("failed to clone map file reader", e);
      throw new RuntimeException(e);
    }
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
    Path path = new Path(this.dirName, name);
    if (!fs.exists(path))
      throw new NoSuchMetaStoreException("name = " + name);
    return fs.open(path);
  }

  @Override
  public void closeDeepCopies() throws IOException {
    // nothing to do, deep copies are externally managed/closed
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
