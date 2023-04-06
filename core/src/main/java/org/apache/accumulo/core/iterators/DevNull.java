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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

/**
 * An iterator that is useful testing... for example if you want to test ingest performance w/o
 * writing data to disk, insert this iterator for scan as follows using the accumulo shell.
 *
 * config -t ci -s table.iterator.minc.devnull=21,org.apache.accumulo.core.iterators.DevNull
 *
 * Could also make scans never return anything very quickly by adding it to the scan stack
 *
 * config -t ci -s table.iterator.scan.devnull=21,org.apache.accumulo.core.iterators.DevNull
 *
 * And to make major compactions never write anything
 *
 * config -t ci -s table.iterator.majc.devnull=21,org.apache.accumulo.core.iterators.DevNull
 */
public class DevNull implements SortedKeyValueIterator<Key,Value> {

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Key getTopKey() {
    return null;
  }

  @Override
  public Value getTopValue() {
    return null;
  }

  @Override
  public boolean hasTop() {
    return false;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {

  }

  @Override
  public void next() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {

  }

}
