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
package org.apache.accumulo.core.iterators;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

public class ColumnFamilyCounter implements SortedKeyValueIterator<Key,Value> {

  private SortedKeyValueIterator<Key,Value> source;
  private Key key;
  private Value value;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
  }

  @Override
  public boolean hasTop() {
    return key != null;
  }

  @Override
  public void next() throws IOException {
    if (source.hasTop()) {
      ByteSequence currentRow = source.getTopKey().getRowData();
      ByteSequence currentColf = source.getTopKey().getColumnFamilyData();
      long ts = source.getTopKey().getTimestamp();

      source.next();

      int count = 1;

      while (source.hasTop() && source.getTopKey().getRowData().equals(currentRow) && source.getTopKey().getColumnFamilyData().equals(currentColf)) {
        count++;
        source.next();
      }

      this.key = new Key(currentRow.toArray(), currentColf.toArray(), new byte[0], new byte[0], ts);
      this.value = new Value(Integer.toString(count).getBytes(UTF_8));

    } else {
      this.key = null;
      this.value = null;
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
    next();
  }

  @Override
  public Key getTopKey() {
    return key;
  }

  @Override
  public Value getTopValue() {
    return value;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return null;
  }

}
