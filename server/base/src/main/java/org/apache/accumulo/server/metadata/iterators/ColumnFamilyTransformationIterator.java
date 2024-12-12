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
package org.apache.accumulo.server.metadata.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * Provides the ability to seek to and transform an entire column family in a row into a single
 * value. This class is intended to be used with conditional mutation iterators that want to check
 * an entire column family.
 */
public abstract class ColumnFamilyTransformationIterator
    implements SortedKeyValueIterator<Key,Value> {

  protected static final Text EMPTY = new Text();
  private SortedKeyValueIterator<Key,Value> source;

  private Key startKey = null;
  private Value topValue = null;

  static Text getTabletRow(Range range) {
    var row = range.getStartKey().getRow();
    // expecting this range to cover a single metadata row, so validate the range meets expectations
    MetadataSchema.TabletsSection.validateRow(row);
    Preconditions.checkArgument(row.equals(range.getEndKey().getRow()));
    return row;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {

    Text tabletRow = getTabletRow(range);
    Text family = range.getStartKey().getColumnFamily();

    Preconditions.checkArgument(
        family.getLength() > 0 && range.getStartKey().getColumnQualifier().getLength() == 0);

    Key startKey = new Key(tabletRow, family);
    Key endKey = new Key(tabletRow, family).followingKey(PartialKey.ROW_COLFAM);

    Range r = new Range(startKey, true, endKey, false);

    source.seek(r, Set.of(), false);

    topValue = transform(source);

    this.startKey = startKey;
  }

  /**
   * An iterator that is limited to a single column family is passed in and should be transformed to
   * a single Value. Can return null if this iterator should not have a top.
   */
  protected abstract Value transform(SortedKeyValueIterator<Key,Value> familyIterator)
      throws IOException;

  @Override
  public Key getTopKey() {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    if (topValue == null) {
      throw new NoSuchElementException();
    }

    return startKey;
  }

  @Override
  public Value getTopValue() {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    if (topValue == null) {
      throw new NoSuchElementException();
    }
    return topValue;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isRunningLowOnMemory() {
    return source.isRunningLowOnMemory();
  }

  @Override
  public boolean hasTop() {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    return topValue != null;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
  }

  @Override
  public void next() throws IOException {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    topValue = null;
  }

}
