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
package org.apache.accumulo.core.iterators.user;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * An iterator for deleting whole rows.
 *
 * After setting this iterator up for your table, to delete a row insert a row with empty column
 * family, empty column qualifier, empty column visibility, and a value of DEL_ROW. Do not use empty
 * columns for anything else when using this iterator.
 *
 * When using this iterator the locality group containing the row deletes will always be read. The
 * locality group containing the empty column family will contain row deletes. Always reading this
 * locality group can have an impact on performance.
 *
 * For example assume there are two locality groups, one containing large images and one containing
 * small metadata about the images. If row deletes are in the same locality group as the images,
 * then this will significantly slow down scans and major compactions that are only reading the
 * metadata locality group. Therefore, you would want to put the empty column family in the locality
 * group that contains the metadata. Another option is to put the empty column in its own locality
 * group. Which is best depends on your data.
 */
public class RowDeletingIterator implements SortedKeyValueIterator<Key,Value> {

  public static final Value DELETE_ROW_VALUE = new Value("DEL_ROW");
  private SortedKeyValueIterator<Key,Value> source;
  private boolean propagateDeletes;
  private ByteSequence currentRow;
  private boolean currentRowDeleted;
  private long deleteTS;

  private boolean dropEmptyColFams;

  private static final ByteSequence EMPTY = new ArrayByteSequence(new byte[] {});

  private RowDeletingIterator(SortedKeyValueIterator<Key,Value> source, boolean propagateDeletes2) {
    this.source = source;
    this.propagateDeletes = propagateDeletes2;
  }

  public RowDeletingIterator() {}

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new RowDeletingIterator(source.deepCopy(env), propagateDeletes);
  }

  @Override
  public Key getTopKey() {
    return source.getTopKey();
  }

  @Override
  public Value getTopValue() {
    return source.getTopValue();
  }

  @Override
  public boolean hasTop() {
    return source.hasTop();
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
    this.propagateDeletes =
        (env.getIteratorScope() == IteratorScope.majc && !env.isFullMajorCompaction())
            || env.getIteratorScope() == IteratorScope.minc;
  }

  @Override
  public void next() throws IOException {
    source.next();
    consumeDeleted();
    consumeEmptyColFams();
  }

  private void consumeEmptyColFams() throws IOException {
    while (dropEmptyColFams && source.hasTop()
        && source.getTopKey().getColumnFamilyData().length() == 0) {
      source.next();
      consumeDeleted();
    }
  }

  private boolean isDeleteMarker(Key key, Value val) {
    return key.getColumnFamilyData().length() == 0 && key.getColumnQualifierData().length() == 0
        && key.getColumnVisibilityData().length() == 0 && val.equals(DELETE_ROW_VALUE);
  }

  private void consumeDeleted() throws IOException {
    // this method tries to do as little work as possible when nothing is deleted
    while (source.hasTop()) {
      if (currentRowDeleted) {
        while (source.hasTop() && currentRow.equals(source.getTopKey().getRowData())
            && source.getTopKey().getTimestamp() <= deleteTS) {
          source.next();
        }

        if (source.hasTop() && !currentRow.equals(source.getTopKey().getRowData())) {
          currentRowDeleted = false;
        }
      }

      if (!currentRowDeleted && source.hasTop()
          && isDeleteMarker(source.getTopKey(), source.getTopValue())) {
        currentRow = source.getTopKey().getRowData();
        currentRowDeleted = true;
        deleteTS = source.getTopKey().getTimestamp();

        if (propagateDeletes) {
          break;
        }
      } else {
        break;
      }
    }

  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {

    if (inclusive && !columnFamilies.contains(EMPTY)) {
      columnFamilies = new HashSet<>(columnFamilies);
      columnFamilies.add(EMPTY);
      dropEmptyColFams = true;
    } else if (!inclusive && columnFamilies.contains(EMPTY)) {
      columnFamilies = new HashSet<>(columnFamilies);
      columnFamilies.remove(EMPTY);
      dropEmptyColFams = true;
    } else {
      dropEmptyColFams = false;
    }

    currentRowDeleted = false;

    if (range.getStartKey() != null) {
      // seek to beginning of row
      Range newRange = new Range(new Key(range.getStartKey().getRow()), true, range.getEndKey(),
          range.isEndKeyInclusive());
      source.seek(newRange, columnFamilies, inclusive);
      consumeDeleted();
      consumeEmptyColFams();

      if (source.hasTop() && range.beforeStartKey(source.getTopKey())) {
        source.seek(range, columnFamilies, inclusive);
        consumeDeleted();
        consumeEmptyColFams();
      }
    } else {
      source.seek(range, columnFamilies, inclusive);
      consumeDeleted();
      consumeEmptyColFams();
    }

  }

}
