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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class ColumnFamilySkippingIterator extends SkippingIterator implements InterruptibleIterator {

  protected Set<ByteSequence> colFamSet = null;
  protected TreeSet<ByteSequence> sortedColFams = null;

  protected boolean inclusive = false;
  protected Range range;

  public ColumnFamilySkippingIterator(SortedKeyValueIterator<Key,Value> source) {
    this.setSource(source);
  }

  protected ColumnFamilySkippingIterator(SortedKeyValueIterator<Key,Value> source, Set<ByteSequence> colFamSet, boolean inclusive) {
    this(source);
    this.colFamSet = colFamSet;
    this.inclusive = inclusive;
  }

  @Override
  protected void consume() throws IOException {
    int count = 0;

    if (inclusive)
      while (getSource().hasTop() && !colFamSet.contains(getSource().getTopKey().getColumnFamilyData())) {
        if (count < 10) {
          // it is quicker to call next if we are close, but we never know if we are close
          // so give next a try a few times
          getSource().next();
          count++;
        } else {
          ByteSequence higherCF = sortedColFams.higher(getSource().getTopKey().getColumnFamilyData());
          if (higherCF == null) {
            // seek to the next row
            reseek(getSource().getTopKey().followingKey(PartialKey.ROW));
          } else {
            // seek to the next column family in the sorted list of column families
            reseek(new Key(getSource().getTopKey().getRowData().toArray(), higherCF.toArray(), new byte[0], new byte[0], Long.MAX_VALUE));
          }

          count = 0;
        }
      }
    else if (colFamSet != null && colFamSet.size() > 0)
      while (getSource().hasTop() && colFamSet.contains(getSource().getTopKey().getColumnFamilyData())) {
        if (count < 10) {
          getSource().next();
          count++;
        } else {
          // seek to the next column family in the data
          reseek(getSource().getTopKey().followingKey(PartialKey.ROW_COLFAM));
          count = 0;
        }
      }
  }

  private void reseek(Key key) throws IOException {
    if (range.afterEndKey(key)) {
      range = new Range(range.getEndKey(), true, range.getEndKey(), range.isEndKeyInclusive());
      getSource().seek(range, colFamSet, inclusive);
    } else {
      range = new Range(key, true, range.getEndKey(), range.isEndKeyInclusive());
      getSource().seek(range, colFamSet, inclusive);
    }
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new ColumnFamilySkippingIterator(getSource().deepCopy(env), colFamSet, inclusive);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

    if (columnFamilies instanceof Set<?>) {
      colFamSet = (Set<ByteSequence>) columnFamilies;
    } else {
      colFamSet = new HashSet<ByteSequence>();
      colFamSet.addAll(columnFamilies);
    }

    if (inclusive) {
      sortedColFams = new TreeSet<ByteSequence>(colFamSet);
    } else {
      sortedColFams = null;
    }

    this.range = range;
    this.inclusive = inclusive;
    super.seek(range, colFamSet, inclusive);
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    ((InterruptibleIterator) getSource()).setInterruptFlag(flag);
  }

}
