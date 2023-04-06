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
import java.util.Map;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;

/**
 * This iterator makes it easy to select rows that meet a given criteria. Its an alternative to the
 * {@link WholeRowIterator}. There are a few things to consider when deciding which one to use.
 *
 * First the WholeRowIterator requires that the row fit in memory and that the entire row is read
 * before a decision is made. This iterator has neither requirement, it allows seeking within a row
 * to avoid reading the entire row to make a decision. So even if your rows fit into memory, this
 * extending this iterator may be better choice because you can seek.
 *
 * Second the WholeRowIterator is currently the only way to achieve row isolation with the
 * {@link BatchScanner}. With the normal {@link Scanner} row isolation can be enabled and this
 * Iterator may be used.
 *
 * Third the row acceptance test will be executed every time this Iterator is seeked. If the row is
 * large, then the row will fetched in batches of key/values. As each batch is fetched the test may
 * be re-executed because the iterator stack is reseeked for each batch. The batch size may be
 * increased to reduce the number of times the test is executed. With the normal Scanner, if
 * isolation is enabled then it will read an entire row w/o seeking this iterator.
 */
public abstract class RowFilter extends WrappingIterator {

  private RowIterator decisionIterator;
  private Collection<ByteSequence> columnFamilies;
  Text currentRow;
  private boolean inclusive;
  private Range range;
  private boolean hasTop;

  private static class RowIterator extends WrappingIterator {
    private Range rowRange;
    private boolean hasTop;

    RowIterator(SortedKeyValueIterator<Key,Value> source) {
      super.setSource(source);
    }

    void setRow(Range row) {
      this.rowRange = row;
    }

    @Override
    public boolean hasTop() {
      return hasTop && super.hasTop();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
        throws IOException {

      range = rowRange.clip(range, true);
      if (range == null) {
        hasTop = false;
      } else {
        hasTop = true;
        super.seek(range, columnFamilies, inclusive);
      }
    }
  }

  private void skipRows() throws IOException {
    SortedKeyValueIterator<Key,Value> source = getSource();
    while (source.hasTop()) {
      Text row = source.getTopKey().getRow();

      if (currentRow != null && currentRow.equals(row)) {
        break;
      }

      Range rowRange = new Range(row);
      decisionIterator.setRow(rowRange);
      decisionIterator.seek(rowRange, columnFamilies, inclusive);

      if (acceptRow(decisionIterator)) {
        currentRow = row;
        break;
      } else {
        currentRow = null;
        int count = 0;
        while (source.hasTop() && count < 10 && source.getTopKey().getRow().equals(row)) {
          count++;
          source.next();
        }

        if (source.hasTop() && source.getTopKey().getRow().equals(row)) {
          Range nextRow = new Range(row, false, null, false);
          nextRow = range.clip(nextRow, true);
          if (nextRow == null) {
            hasTop = false;
          } else {
            source.seek(nextRow, columnFamilies, inclusive);
          }
        }
      }
    }
  }

  /**
   * Implementation should return false to suppress a row.
   *
   *
   * @param rowIterator - An iterator over the row. This iterator is confined to the row. Seeking
   *        past the end of the row will return no data. Seeking before the row will always set top
   *        to the first column in the current row. By default this iterator will only see the
   *        columns the parent was seeked with. To see more columns reseek this iterator with those
   *        columns.
   * @return false if a row should be suppressed, otherwise true.
   */
  public abstract boolean acceptRow(SortedKeyValueIterator<Key,Value> rowIterator)
      throws IOException;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    this.decisionIterator = new RowIterator(source.deepCopy(env));
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    RowFilter newInstance;
    try {
      newInstance = getClass().getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    newInstance.setSource(getSource().deepCopy(env));
    newInstance.decisionIterator = new RowIterator(getSource().deepCopy(env));
    return newInstance;
  }

  @Override
  public boolean hasTop() {
    return hasTop && super.hasTop();
  }

  @Override
  public void next() throws IOException {
    super.next();
    skipRows();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    super.seek(range, columnFamilies, inclusive);
    this.columnFamilies = columnFamilies;
    this.inclusive = inclusive;
    this.range = range;
    currentRow = null;
    hasTop = true;
    skipRows();

  }
}
