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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

/**
 * This iterator suppresses rows that exceed a specified number of columns. Once a row exceeds the
 * threshold, a marker is emitted and the row is always suppressed by this iterator after that point
 * in time.
 *
 * This iterator works in a similar way to the RowDeletingIterator. See its javadoc about locality
 * groups.
 */
public class LargeRowFilter implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

  public static final Value SUPPRESS_ROW_VALUE = new Value("SUPPRESS_ROW");

  private static final ByteSequence EMPTY = new ArrayByteSequence(new byte[] {});

  /* key into hash map, value refers to the row suppression limit (maxColumns) */
  private static final String MAX_COLUMNS = "max_columns";

  private SortedKeyValueIterator<Key,Value> source;

  // a cache of keys
  private ArrayList<Key> keys = new ArrayList<>();
  private ArrayList<Value> values = new ArrayList<>();

  private int currentPosition;

  private int maxColumns;

  private boolean propagateSuppression = false;

  private Range range;
  private Collection<ByteSequence> columnFamilies;
  private boolean inclusive;
  private boolean dropEmptyColFams;

  private boolean isSuppressionMarker(Key key, Value val) {
    return key.getColumnFamilyData().length() == 0 && key.getColumnQualifierData().length() == 0
        && key.getColumnVisibilityData().length() == 0 && val.equals(SUPPRESS_ROW_VALUE);
  }

  private void reseek(Key key) throws IOException {
    if (range.afterEndKey(key)) {
      range = new Range(range.getEndKey(), true, range.getEndKey(), range.isEndKeyInclusive());
      source.seek(range, columnFamilies, inclusive);
    } else {
      range = new Range(key, true, range.getEndKey(), range.isEndKeyInclusive());
      source.seek(range, columnFamilies, inclusive);
    }
  }

  private void consumeRow(ByteSequence row) throws IOException {
    // try reading a few and if still not to next row, then seek
    int count = 0;

    while (source.hasTop() && source.getTopKey().getRowData().equals(row)) {
      source.next();
      count++;
      if (count >= 10) {
        Key nextRowStart = new Key(new Text(row.toArray())).followingKey(PartialKey.ROW);
        reseek(nextRowStart);
        count = 0;
      }
    }
  }

  private void addKeyValue(Key k, Value v) {
    if (dropEmptyColFams && k.getColumnFamilyData().equals(EMPTY)) {
      return;
    }
    keys.add(new Key(k));
    values.add(new Value(v));
  }

  private void bufferNextRow() throws IOException {

    keys.clear();
    values.clear();
    currentPosition = 0;

    while (source.hasTop() && keys.isEmpty()) {

      addKeyValue(source.getTopKey(), source.getTopValue());

      if (isSuppressionMarker(source.getTopKey(), source.getTopValue())) {

        consumeRow(source.getTopKey().getRowData());

      } else {

        ByteSequence currentRow = keys.get(0).getRowData();
        source.next();

        while (source.hasTop() && source.getTopKey().getRowData().equals(currentRow)) {

          addKeyValue(source.getTopKey(), source.getTopValue());

          if (keys.size() > maxColumns) {
            keys.clear();
            values.clear();

            // when the row is to big, just emit a suppression
            // marker
            addKeyValue(new Key(new Text(currentRow.toArray())), SUPPRESS_ROW_VALUE);
            consumeRow(currentRow);
          } else {
            source.next();
          }
        }
      }

    }
  }

  private void readNextRow() throws IOException {

    bufferNextRow();

    while (!propagateSuppression && currentPosition < keys.size()
        && isSuppressionMarker(keys.get(0), values.get(0))) {
      bufferNextRow();
    }
  }

  private LargeRowFilter(SortedKeyValueIterator<Key,Value> source, boolean propagateSuppression,
      int maxColumns) {
    this.source = source;
    this.propagateSuppression = propagateSuppression;
    this.maxColumns = maxColumns;
  }

  public LargeRowFilter() {}

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
    this.maxColumns = Integer.parseInt(options.get(MAX_COLUMNS));
    this.propagateSuppression = env.getIteratorScope() != IteratorScope.scan;
  }

  @Override
  public boolean hasTop() {
    return currentPosition < keys.size();
  }

  @Override
  public void next() throws IOException {

    if (currentPosition >= keys.size()) {
      throw new IllegalStateException("Called next() when hasTop() is false");
    }

    currentPosition++;

    if (currentPosition == keys.size()) {
      readNextRow();
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

    this.range = range;
    this.columnFamilies = columnFamilies;
    this.inclusive = inclusive;

    if (range.getStartKey() != null) {
      // seek to beginning of row to see if there is a suppression marker
      Range newRange = new Range(new Key(range.getStartKey().getRow()), true, range.getEndKey(),
          range.isEndKeyInclusive());
      source.seek(newRange, columnFamilies, inclusive);

      readNextRow();

      // it is possible that all or some of the data read for the current
      // row is before the start of the range
      while (currentPosition < keys.size() && range.beforeStartKey(keys.get(currentPosition))) {
        currentPosition++;
      }

      if (currentPosition == keys.size()) {
        readNextRow();
      }

    } else {
      source.seek(range, columnFamilies, inclusive);
      readNextRow();
    }

  }

  @Override
  public Key getTopKey() {
    return keys.get(currentPosition);
  }

  @Override
  public Value getTopValue() {
    return values.get(currentPosition);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new LargeRowFilter(source.deepCopy(env), propagateSuppression, maxColumns);
  }

  @Override
  public IteratorOptions describeOptions() {
    String description =
        "This iterator suppresses rows that exceed a specified number of columns. Once\n"
            + "a row exceeds the threshold, a marker is emitted and the row is always\n"
            + "suppressed by this iterator after that point in time.\n"
            + " This iterator works in a similar way to the RowDeletingIterator. See its\n"
            + " javadoc about locality groups.\n";
    return new IteratorOptions(this.getClass().getSimpleName(), description,
        Collections.singletonMap(MAX_COLUMNS, "Number Of Columns To Begin Suppression"), null);
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (options == null || options.size() < 1) {
      throw new IllegalArgumentException(
          "Bad # of options, must supply: " + MAX_COLUMNS + " as value");
    }

    if (!options.containsKey(MAX_COLUMNS)) {
      throw new IllegalArgumentException(
          "Bad # of options, must supply: " + MAX_COLUMNS + " as value");
    }
    try {
      maxColumns = Integer.parseInt(options.get(MAX_COLUMNS));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "bad integer " + MAX_COLUMNS + ":" + options.get(MAX_COLUMNS));
    }

    return true;
  }

  /**
   * A convenience method for setting the maximum number of columns to keep.
   *
   * @param is IteratorSetting object to configure.
   * @param maxColumns number of columns to keep.
   */
  public static void setMaxColumns(IteratorSetting is, int maxColumns) {
    is.addOption(MAX_COLUMNS, Integer.toString(maxColumns));
  }
}
