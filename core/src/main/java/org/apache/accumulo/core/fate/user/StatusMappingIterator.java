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
package org.apache.accumulo.core.fate.user;

import static org.apache.accumulo.core.fate.user.schema.FateSchema.TxColumnFamily.STATUS_COLUMN;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * A specialized iterator that maps the value of the status column to "present" or "absent". This
 * iterator allows for checking of the status column's value against a set of acceptable statuses
 * within a conditional mutation.
 */
public class StatusMappingIterator implements SortedKeyValueIterator<Key,Value> {

  private static final String PRESENT = "present";
  private static final String ABSENT = "absent";
  private static final String STATUS_SET_KEY = "statusSet";

  private SortedKeyValueIterator<Key,Value> source;
  private final Set<String> acceptableStatuses = new HashSet<>();
  private Value mappedValue;

  /**
   * The set of acceptable must be provided as an option to the iterator using the
   * {@link #STATUS_SET_KEY} key.
   */
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
    if (options.containsKey(STATUS_SET_KEY)) {
      String[] statuses = decodeStatuses(options.get(STATUS_SET_KEY));
      acceptableStatuses.addAll(Arrays.asList(statuses));
    } else {
      throw new IllegalArgumentException("Expected option " + STATUS_SET_KEY + " to be set.");
    }
  }

  @Override
  public boolean hasTop() {
    return source.hasTop();
  }

  @Override
  public void next() throws IOException {
    source.next();
    mapValue();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    source.seek(range, columnFamilies, inclusive);
    mapValue();
  }

  /**
   * Maps the value of the status column to "present" or "absent" based on its presence within the
   * set of statuses.
   */
  private void mapValue() {
    if (source.hasTop()) {
      String currentValue = source.getTopValue().toString();
      mappedValue =
          acceptableStatuses.contains(currentValue) ? new Value(PRESENT) : new Value(ABSENT);
    } else {
      mappedValue = null;
    }
  }

  @Override
  public Key getTopKey() {
    return source.getTopKey();
  }

  @Override
  public Value getTopValue() {
    return Objects.requireNonNull(mappedValue);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  /**
   * Creates a condition that checks if the status column's value is one of the given acceptable
   * statuses.
   *
   * @param statuses The acceptable statuses.
   * @return A condition configured to use this iterator.
   */
  public static Condition createCondition(ReadOnlyFateStore.TStatus... statuses) {
    Condition condition =
        new Condition(STATUS_COLUMN.getColumnFamily(), STATUS_COLUMN.getColumnQualifier());

    if (statuses.length == 0) {
      // If no statuses are provided, require the status column to be absent. Return the condition
      // with no value set so that the mutation will be rejected if the status column is present.
      return condition;
    } else {
      IteratorSetting is = new IteratorSetting(100, StatusMappingIterator.class);
      is.addOption(STATUS_SET_KEY, encodeStatuses(statuses));

      // If the value of the status column is in the set, it will be mapped to "present", so set the
      // value of the condition to "present".
      return condition.setValue(PRESENT).setIterators(is);
    }
  }

  private static String encodeStatuses(ReadOnlyFateStore.TStatus[] statuses) {
    return Arrays.stream(statuses).map(Enum::name).collect(Collectors.joining(","));
  }

  private static String[] decodeStatuses(String statuses) {
    return statuses.split(",");
  }

}
