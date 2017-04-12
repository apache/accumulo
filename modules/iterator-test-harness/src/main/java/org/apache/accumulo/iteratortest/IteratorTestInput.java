/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.iteratortest;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * The necessary user-input to invoke a test on a {@link SortedKeyValueIterator}.
 */
public class IteratorTestInput {

  private final Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass;
  private final Map<String,String> iteratorOptions;
  private final Range range;
  private final Collection<ByteSequence> families;
  private final boolean inclusive;
  private final SortedMap<Key,Value> input;

  /**
   * Construct an instance of the test input.
   *
   * @param iteratorClass
   *          The class for the iterator to test.
   * @param iteratorOptions
   *          Options, if any, to provide to the iterator ({@link IteratorSetting}'s Map of properties).
   * @param range
   *          The Range of data to query ({@link Scanner#setRange(Range)}). By default no column families filter is specified.
   * @param input
   *          A sorted collection of Key-Value pairs acting as the table.
   */
  public IteratorTestInput(Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass, Map<String,String> iteratorOptions, Range range,
      SortedMap<Key,Value> input) {
    this(iteratorClass, iteratorOptions, range, input, Collections.<ByteSequence> emptySet(), false);
  }

  /**
   * Construct an instance of the test input.
   *
   * @param iteratorClass
   *          The class for the iterator to test.
   * @param iteratorOptions
   *          Options, if any, to provide to the iterator ({@link IteratorSetting}'s Map of properties).
   * @param range
   *          The Range of data to query ({@link Scanner#setRange(Range)})
   * @param input
   *          A sorted collection of Key-Value pairs acting as the table.
   * @param families
   *          Column families passed to {@link SortedKeyValueIterator#seek}.
   * @param inclusive
   *          Whether the families are inclusive or exclusive.
   */
  public IteratorTestInput(Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass, Map<String,String> iteratorOptions, Range range,
      SortedMap<Key,Value> input, Collection<ByteSequence> families, boolean inclusive) {
    // Already immutable
    this.iteratorClass = requireNonNull(iteratorClass);
    // Make it immutable to the test
    this.iteratorOptions = Collections.unmodifiableMap(requireNonNull(iteratorOptions));
    // Already immutable
    this.range = requireNonNull(range);
    // Make it immutable to the test
    this.input = Collections.unmodifiableSortedMap((requireNonNull(input)));
    this.families = Collections.unmodifiableCollection(requireNonNull(families));
    this.inclusive = inclusive;
  }

  public Class<? extends SortedKeyValueIterator<Key,Value>> getIteratorClass() {
    return iteratorClass;
  }

  public Map<String,String> getIteratorOptions() {
    return iteratorOptions;
  }

  public Range getRange() {
    return range;
  }

  public Collection<ByteSequence> getFamilies() {
    return families;
  }

  public boolean isInclusive() {
    return inclusive;
  }

  public SortedMap<Key,Value> getInput() {
    return input;
  }

  @Override
  public String toString() {
    return "[iteratorClass=" + iteratorClass + ", iteratorOptions=" + iteratorOptions + ", range=" + range + ", families=" + families + ", inclusive="
        + inclusive + ", input='" + input + "']";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof IteratorTestInput))
      return false;
    IteratorTestInput that = (IteratorTestInput) o;

    return inclusive == that.inclusive && iteratorClass.equals(that.iteratorClass) && iteratorOptions.equals(that.iteratorOptions) && range.equals(that.range)
        && families.equals(that.families) && input.equals(that.input);
  }

  @Override
  public int hashCode() {
    int result = iteratorClass.hashCode();
    result = 31 * result + iteratorOptions.hashCode();
    result = 31 * result + range.hashCode();
    result = 31 * result + families.hashCode();
    result = 31 * result + (inclusive ? 1 : 0);
    result = 31 * result + input.hashCode();
    return result;
  }
}
