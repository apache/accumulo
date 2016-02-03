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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;

/**
 * A collection of methods that are helpful to the development of {@link IteratorTestCase}s.
 */
public class IteratorTestUtil {

  public static SortedKeyValueIterator<Key,Value> instantiateIterator(IteratorTestInput input) {
    try {
      return requireNonNull(input.getIteratorClass()).newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static SortedKeyValueIterator<Key,Value> createSource(IteratorTestInput input) {
    return new SimpleKVReusingIterator(new ColumnFamilySkippingIterator(new SortedMapIterator(requireNonNull(input).getInput())));
  }
}
