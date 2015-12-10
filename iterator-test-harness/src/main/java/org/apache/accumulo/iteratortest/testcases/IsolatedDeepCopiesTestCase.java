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
package org.apache.accumulo.iteratortest.testcases;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.IteratorTestUtil;
import org.apache.accumulo.iteratortest.environments.SimpleIteratorEnvironment;

/**
 * Test case that verifies that copies do not impact one another.
 */
public class IsolatedDeepCopiesTestCase extends OutputVerifyingTestCase {

  @Override
  public IteratorTestOutput test(IteratorTestInput testInput) {
    final SortedKeyValueIterator<Key,Value> skvi = IteratorTestUtil.instantiateIterator(testInput);
    final SortedKeyValueIterator<Key,Value> source = IteratorTestUtil.createSource(testInput);

    try {
      skvi.init(source, testInput.getIteratorOptions(), new SimpleIteratorEnvironment());

      SortedKeyValueIterator<Key,Value> copy1 = skvi.deepCopy(new SimpleIteratorEnvironment());
      SortedKeyValueIterator<Key,Value> copy2 = copy1.deepCopy(new SimpleIteratorEnvironment());

      skvi.seek(testInput.getRange(), Collections.<ByteSequence> emptySet(), false);
      copy1.seek(testInput.getRange(), Collections.<ByteSequence> emptySet(), false);
      copy2.seek(testInput.getRange(), Collections.<ByteSequence> emptySet(), false);

      TreeMap<Key,Value> output = consumeMany(Arrays.asList(skvi, copy1, copy2));

      return new IteratorTestOutput(output);
    } catch (IOException e) {
      return new IteratorTestOutput(e);
    }
  }

  TreeMap<Key,Value> consumeMany(Collection<SortedKeyValueIterator<Key,Value>> iterators) throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    // All of the copies should have consistent results from concurrent use
    while (allHasTop(iterators)) {
      data.put(getTopKey(iterators), getTopValue(iterators));
      next(iterators);
    }

    // All of the iterators should be consumed.
    for (SortedKeyValueIterator<Key,Value> iter : iterators) {
      if (iter.hasTop()) {
        return null;
      }
    }

    return data;
  }

  boolean allHasTop(Collection<SortedKeyValueIterator<Key,Value>> iterators) {
    for (SortedKeyValueIterator<Key,Value> iter : iterators) {
      if (!iter.hasTop()) {
        return false;
      }
    }
    return true;
  }

  Key getTopKey(Collection<SortedKeyValueIterator<Key,Value>> iterators) {
    boolean first = true;
    Key topKey = null;
    for (SortedKeyValueIterator<Key,Value> iter : iterators) {
      if (first) {
        topKey = iter.getTopKey();
        first = false;
      } else if (!topKey.equals(iter.getTopKey())) {
        throw new IllegalStateException("Inconsistent keys between two iterators: " + topKey + " " + iter.getTopKey());
      }
    }

    return topKey;
  }

  Value getTopValue(Collection<SortedKeyValueIterator<Key,Value>> iterators) {
    boolean first = true;
    Value topValue = null;
    for (SortedKeyValueIterator<Key,Value> iter : iterators) {
      if (first) {
        topValue = iter.getTopValue();
        first = false;
      } else if (!topValue.equals(iter.getTopValue())) {
        throw new IllegalStateException("Inconsistent values between two iterators: " + topValue + " " + iter.getTopValue()); 
      }
    }

    return topValue;
  }

  void next(Collection<SortedKeyValueIterator<Key,Value>> iterators) throws IOException {
    for (SortedKeyValueIterator<Key,Value> iter : iterators) {
      iter.next();
    }
  }
}
