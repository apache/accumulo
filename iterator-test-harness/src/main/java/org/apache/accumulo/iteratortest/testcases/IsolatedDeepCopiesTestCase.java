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
package org.apache.accumulo.iteratortest.testcases;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.iteratortest.IteratorTestCase;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test case that verifies that copies do not impact one another.
 */
public class IsolatedDeepCopiesTestCase implements IteratorTestCase {
  private static final Logger log = LoggerFactory.getLogger(IsolatedDeepCopiesTestCase.class);

  private static final SecureRandom random = new SecureRandom();

  @Override
  public IteratorTestOutput test(IteratorTestInput testInput) {
    final SortedKeyValueIterator<Key,Value> skvi = IteratorTestUtil.instantiateIterator(testInput);
    final SortedKeyValueIterator<Key,Value> source = IteratorTestUtil.createSource(testInput);

    try {
      var iteratorEnvironment = testInput.getIteratorEnvironment();
      skvi.init(source, testInput.getIteratorOptions(), iteratorEnvironment);

      SortedKeyValueIterator<Key,Value> copy1 = skvi.deepCopy(iteratorEnvironment);
      SortedKeyValueIterator<Key,Value> copy2 = copy1.deepCopy(iteratorEnvironment);

      Range seekRange = testInput.getRange();
      Collection<ByteSequence> seekColumnFamilies = testInput.getFamilies();
      boolean seekInclusive = testInput.isInclusive();

      skvi.seek(testInput.getRange(), seekColumnFamilies, seekInclusive);
      copy1.seek(testInput.getRange(), seekColumnFamilies, seekInclusive);
      copy2.seek(testInput.getRange(), seekColumnFamilies, seekInclusive);

      TreeMap<Key,Value> output = consumeMany(new ArrayList<>(Arrays.asList(skvi, copy1, copy2)),
          seekRange, seekColumnFamilies, seekInclusive, iteratorEnvironment);

      return new IteratorTestOutput(output);
    } catch (IOException e) {
      return new IteratorTestOutput(e);
    }
  }

  TreeMap<Key,Value> consumeMany(Collection<SortedKeyValueIterator<Key,Value>> iterators,
      Range range, Collection<ByteSequence> seekColumnFamilies, boolean seekInclusive,
      IteratorEnvironment iterEnv) throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    // All of the copies should have consistent results from concurrent use
    while (allHasTop(iterators)) {
      // occasionally deep copy one of the existing iterators
      if (random.nextInt(3) == 0) {
        log.debug("Deep-copying and re-seeking an iterator");
        SortedKeyValueIterator<Key,Value> newcopy = getRandomElement(iterators).deepCopy(iterEnv);
        newcopy.seek(
            new Range(getTopKey(iterators), true, range.getEndKey(), range.isEndKeyInclusive()),
            seekColumnFamilies, seekInclusive);
        // keep using the new one too, should act like the others
        iterators.add(newcopy);
      }

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

  private <E> E getRandomElement(Collection<E> iterators) {
    if (iterators == null || iterators.isEmpty()) {
      throw new IllegalArgumentException("should not pass an empty collection");
    }
    int num = random.nextInt(iterators.size());
    for (E e : iterators) {
      if (num-- == 0) {
        return e;
      }
    }
    throw new AssertionError();
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
        throw new IllegalStateException(
            "Inconsistent keys between two iterators: " + topKey + " " + iter.getTopKey());
      }
    }

    // Copy the key
    return new Key(topKey);
  }

  Value getTopValue(Collection<SortedKeyValueIterator<Key,Value>> iterators) {
    boolean first = true;
    Value topValue = null;
    for (SortedKeyValueIterator<Key,Value> iter : iterators) {
      if (first) {
        topValue = iter.getTopValue();
        first = false;
      } else if (!topValue.equals(iter.getTopValue())) {
        throw new IllegalStateException(
            "Inconsistent values between two iterators: " + topValue + " " + iter.getTopValue());
      }
    }

    // Copy the value
    if (topValue == null) {
      throw new IllegalStateException(
          "Should always find a non-null Value from the iterator being tested.");
    }
    return new Value(topValue);
  }

  void next(Collection<SortedKeyValueIterator<Key,Value>> iterators) throws IOException {
    for (SortedKeyValueIterator<Key,Value> iter : iterators) {
      iter.next();
    }
  }
}
