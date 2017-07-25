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
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.IteratorTestUtil;
import org.apache.accumulo.iteratortest.environments.SimpleIteratorEnvironment;

/**
 * Test case that verifies that an iterator works correctly with the yielding api. Note that most iterators do nothing in terms of yielding in which case this
 * merely tests that the iterator produces the correct output. If however the iterator does override the yielding api, then this ensures that it works correctly
 * iff the iterator actually decides to yield. Nothing can force an iterator to yield without knowing something about the internals of the iterator being
 * tested.
 */
public class YieldingTestCase extends OutputVerifyingTestCase {

  @Override
  public IteratorTestOutput test(IteratorTestInput testInput) {
    final SortedKeyValueIterator<Key,Value> skvi = IteratorTestUtil.instantiateIterator(testInput);
    final SortedKeyValueIterator<Key,Value> source = IteratorTestUtil.createSource(testInput);

    try {
      skvi.init(source, testInput.getIteratorOptions(), new SimpleIteratorEnvironment());

      YieldCallback<Key> yield = new YieldCallback<>();
      skvi.enableYielding(yield);

      skvi.seek(testInput.getRange(), testInput.getFamilies(), testInput.isInclusive());
      return new IteratorTestOutput(consume(testInput, skvi, yield));
    } catch (IOException e) {
      return new IteratorTestOutput(e);
    }
  }

  TreeMap<Key,Value> consume(IteratorTestInput testInput, SortedKeyValueIterator<Key,Value> skvi, YieldCallback<Key> yield) throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    Key lastKey = null;
    while (yield.hasYielded() || skvi.hasTop()) {
      if (yield.hasYielded()) {
        Range r = testInput.getRange();
        Key yieldPosition = yield.getPositionAndReset();
        if (!r.contains(yieldPosition)) {
          throw new IOException("Underlying iterator yielded to a position outside of its range: " + yieldPosition + " not in " + r);
        }
        if (skvi.hasTop()) {
          throw new IOException("Underlying iterator reports having a top, but has yielded: " + yieldPosition);
        }
        if (lastKey != null && yieldPosition.compareTo(lastKey) <= 0) {
          throw new IOException("Underlying iterator yielded at a position that is not past the last key returned");
        }
        skvi.seek(new Range(yieldPosition, false, r.getEndKey(), r.isEndKeyInclusive()), testInput.getFamilies(), testInput.isInclusive());
      } else {
        // Make sure to copy the K-V
        data.put(new Key(skvi.getTopKey()), new Value(skvi.getTopValue()));
        skvi.next();
      }
    }
    return data;
  }

}
