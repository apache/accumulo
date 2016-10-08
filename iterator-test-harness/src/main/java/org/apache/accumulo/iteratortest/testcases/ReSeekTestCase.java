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
import java.util.Collection;
import java.util.Random;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.IteratorTestUtil;
import org.apache.accumulo.iteratortest.environments.SimpleIteratorEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test case that verifies that an iterator can use the generated instance from {@code deepCopy}.
 */
public class ReSeekTestCase extends OutputVerifyingTestCase {
  private static final Logger log = LoggerFactory.getLogger(ReSeekTestCase.class);

  /**
   * Let N be a random number between [0, RESEEK_INTERVAL). After every Nth entry "returned" to the client, recreate and reseek the iterator.
   */
  private static final int RESEEK_INTERVAL = 4;

  private final Random random;

  public ReSeekTestCase() {
    this.random = new Random();
  }

  @Override
  public IteratorTestOutput test(IteratorTestInput testInput) {
    final SortedKeyValueIterator<Key,Value> skvi = IteratorTestUtil.instantiateIterator(testInput);
    final SortedKeyValueIterator<Key,Value> source = IteratorTestUtil.createSource(testInput);

    try {
      skvi.init(source, testInput.getIteratorOptions(), new SimpleIteratorEnvironment());
      skvi.seek(testInput.getRange(), testInput.getFamilies(), testInput.isInclusive());
      return new IteratorTestOutput(consume(skvi, testInput));
    } catch (IOException e) {
      return new IteratorTestOutput(e);
    }
  }

  TreeMap<Key,Value> consume(SortedKeyValueIterator<Key,Value> skvi, IteratorTestInput testInput) throws IOException {
    final TreeMap<Key,Value> data = new TreeMap<>();
    final Range origRange = testInput.getRange();
    final Collection<ByteSequence> origFamilies = testInput.getFamilies();
    final boolean origInclusive = testInput.isInclusive();
    int reseekCount = random.nextInt(RESEEK_INTERVAL);

    int i = 0;
    while (skvi.hasTop()) {
      data.put(new Key(skvi.getTopKey()), new Value(skvi.getTopValue()));

      /*
       * One of the trickiest cases in writing iterators: After any result is returned from a TabletServer to the client, the Iterator in the TabletServer's
       * memory may be torn down. To preserve the state and guarantee that all records are received, the TabletServer does remember the last Key it returned to
       * the client. It will recreate the Iterator (stack), and seek it using an updated Range. This range's start key is set to the last Key returned,
       * non-inclusive.
       */
      if (i % RESEEK_INTERVAL == reseekCount) {
        // Last key
        Key reSeekStartKey = skvi.getTopKey();

        // Make a new instance of the iterator
        skvi = IteratorTestUtil.instantiateIterator(testInput);
        final SortedKeyValueIterator<Key,Value> sourceCopy = IteratorTestUtil.createSource(testInput);

        skvi.init(sourceCopy, testInput.getIteratorOptions(), new SimpleIteratorEnvironment());

        // The new range, resume where we left off (non-inclusive), with same families filter
        final Range newRange = new Range(reSeekStartKey, false, origRange.getEndKey(), origRange.isEndKeyInclusive());
        log.debug("Re-seeking to {}", newRange);

        // Seek there
        skvi.seek(newRange, origFamilies, origInclusive);
      } else {
        // Every other time, it's a simple call to next()
        skvi.next();
      }

      i++;
    }

    return data;
  }

}
