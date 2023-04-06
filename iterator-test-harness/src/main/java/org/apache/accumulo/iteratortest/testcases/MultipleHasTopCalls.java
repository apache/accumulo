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
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.iteratortest.IteratorTestCase;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;

/**
 * TestCase which asserts that multiple calls to {@link SortedKeyValueIterator#hasTop()} should not
 * alter the internal state of the iterator and should not return different values due to multiple,
 * sequential invocations.
 * <p>
 * This test case will call {@code hasTop()} multiple times, verifying that each call returns the
 * same value as the first.
 */
public class MultipleHasTopCalls implements IteratorTestCase {

  private static final SecureRandom random = new SecureRandom();

  @Override
  public IteratorTestOutput test(IteratorTestInput testInput) {
    final SortedKeyValueIterator<Key,Value> skvi = IteratorTestUtil.instantiateIterator(testInput);
    final SortedKeyValueIterator<Key,Value> source = IteratorTestUtil.createSource(testInput);

    try {
      skvi.init(source, testInput.getIteratorOptions(), testInput.getIteratorEnvironment());
      skvi.seek(testInput.getRange(), testInput.getFamilies(), testInput.isInclusive());
      return new IteratorTestOutput(consume(skvi));
    } catch (IOException e) {
      return new IteratorTestOutput(e);
    }
  }

  TreeMap<Key,Value> consume(SortedKeyValueIterator<Key,Value> skvi) throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    while (skvi.hasTop()) {
      // Check 1 to 5 times. If hasTop returned true, it should continue to return true.
      for (int i = 0; i < random.nextInt(5) + 1; i++) {
        if (!skvi.hasTop()) {
          throw badStateException(true);
        }
      }
      // Make sure to copy the K-V
      data.put(new Key(skvi.getTopKey()), new Value(skvi.getTopValue()));
      skvi.next();
    }

    // Check 1 to 5 times. Once hasTop returned false, it should continue to return false
    for (int i = 0; i < random.nextInt(5) + 1; i++) {
      if (skvi.hasTop()) {
        throw badStateException(false);
      }
    }
    return data;
  }

  IllegalStateException badStateException(boolean expectedState) {
    return new IllegalStateException("Multiple sequential calls to hasTop"
        + " should not alter the state or return value of the iterator. Expected '" + expectedState
        + ", but got '" + !expectedState + "'.");
  }
}
