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
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.IteratorTestUtil;
import org.apache.accumulo.iteratortest.environments.SimpleIteratorEnvironment;

/**
 * Test case that verifies that an iterator can use the generated instance from {@code deepCopy}.
 */
public class DeepCopyTestCase extends OutputVerifyingTestCase {

  @Override
  public IteratorTestOutput test(IteratorTestInput testInput) {
    final SortedKeyValueIterator<Key,Value> skvi = IteratorTestUtil.instantiateIterator(testInput);
    final SortedKeyValueIterator<Key,Value> source = IteratorTestUtil.createSource(testInput);

    try {
      skvi.init(source, testInput.getIteratorOptions(), new SimpleIteratorEnvironment());

      SortedKeyValueIterator<Key,Value> copy = skvi.deepCopy(new SimpleIteratorEnvironment());

      copy.seek(testInput.getRange(), testInput.getFamilies(), testInput.isInclusive());
      return new IteratorTestOutput(consume(copy));
    } catch (IOException e) {
      return new IteratorTestOutput(e);
    }
  }

  TreeMap<Key,Value> consume(SortedKeyValueIterator<Key,Value> skvi) throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    while (skvi.hasTop()) {
      // Make sure to copy the K-V
      data.put(new Key(skvi.getTopKey()), new Value(skvi.getTopValue()));
      skvi.next();
    }
    return data;
  }

}
