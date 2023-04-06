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
package org.apache.accumulo.iteratortest.framework;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.iteratortest.IteratorTestBase;
import org.apache.accumulo.iteratortest.IteratorTestCase;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.IteratorTestOutput.TestOutcome;
import org.apache.accumulo.iteratortest.IteratorTestParameters;

/**
 * A Basic test asserting that the framework is functional.
 */
public class JUnitFrameworkTest extends IteratorTestBase {

  @Override
  protected Stream<IteratorTestParameters> parameters() {
    var data = createData();
    var input = new IteratorTestInput(IdentityIterator.class, Map.of(), new Range(), data);
    var expectedOutput = new IteratorTestOutput(data);
    return Stream.of(new NoopIteratorTestCase().toParameters(input, expectedOutput));
  }

  private static TreeMap<Key,Value> createData() {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("1", "a", ""), new Value("1a"));
    data.put(new Key("2", "a", ""), new Value("2a"));
    data.put(new Key("3", "a", ""), new Value("3a"));
    return data;
  }

  /**
   * Noop iterator implementation.
   */
  private static class IdentityIterator extends WrappingIterator {

    @Override
    public IdentityIterator deepCopy(IteratorEnvironment env) {
      return new IdentityIterator();
    }
  }

  /**
   * An IteratorTestCase implementation that returns the original input without any external action.
   */
  private static class NoopIteratorTestCase implements IteratorTestCase {

    @Override
    public IteratorTestOutput test(IteratorTestInput testInput) {
      return new IteratorTestOutput(TestOutcome.PASSED);
    }

    @Override
    public boolean verify(IteratorTestOutput expected, IteratorTestOutput actual) {
      // Always passes
      return true;
    }

  }

}
