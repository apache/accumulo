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
package org.apache.accumulo.iteratortest.framework;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.IteratorTestOutput.TestOutcome;
import org.apache.accumulo.iteratortest.junit4.BaseJUnit4IteratorTest;
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;
import org.junit.runners.Parameterized.Parameters;

/**
 * A Basic test asserting that the framework is functional.
 */
public class JUnitFrameworkTest extends BaseJUnit4IteratorTest {

  /**
   * An IteratorTestCase implementation that returns the original input without any external action.
   */
  public static class NoopIteratorTestCase implements IteratorTestCase {

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

  @Parameters
  public static Object[][] parameters() {
    IteratorTestInput input = getIteratorInput();
    IteratorTestOutput output = getIteratorOutput();
    List<IteratorTestCase> tests = Collections.<IteratorTestCase> singletonList(new NoopIteratorTestCase());
    return BaseJUnit4IteratorTest.createParameters(input, output, tests);
  }

  private static final TreeMap<Key,Value> DATA = createData();

  private static TreeMap<Key,Value> createData() {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("1", "a", ""), new Value("1a".getBytes()));
    data.put(new Key("2", "a", ""), new Value("2a".getBytes()));
    data.put(new Key("3", "a", ""), new Value("3a".getBytes()));
    return data;
  }

  private static IteratorTestInput getIteratorInput() {
    return new IteratorTestInput(IdentityIterator.class, Collections.<String,String> emptyMap(), new Range(), DATA);
  }

  private static IteratorTestOutput getIteratorOutput() {
    return new IteratorTestOutput(DATA);
  }

  public JUnitFrameworkTest(IteratorTestInput input, IteratorTestOutput expectedOutput, IteratorTestCase testCase) {
    super(input, expectedOutput, testCase);
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
}
