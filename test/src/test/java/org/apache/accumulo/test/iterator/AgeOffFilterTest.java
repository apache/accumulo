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
package org.apache.accumulo.test.iterator;

import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.iteratortest.IteratorTestCaseFinder;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.junit4.BaseJUnit4IteratorTest;
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Iterables;

/**
 * Iterator test harness tests for AgeOffFilter
 */
public class AgeOffFilterTest extends BaseJUnit4IteratorTest {
  public static long NOW;
  public static long TTL;

  @Parameters
  public static Object[][] parameters() {
    // Test ageoff after 30 seconds.
    NOW = System.currentTimeMillis();
    TTL = 30 * 1000;

    IteratorTestInput input = getIteratorInput();
    IteratorTestOutput output = getIteratorOutput();
    List<IteratorTestCase> tests = IteratorTestCaseFinder.findAllTestCases();
    return BaseJUnit4IteratorTest.createParameters(input, output, tests);
  }

  private static final TreeMap<Key,Value> INPUT_DATA = createInputData();
  private static final TreeMap<Key,Value> OUTPUT_DATA = createOutputData();

  private static TreeMap<Key,Value> createInputData() {
    TreeMap<Key,Value> data = new TreeMap<>();
    final Value value = new Value(new byte[] {'a'});

    data.put(new Key("1", "a", "a", nowDelta(25)), value);
    data.put(new Key("2", "a", "a", nowDelta(35)), value);
    data.put(new Key("3", "a", "a", nowDelta(55)), value);
    data.put(new Key("4", "a", "a", nowDelta(0)), value);
    data.put(new Key("5", "a", "a", nowDelta(-29)), value);
    data.put(new Key("6", "a", "a", nowDelta(-28)), value);
    // Dropped
    data.put(new Key("7", "a", "a", nowDelta(-40)), value);
    // Dropped (comparison is not inclusive)
    data.put(new Key("8", "a", "a", nowDelta(-30)), value);
    // Dropped
    data.put(new Key("9", "a", "a", nowDelta(-31)), value);

    // Dropped
    data.put(new Key("a", "", "", nowDelta(-50)), value);
    data.put(new Key("a", "a", "", nowDelta(-20)), value);
    data.put(new Key("a", "a", "a", nowDelta(50)), value);
    data.put(new Key("a", "a", "b", nowDelta(-15)), value);
    // Dropped
    data.put(new Key("a", "a", "c", nowDelta(-32)), value);
    // Dropped
    data.put(new Key("a", "a", "d", nowDelta(-32)), value);

    return data;
  }

  /**
   * Compute a timestamp (milliseconds) based on {@link #NOW} plus the <code>seconds</code> argument.
   *
   * @param seconds
   *          The number of seconds to add to <code>NOW</code> .
   * @return A Key timestamp the provided number of seconds after <code>NOW</code>.
   */
  private static long nowDelta(long seconds) {
    return NOW + (seconds * 1000);
  }

  private static TreeMap<Key,Value> createOutputData() {
    TreeMap<Key,Value> data = new TreeMap<>();

    Iterable<Entry<Key,Value>> filtered = Iterables.filter(data.entrySet(), input -> NOW - input.getKey().getTimestamp() > TTL);

    for (Entry<Key,Value> entry : filtered) {
      data.put(entry.getKey(), entry.getValue());
    }

    return data;
  }

  private static IteratorTestInput getIteratorInput() {
    IteratorSetting setting = new IteratorSetting(50, AgeOffFilter.class);
    AgeOffFilter.setCurrentTime(setting, NOW);
    AgeOffFilter.setTTL(setting, TTL);
    return new IteratorTestInput(AgeOffFilter.class, setting.getOptions(), new Range(), INPUT_DATA);
  }

  private static IteratorTestOutput getIteratorOutput() {
    return new IteratorTestOutput(OUTPUT_DATA);
  }

  public AgeOffFilterTest(IteratorTestInput input, IteratorTestOutput expectedOutput, IteratorTestCase testCase) {
    super(input, expectedOutput, testCase);
  }

}
