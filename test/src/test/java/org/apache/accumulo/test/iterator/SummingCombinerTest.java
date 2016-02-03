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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.iteratortest.IteratorTestCaseFinder;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.junit4.BaseJUnit4IteratorTest;
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;
import org.junit.runners.Parameterized.Parameters;

/**
 * Iterator test harness tests for SummingCombiner
 */
public class SummingCombinerTest extends BaseJUnit4IteratorTest {

  @Parameters
  public static Object[][] parameters() {
    IteratorTestInput input = getIteratorInput();
    IteratorTestOutput output = getIteratorOutput();
    List<IteratorTestCase> tests = IteratorTestCaseFinder.findAllTestCases();
    return BaseJUnit4IteratorTest.createParameters(input, output, tests);
  }

  private static final TreeMap<Key,Value> INPUT_DATA = createInputData();
  private static final TreeMap<Key,Value> OUTPUT_DATA = createOutputData();

  private static TreeMap<Key,Value> createInputData() {
    TreeMap<Key,Value> data = new TreeMap<>();

    // 3
    data.put(new Key("1", "a", "a", 1), new Value(bytes("1")));
    data.put(new Key("1", "a", "a", 5), new Value(bytes("1")));
    data.put(new Key("1", "a", "a", 10), new Value(bytes("1")));
    // 7
    data.put(new Key("1", "a", "b", 1), new Value(bytes("5")));
    data.put(new Key("1", "a", "b", 5), new Value(bytes("2")));
    // 0
    data.put(new Key("1", "a", "f", 1), new Value(bytes("0")));
    // -10
    data.put(new Key("1", "a", "g", 5), new Value(bytes("1")));
    data.put(new Key("1", "a", "g", 10), new Value(bytes("-11")));
    // -5
    data.put(new Key("1", "b", "d", 10), new Value(bytes("-5")));
    // MAX_VALUE
    data.put(new Key("1", "b", "e", 10), new Value(bytes(Long.toString(Long.MAX_VALUE))));
    // MIN_VALUE
    data.put(new Key("1", "d", "d", 10), new Value(bytes(Long.toString(Long.MIN_VALUE))));
    // 30
    data.put(new Key("2", "a", "a", 1), new Value(bytes("5")));
    data.put(new Key("2", "a", "a", 5), new Value(bytes("10")));
    data.put(new Key("2", "a", "a", 10), new Value(bytes("15")));

    return data;
  }

  private static final byte[] bytes(String value) {
    return requireNonNull(value).getBytes(UTF_8);
  }

  private static TreeMap<Key,Value> createOutputData() {
    TreeMap<Key,Value> data = new TreeMap<>();

    Key lastKey = null;
    long sum = 0;
    for (Entry<Key,Value> entry : INPUT_DATA.entrySet()) {
      if (null == lastKey) {
        lastKey = entry.getKey();
        sum += Long.parseLong(entry.getValue().toString());
      } else {
        if (0 != lastKey.compareTo(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
          // Different key, store the running sum.
          data.put(lastKey, new Value(Long.toString(sum).getBytes(UTF_8)));
          // Reset lastKey and the sum
          lastKey = entry.getKey();
          sum = 0;
        }

        sum += Long.parseLong(entry.getValue().toString());
      }
    }

    data.put(lastKey, new Value(Long.toString(sum).getBytes(UTF_8)));

    return data;
  }

  private static IteratorTestInput getIteratorInput() {
    IteratorSetting setting = new IteratorSetting(50, SummingCombiner.class);
    LongCombiner.setEncodingType(setting, LongCombiner.Type.STRING);
    Combiner.setCombineAllColumns(setting, true);
    return new IteratorTestInput(SummingCombiner.class, setting.getOptions(), new Range(), INPUT_DATA);
  }

  private static IteratorTestOutput getIteratorOutput() {
    return new IteratorTestOutput(OUTPUT_DATA);
  }

  public SummingCombinerTest(IteratorTestInput input, IteratorTestOutput expectedOutput, IteratorTestCase testCase) {
    super(input, expectedOutput, testCase);
  }

}
