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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.iteratortest.IteratorTestCaseFinder;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.junit4.BaseJUnit4IteratorTest;
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;
import org.apache.hadoop.io.Text;
import org.junit.runners.Parameterized.Parameters;

/**
 * Framework tests for {@link WholeRowIterator}.
 */
public class WholeRowIteratorTest extends BaseJUnit4IteratorTest {

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

    data.put(new Key("1", "", "a"), new Value("1a".getBytes()));
    data.put(new Key("1", "", "b"), new Value("1b".getBytes()));
    data.put(new Key("1", "a", "a"), new Value("1aa".getBytes()));
    data.put(new Key("1", "a", "b"), new Value("1ab".getBytes()));
    data.put(new Key("1", "b", "a"), new Value("1ba".getBytes()));

    data.put(new Key("2", "a", "a"), new Value("2aa".getBytes()));
    data.put(new Key("2", "a", "b"), new Value("2ab".getBytes()));
    data.put(new Key("2", "a", "c"), new Value("2ac".getBytes()));
    data.put(new Key("2", "c", "c"), new Value("2cc".getBytes()));

    data.put(new Key("3", "a", ""), new Value("3a".getBytes()));

    data.put(new Key("4", "a", "b"), new Value("4ab".getBytes()));

    data.put(new Key("5", "a", "a"), new Value("5aa".getBytes()));
    data.put(new Key("5", "a", "b"), new Value("5ab".getBytes()));
    data.put(new Key("5", "a", "c"), new Value("5ac".getBytes()));
    data.put(new Key("5", "a", "d"), new Value("5ad".getBytes()));

    data.put(new Key("6", "", "a"), new Value("6a".getBytes()));
    data.put(new Key("6", "", "b"), new Value("6b".getBytes()));
    data.put(new Key("6", "", "c"), new Value("6c".getBytes()));
    data.put(new Key("6", "", "d"), new Value("6d".getBytes()));
    data.put(new Key("6", "", "e"), new Value("6e".getBytes()));
    data.put(new Key("6", "1", "a"), new Value("61a".getBytes()));
    data.put(new Key("6", "1", "b"), new Value("61b".getBytes()));
    data.put(new Key("6", "1", "c"), new Value("61c".getBytes()));
    data.put(new Key("6", "1", "d"), new Value("61d".getBytes()));
    data.put(new Key("6", "1", "e"), new Value("61e".getBytes()));

    return data;
  }

  private static TreeMap<Key,Value> createOutputData() {
    TreeMap<Key,Value> data = new TreeMap<>();

    Text row = null;
    List<Key> keys = new ArrayList<>();
    List<Value> values = new ArrayList<>();

    // Generate the output data from the input data
    for (Entry<Key,Value> entry : INPUT_DATA.entrySet()) {
      if (null == row) {
        row = entry.getKey().getRow();
      }

      if (!row.equals(entry.getKey().getRow())) {
        // Moved to the next row
        try {
          // Serialize and save
          Value encoded = WholeRowIterator.encodeRow(keys, values);
          data.put(new Key(row), encoded);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        // Empty the aggregated k-v's
        keys = new ArrayList<>();
        values = new ArrayList<>();
        // Set the new current row
        row = entry.getKey().getRow();
      }

      // Aggregate the current row
      keys.add(entry.getKey());
      values.add(entry.getValue());
    }

    if (!keys.isEmpty()) {
      try {
        Value encoded = WholeRowIterator.encodeRow(keys, values);
        data.put(new Key(row), encoded);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    return data;
  }

  private static IteratorTestInput getIteratorInput() {
    return new IteratorTestInput(WholeRowIterator.class, Collections.<String,String> emptyMap(), new Range(), INPUT_DATA);
  }

  private static IteratorTestOutput getIteratorOutput() {
    return new IteratorTestOutput(OUTPUT_DATA);
  }

  public WholeRowIteratorTest(IteratorTestInput input, IteratorTestOutput expectedOutput, IteratorTestCase testCase) {
    super(input, expectedOutput, testCase);
  }

}
