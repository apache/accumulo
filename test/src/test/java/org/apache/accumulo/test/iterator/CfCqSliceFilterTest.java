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
package org.apache.accumulo.test.iterator;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.CfCqSliceFilter;
import org.apache.accumulo.core.iterators.user.CfCqSliceOpts;
import org.apache.accumulo.iteratortest.IteratorTestBase;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.IteratorTestParameters;

/**
 * Iterator test harness tests for CfCqSliceFilter
 */
public class CfCqSliceFilterTest extends IteratorTestBase {

  // Default is inclusive on min and max
  private static final String MIN_CF = "f";
  private static final String MAX_CF = "m";
  private static final String MIN_CQ = "q";
  private static final String MAX_CQ = "y";

  private static final TreeMap<Key,Value> INPUT_DATA = createInputData();
  private static final TreeMap<Key,Value> OUTPUT_DATA = createOutputData();

  @Override
  protected Stream<IteratorTestParameters> parameters() {
    var input = new IteratorTestInput(CfCqSliceFilter.class, createOpts(), new Range(), INPUT_DATA);
    var expectedOutput = new IteratorTestOutput(OUTPUT_DATA);
    return builtinTestCases().map(test -> test.toParameters(input, expectedOutput));
  }

  private static Map<String,String> createOpts() {
    HashMap<String,String> options = new HashMap<>();
    options.put(CfCqSliceOpts.OPT_MIN_CF, MIN_CF);
    options.put(CfCqSliceOpts.OPT_MAX_CF, MAX_CF);
    options.put(CfCqSliceOpts.OPT_MIN_CQ, MIN_CQ);
    options.put(CfCqSliceOpts.OPT_MAX_CQ, MAX_CQ);
    return options;
  }

  private static TreeMap<Key,Value> createInputData() {
    TreeMap<Key,Value> data = new TreeMap<>();
    Value value = new Value("a");

    // Dropped
    data.put(new Key("1", "a", "g"), value);
    data.put(new Key("1", "f", "q"), value);
    data.put(new Key("1", "f", "t"), value);
    data.put(new Key("1", "g", "q"), value);
    data.put(new Key("1", "g", "y"), value);
    // Dropped
    data.put(new Key("1", "g", "z"), value);

    // Dropped
    data.put(new Key("2", "m", "a"), value);

    data.put(new Key("3", "j", "u"), value);

    data.put(new Key("4", "h", "w"), value);
    data.put(new Key("4", "h", "x"), value);
    data.put(new Key("4", "h", "y"), value);
    data.put(new Key("4", "l", "r"), value);
    // Dropped
    data.put(new Key("4", "l", "z"), value);
    data.put(new Key("4", "m", "y"), value);

    return data;
  }

  private static TreeMap<Key,Value> createOutputData() {
    TreeMap<Key,Value> data = new TreeMap<>(INPUT_DATA);
    data.entrySet().removeIf(entry -> {
      String cf = entry.getKey().getColumnFamily().toString();
      String cq = entry.getKey().getColumnQualifier().toString();
      return MIN_CF.compareTo(cf) > 0 || MAX_CF.compareTo(cf) < 0 || MIN_CQ.compareTo(cq) > 0
          || MAX_CQ.compareTo(cq) < 0;
    });
    return data;
  }
}
