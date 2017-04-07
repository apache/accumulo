// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

== Iterator Testing

Iterators, while extremely powerful, are notoriously difficult to test. While the API defines
the methods an Iterator must implement and each method's functionality, the actual invocation
of these methods by Accumulo TabletServers can be surprisingly difficult to mimic in unit tests.

The Apache Accumulo "Iterator Test Harness" is designed to provide a generalized testing framework
for all Accumulo Iterators to leverage to identify common pitfalls in user-created Iterators.

=== Framework Use

The harness provides an abstract class for use with JUnit4. Users must define the following for this
abstract class:

  * A `SortedMap` of input data (`Key`-`Value` pairs)
  * A `Range` to use in tests
  * A `Map` of options (`String` to `String` pairs)
  * A `SortedMap` of output data (`Key`-`Value` pairs)
  * A list of `IteratorTestCase`s (these can be automatically discovered)

The majority of effort a user must make is in creating the input dataset and the expected
output dataset for the iterator being tested.

=== Normal Test Outline

Most iterator tests will follow the given outline:

[source,java]
----
import java.util.List;
import java.util.SortedMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.iteratortest.IteratorTestCaseFinder;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.junit4.BaseJUnit4IteratorTest;
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;
import org.junit.runners.Parameterized.Parameters;

public class MyIteratorTest extends BaseJUnit4IteratorTest {

  @Parameters
  public static Object[][] parameters() {
    final IteratorTestInput input = createIteratorInput();
    final IteratorTestOutput output = createIteratorOutput();
    final List<IteratorTestCase> testCases = IteratorTestCaseFinder.findAllTestCases();
    return BaseJUnit4IteratorTest.createParameters(input, output, tests);
  }

  private static SortedMap<Key,Value> INPUT_DATA = createInputData();
  private static SortedMap<Key,Value> OUTPUT_DATA = createOutputData();

  private static SortedMap<Key,Value> createInputData() {
    // TODO -- implement this method
  }

  private static SortedMap<Key,Value> createOutputData() {
    // TODO -- implement this method
  }

  private static IteratorTestInput createIteratorInput() {
    final Map<String,String> options = createIteratorOptions(); 
    final Range range = createRange();
    return new IteratorTestInput(MyIterator.class, options, range, INPUT_DATA);
  }

  private static Map<String,String> createIteratorOptions() {
    // TODO -- implement this method
    // Tip: Use INPUT_DATA if helpful in generating output
  }

  private static Range createRange() {
    // TODO -- implement this method
  }

  private static IteratorTestOutput createIteratorOutput() {
    return new IteratorTestOutput(OUTPUT_DATA);
  }

}
----

=== Limitations

While the provided `IteratorTestCase`s should exercise common edge-cases in user iterators,
there are still many limitations to the existing test harness. Some of them are:

  * Can only specify a single iterator, not many (a "stack")
  * No control over provided IteratorEnvironment for tests
  * Exercising delete keys (especially with major compactions that do not include all files)

These are left as future improvements to the harness.
