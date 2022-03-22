/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.iteratortest.junit5;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Stream;

import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.IteratorTestReport;
import org.apache.accumulo.iteratortest.IteratorTestRunner;
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base JUnit5 test class for users to leverage with the JUnit ParameterizedTest.
 * <p>
 * Users should extend this class and override its abstract methods to provide IteratorTestInput,
 * IteratorTestOutput and a List&lt;IteratorTestCase&gt; which will all serve as the parameters for
 * an iterator test case.
 *
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseJUnit5IteratorTest {

  private static final Logger log = LoggerFactory.getLogger(BaseJUnit5IteratorTest.class);

  /**
   * When the ParameterizedTest, BaseJUnit5IteratorTest.testIterator is run, it will use the
   * returned Stream&lt;IteratorTestParameters&gt; from this method as its parameters. Each
   * {@link IteratorTestParameters} in the returned list will essentially serve as parameters for
   * its own test case.
   *
   * @return A List of Arguments suitable to pass as JUnit's parameters.
   */
  private Stream<IteratorTestParameters> parameters() {
    IteratorTestInput input = getIteratorInput();
    IteratorTestOutput output = getIteratorOutput();
    List<IteratorTestCase> testCases = getIteratorTestCases();

    return testCases.stream().map(testCase -> new IteratorTestParameters(input, output, testCase));
  }

  /**
   * The {@link IteratorTestInput} to use for each IteratorTestCase
   */
  protected abstract IteratorTestInput getIteratorInput();

  /**
   * The {@link IteratorTestOutput} to use for each IteratorTestCase
   */
  protected abstract IteratorTestOutput getIteratorOutput();

  /**
   * The List of {@link IteratorTestCase} to run the iterator against
   */
  protected abstract List<IteratorTestCase> getIteratorTestCases();

  @ParameterizedTest(name = "{index}")
  @MethodSource("parameters")
  public void testIterator(IteratorTestParameters params) {
    IteratorTestRunner runner =
        new IteratorTestRunner(params.input, params.expectedOutput, List.of(params.testCase));
    List<IteratorTestReport> reports = runner.runTests();
    assertEquals(1, reports.size());

    IteratorTestReport report = reports.get(0);
    assertNotNull(report);

    // Present for manual verification
    log.trace("Expected: {}, Actual: {}", report.getExpectedOutput(), report.getActualOutput());

    assertTrue(report.didTestSucceed(), report.getSummary());
  }

  static class IteratorTestParameters {

    public final IteratorTestInput input;
    public final IteratorTestOutput expectedOutput;
    public final IteratorTestCase testCase;

    public IteratorTestParameters(IteratorTestInput input, IteratorTestOutput expectedOutput,
        IteratorTestCase testCase) {
      this.input = input;
      this.expectedOutput = expectedOutput;
      this.testCase = testCase;
    }

  }
}
