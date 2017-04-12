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
package org.apache.accumulo.iteratortest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A runner for invoking some tests over some input and expecting some output.
 */
public class IteratorTestRunner {
  private static final Logger log = LoggerFactory.getLogger(IteratorTestRunner.class);

  private final IteratorTestInput testInput;
  private final IteratorTestOutput testOutput;
  private final Collection<IteratorTestCase> testCases;

  /**
   * Construct an instance of the class.
   *
   * @param testInput
   *          The input to the tests
   * @param testOutput
   *          The expected output given the input
   * @param testCases
   *          The test cases to invoke
   */
  public IteratorTestRunner(IteratorTestInput testInput, IteratorTestOutput testOutput, Collection<IteratorTestCase> testCases) {
    this.testInput = testInput;
    this.testOutput = testOutput;
    this.testCases = testCases;
  }

  public IteratorTestInput getTestInput() {
    return testInput;
  }

  public IteratorTestOutput getTestOutput() {
    return testOutput;
  }

  public Collection<IteratorTestCase> getTestCases() {
    return testCases;
  }

  /**
   * Invokes each test case on the input, verifying the output.
   *
   * @return true if all tests passed, false
   */
  public List<IteratorTestReport> runTests() {
    List<IteratorTestReport> testReports = new ArrayList<>(testCases.size());
    for (IteratorTestCase testCase : testCases) {
      log.info("Invoking {} on {}", testCase.getClass().getName(), testInput.getIteratorClass().getName());

      IteratorTestOutput actualOutput;

      try {
        actualOutput = testCase.test(testInput);
      } catch (Exception e) {
        log.error("Failed to invoke {} on {}", testCase.getClass().getName(), testInput.getIteratorClass().getName(), e);
        actualOutput = new IteratorTestOutput(e);
      }

      // Sanity-check on the IteratorTestCase implementation.
      if (null == actualOutput) {
        throw new IllegalStateException("IteratorTestCase implementations should always return a non-null IteratorTestOutput. " + testCase.getClass().getName()
            + " did not!");
      }

      testReports.add(new IteratorTestReport(testInput, testOutput, actualOutput, testCase));
    }

    return testReports;
  }
}
