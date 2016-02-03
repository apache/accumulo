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

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;

/**
 * A summary of the invocation of an {@link IteratorTestInput} over a {@link IteratorTestCase} with the expected {@link IteratorTestOutput}.
 */
public class IteratorTestReport {

  private final IteratorTestInput input;
  private final IteratorTestOutput expectedOutput;
  private final IteratorTestCase testCase;
  private final IteratorTestOutput actualOutput;

  public IteratorTestReport(IteratorTestInput input, IteratorTestOutput expectedOutput, IteratorTestOutput actualOutput, IteratorTestCase testCase) {
    this.input = requireNonNull(input);
    this.expectedOutput = requireNonNull(expectedOutput);
    this.testCase = requireNonNull(testCase);
    this.actualOutput = requireNonNull(actualOutput);
  }

  public IteratorTestInput getInput() {
    return input;
  }

  public IteratorTestOutput getExpectedOutput() {
    return expectedOutput;
  }

  public IteratorTestCase getTestCase() {
    return testCase;
  }

  public IteratorTestOutput getActualOutput() {
    return actualOutput;
  }

  /**
   * Evaluate whether the test passed or failed.
   *
   * @return True if the actual output matches the expected output, false otherwise.
   */
  public boolean didTestSucceed() {
    return testCase.verify(expectedOutput, actualOutput);
  }

  public String getSummary() {
    StringBuilder sb = new StringBuilder(64);
    // @formatter:off
    sb.append("IteratorTestReport Summary: \n")
        .append("\tTest Case = ").append(testCase.getClass().getName())
        .append("\tInput Data = '").append(input).append("'\n")
        .append("\tExpected Output = '").append(expectedOutput).append("'\n")
        .append("\tActual Output = '").append(actualOutput).append("'\n");
    // @formatter:on
    return sb.toString();
  }
}
