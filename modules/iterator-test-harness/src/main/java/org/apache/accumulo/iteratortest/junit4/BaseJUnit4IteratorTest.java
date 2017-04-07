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
package org.apache.accumulo.iteratortest.junit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.IteratorTestReport;
import org.apache.accumulo.iteratortest.IteratorTestRunner;
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base JUnit4 test class for users to leverage with the JUnit Parameterized Runner.
 * <p>
 * Users should extend this class and implement a static method using the {@code @Parameters} annotation.
 *
 * <pre>
 * &#064;Parameters
 * public static Object[][] data() {
 *   IteratorTestInput input = createIteratorInput();
 *   IteratorTestOutput expectedOutput = createIteratorOuput();
 *   List&lt;IteratorTestCase&gt; testCases = createTestCases();
 *   return BaseJUnit4IteratorTest.createParameters(input, expectedOutput, testCases);
 * }
 * </pre>
 *
 */
@RunWith(Parameterized.class)
public class BaseJUnit4IteratorTest {
  private static final Logger log = LoggerFactory.getLogger(BaseJUnit4IteratorTest.class);

  public final IteratorTestRunner runner;

  public BaseJUnit4IteratorTest(IteratorTestInput input, IteratorTestOutput expectedOutput, IteratorTestCase testCase) {
    this.runner = new IteratorTestRunner(input, expectedOutput, Collections.singleton(testCase));
  }

  /**
   * A helper function to convert input, output and a list of test cases into a two-dimensional array for JUnit's Parameterized runner.
   *
   * @param input
   *          The input
   * @param output
   *          The output
   * @param testCases
   *          A list of desired test cases to run.
   * @return A two dimensional array suitable to pass as JUnit's parameters.
   */
  public static Object[][] createParameters(IteratorTestInput input, IteratorTestOutput output, Collection<IteratorTestCase> testCases) {
    Object[][] parameters = new Object[testCases.size()][3];
    Iterator<IteratorTestCase> testCaseIter = testCases.iterator();
    for (int i = 0; testCaseIter.hasNext(); i++) {
      final IteratorTestCase testCase = testCaseIter.next();
      parameters[i] = new Object[] {input, output, testCase};
    }
    return parameters;
  }

  @Test
  public void testIterator() {
    List<IteratorTestReport> reports = runner.runTests();
    assertEquals(1, reports.size());

    IteratorTestReport report = reports.get(0);
    assertNotNull(report);

    assertTrue(report.getSummary(), report.didTestSucceed());

    // Present for manual verification
    log.trace("Expected: {}, Actual: {}", report.getExpectedOutput(), report.getActualOutput());
  }
}
