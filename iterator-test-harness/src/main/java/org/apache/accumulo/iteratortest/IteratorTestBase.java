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
package org.apache.accumulo.iteratortest;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.accumulo.iteratortest.testcases.InstantiationTestCase;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

/**
 * A base test class for users to leverage to test their iterators.
 * <p>
 * Users should extend this class and override its abstract methods to provide IteratorTestInput,
 * IteratorTestOutput and a List&lt;IteratorTestCase&gt; which will all serve as the parameters for
 * an iterator test case.
 *
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class IteratorTestBase {

  private static final Logger log = LoggerFactory.getLogger(IteratorTestBase.class);

  protected abstract Stream<IteratorTestParameters> parameters();

  /**
   * The List of {@link IteratorTestCase} to run the iterator against
   */
  protected final Stream<IteratorTestCase> builtinTestCases() {
    String searchPackage = InstantiationTestCase.class.getPackage().getName();
    log.info("Searching {}", searchPackage);
    ClassPath cp;
    try {
      cp = ClassPath.from(getClass().getClassLoader());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    Set<ClassInfo> classes = cp.getTopLevelClasses(searchPackage);

    final List<IteratorTestCase> testCases1 = new ArrayList<>();
    for (ClassInfo classInfo : classes) {
      Class<?> clz;
      try {
        clz = Class.forName(classInfo.getName());
      } catch (Exception e) {
        log.warn("Could not get class for " + classInfo.getName(), e);
        continue;
      }

      if (clz.isInterface() || Modifier.isAbstract(clz.getModifiers())
          || !IteratorTestCase.class.isAssignableFrom(clz)) {
        log.trace("Skipping " + clz);
        continue;
      }

      try {
        testCases1.add((IteratorTestCase) clz.getDeclaredConstructor().newInstance());
      } catch (ReflectiveOperationException e) {
        log.warn("Could not instantiate {}", clz, e);
      }
    }

    return testCases1.stream();
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void testIterator(IteratorTestParameters params) {
    var testCase = params.testCase;
    var expectedOutput = params.expectedOutput;

    log.info("Invoking {} on {}", testCase.getClass().getName(),
        params.input.getIteratorClass().getName());

    IteratorTestOutput actualOutput;

    try {
      actualOutput = testCase.test(params.input);
    } catch (Exception e) {
      log.error("Failed to invoke {} on {}", testCase.getClass().getName(),
          params.input.getIteratorClass().getName(), e);
      actualOutput = new IteratorTestOutput(e);
    }

    assertNotNull(actualOutput,
        "IteratorTestCase implementations should always return a non-null IteratorTestOutput. "
            + testCase.getClass().getName() + " did not!");

    // Present for manual verification
    log.trace("Expected: {}, Actual: {}", expectedOutput, actualOutput);

    final var finalOutput = actualOutput;
    assertTrue(testCase.verify(expectedOutput, actualOutput), () -> {
      StringBuilder sb = new StringBuilder(64);
      // @formatter:off
      sb.append("IteratorTestReport Summary: \n")
          .append("\tTest Case = ").append(params.testCase.getClass().getName())
          .append("\tInput Data = '").append(params.input).append("'\n")
          .append("\tExpected Output = '").append(params.expectedOutput).append("'\n")
          .append("\tActual Output = '").append(finalOutput).append("'\n");
      // @formatter:on
      return sb.toString();
    });
  }

}
