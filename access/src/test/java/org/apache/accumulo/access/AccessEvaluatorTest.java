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
package org.apache.accumulo.access;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.access.AccessExpression.quote;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class AccessEvaluatorTest {

  enum ExpectedResult {
    ACCESSIBLE, INACCESSIBLE, ERROR
  }

  public static class TestExpressions {
    ExpectedResult expectedResult;
    String[] expressions;
  }

  public static class TestDataSet {
    String description;

    String[][] auths;

    List<TestExpressions> tests;

  }

  private List<TestDataSet> readTestData() throws IOException {
    try (var input = getClass().getClassLoader().getResourceAsStream("testdata.json")) {
      if (input == null) {
        throw new IllegalStateException("could not find resource : testdata.json");
      }
      var json = new String(input.readAllBytes(), UTF_8);

      Type listType = new TypeToken<ArrayList<TestDataSet>>() {}.getType();
      return new Gson().fromJson(json, listType);
    }
  }

  @SuppressFBWarnings(value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD"},
      justification = "Field is written by Gson")
  @Test
  public void runTestCases() throws IOException {
    List<TestDataSet> testData = readTestData();

    assertFalse(testData.isEmpty());

    for (var testSet : testData) {
      AccessEvaluator evaluator;
      assertTrue(testSet.auths.length >= 1);
      if (testSet.auths.length == 1) {
        evaluator = AccessEvaluator.builder().authorizations(testSet.auths[0]).build();
        runTestCases(testSet, evaluator);

        evaluator = AccessEvaluator.builder().authorizations(testSet.auths[0]).cacheSize(1).build();
        runTestCases(testSet, evaluator);

        evaluator =
            AccessEvaluator.builder().authorizations(testSet.auths[0]).cacheSize(10).build();
        runTestCases(testSet, evaluator);

        Set<String> auths = Stream.of(testSet.auths[0]).collect(Collectors.toSet());
        evaluator = AccessEvaluator.builder().authorizations(auths::contains).build();
        runTestCases(testSet, evaluator);
      } else {
        var authSets =
            Stream.of(testSet.auths).map(Authorizations::of).collect(Collectors.toList());
        evaluator = AccessEvaluator.builder().authorizations(authSets).build();
        runTestCases(testSet, evaluator);
      }
    }

  }

  @SuppressFBWarnings(value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD"},
      justification = "Field is written by Gson")
  private static void runTestCases(TestDataSet testSet, AccessEvaluator evaluator) {

    assertFalse(testSet.tests.isEmpty());

    for (var tests : testSet.tests) {

      assertTrue(tests.expressions.length > 0);

      for (var expression : tests.expressions) {
        switch (tests.expectedResult) {
          case ACCESSIBLE:
            assertTrue(evaluator.canAccess(expression), expression);
            assertTrue(evaluator.canAccess(expression.getBytes(UTF_8)), expression);
            assertTrue(evaluator.canAccess(AccessExpression.of(expression)), expression);
            assertTrue(evaluator.canAccess(AccessExpression.of(expression).normalize()),
                expression);
            break;
          case INACCESSIBLE:
            assertFalse(evaluator.canAccess(expression), expression);
            assertFalse(evaluator.canAccess(expression.getBytes(UTF_8)), expression);
            assertFalse(evaluator.canAccess(AccessExpression.of(expression)), expression);
            assertFalse(evaluator.canAccess(AccessExpression.of(expression).normalize()),
                expression);
            break;
          case ERROR:
            assertThrows(IllegalAccessExpressionException.class,
                () -> evaluator.canAccess(expression), expression);
            assertThrows(IllegalAccessExpressionException.class,
                () -> evaluator.canAccess(expression.getBytes(UTF_8)), expression);
            assertThrows(IllegalAccessExpressionException.class,
                () -> evaluator.canAccess(AccessExpression.of(expression)), expression);
            break;
          default:
            throw new IllegalArgumentException();
        }
      }
    }
  }

  @Test
  public void testSpecialChars() {
    // special chars do not need quoting
    for (String qt : List.of("A_", "_", "A_C", "_C")) {
      assertEquals(qt, quote(qt));
      for (char c : new char[] {'/', ':', '-', '.'}) {
        String qt2 = qt.replace('_', c);
        assertEquals(qt2, quote(qt2));
      }
    }

    assertEquals("a_b:c/d.e", quote("a_b:c/d.e"));
  }

  @Test
  public void testQuote() {
    assertEquals("\"A#C\"", quote("A#C"));
    assertEquals("\"A\\\"C\"", quote("A\"C"));
    assertEquals("\"A\\\"\\\\C\"", quote("A\"\\C"));
    assertEquals("ACS", quote("ACS"));
    assertEquals("\"九\"", quote("九"));
    assertEquals("\"五十\"", quote("五十"));
  }

  private static String unescape(String s) {
    return AccessEvaluatorImpl.unescape(new BytesWrapper(s.getBytes(UTF_8)));
  }

  @Test
  public void testUnescape() {
    assertEquals("a\"b", unescape("a\\\"b"));
    assertEquals("a\\b", unescape("a\\\\b"));
    assertEquals("a\\\"b", unescape("a\\\\\\\"b"));
    assertEquals("\\\"", unescape("\\\\\\\""));
    assertEquals("a\\b\\c\\d", unescape("a\\\\b\\\\c\\\\d"));

    final String message = "Expected failure to unescape invalid escape sequence";
    final var invalidEscapeSeqList = List.of("a\\b", "a\\b\\c", "a\"b\\");

    invalidEscapeSeqList
        .forEach(seq -> assertThrows(IllegalArgumentException.class, () -> unescape(seq), message));
  }

  // TODO need to copy all test from Accumulo
}
