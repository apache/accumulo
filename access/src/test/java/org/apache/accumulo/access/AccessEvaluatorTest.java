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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class AccessEvaluatorTest {
  @Test
  public void testVisibilityArbiter() {
    for (int cacheSize : List.of(0, 100)) {
      var arbiter =
          AccessEvaluator.builder().authorizations("A1", "Z9").cacheSize(cacheSize).build();
      assertTrue(arbiter.isAccessible("A1"));
      assertTrue(arbiter.isAccessible("Z9"));
      assertTrue(arbiter.isAccessible("A1|G2"));
      assertTrue(arbiter.isAccessible("G2|A1"));
      assertTrue(arbiter.isAccessible("Z9|G2"));
      assertTrue(arbiter.isAccessible("G2|A1"));
      assertTrue(arbiter.isAccessible("Z9|A1"));
      assertTrue(arbiter.isAccessible("A1|Z9"));
      assertTrue(arbiter.isAccessible("(A1|G2)&(Z9|G5)"));

      assertFalse(arbiter.isAccessible("Z8"));
      assertFalse(arbiter.isAccessible("A2"));
      assertFalse(arbiter.isAccessible("A2|Z8"));
      assertFalse(arbiter.isAccessible("A1&Z8"));
      assertFalse(arbiter.isAccessible("Z8&A1"));

      // rerun some of the same labels
      assertTrue(arbiter.isAccessible("Z9|A1"));
      assertTrue(arbiter.isAccessible("(A1|G2)&(Z9|G5)"));
      assertFalse(arbiter.isAccessible("A2|Z8"));
      assertFalse(arbiter.isAccessible("A1&Z8"));
    }
  }

  @Test
  public void testIncorrectExpression() {
    var evaluator = AccessEvaluator.builder().authorizations("A1", "Z9").build();
    assertThrows(IllegalAccessExpressionException.class, () -> evaluator.isAccessible("(A"));
    assertThrows(IllegalAccessExpressionException.class, () -> evaluator.isAccessible("A)"));
    assertThrows(IllegalAccessExpressionException.class, () -> evaluator.isAccessible("((A)"));
    assertThrows(IllegalAccessExpressionException.class, () -> evaluator.isAccessible("A$B"));
    assertThrows(IllegalAccessExpressionException.class,
        () -> evaluator.isAccessible("(A|(B&()))"));
  }

  // copied from VisibilityEvaluatorTest in Accumulo and modified, need to copy more test from that
  // class
  @Test
  public void testVisibilityEvaluator() {
    var ct = AccessEvaluator.builder().authorizations("one", "two", "three", "four").build();

    // test for empty vis
    assertTrue(ct.isAccessible(""));

    // test for and
    assertTrue(ct.isAccessible("one&two"), "'and' test");

    // test for or
    assertTrue(ct.isAccessible("foor|four"), "'or' test");

    // test for and and or
    assertTrue(ct.isAccessible("(one&two)|(foo&bar)"), "'and' and 'or' test");

    // test for false negatives
    for (String marking : new String[] {"one", "one|five", "five|one", "(one)",
        "(one&two)|(foo&bar)", "(one|foo)&three", "one|foo|bar", "(one|foo)|bar",
        "((one|foo)|bar)&two"}) {
      assertTrue(ct.isAccessible(marking), marking);
      assertTrue(ct.isAccessible(marking.getBytes(UTF_8)), marking);
    }

    // test for false positives
    for (String marking : new String[] {"five", "one&five", "five&one", "((one|foo)|bar)&goober"}) {
      assertFalse(ct.isAccessible(marking), marking);
      assertFalse(ct.isAccessible(marking.getBytes(UTF_8)), marking);
    }
  }

  @Test
  public void testQuotedExpressions() {
    runQuoteTest(AccessEvaluator.builder().authorizations("A#C", "A\"C", "A\\C", "AC").build());

    var authsSet = Set.of("A#C", "A\"C", "A\\C", "AC");
    // construct VisibilityEvaluator using another constructor and run test again
    runQuoteTest(AccessEvaluator.builder()
        .authorizations(auth -> authsSet.contains(new String(auth, UTF_8))).build());
  }

  private void runQuoteTest(AccessEvaluator va) {
    assertTrue(va.isAccessible(quote("A#C") + "|" + quote("A?C")));
    assertTrue(va.isAccessible(AccessExpression.of(quote("A#C") + "|" + quote("A?C")).normalize()));
    assertTrue(va.isAccessible(quote("A\"C") + "&" + quote("A\\C")));
    assertTrue(
        va.isAccessible(AccessExpression.of(quote("A\"C") + "&" + quote("A\\C")).normalize()));
    assertTrue(va.isAccessible("(" + quote("A\"C") + "|B)&(" + quote("A#C") + "|D)"));

    assertFalse(va.isAccessible(quote("A#C") + "&B"));

    assertTrue(va.isAccessible(quote("A#C")));
    assertTrue(va.isAccessible("(" + quote("A#C") + ")"));
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

  @Test
  public void testNonAscii() {

    var va = AccessEvaluator.builder().authorizations("五", "六", "八", "九", "五十").build();
    testNonAscii(va);

    va = AccessEvaluator.builder().authorizations(Set.of("五", "六", "八", "九", "五十")).build();
    testNonAscii(va);

    va = AccessEvaluator.builder().authorizations(List.of("五".getBytes(UTF_8), "六".getBytes(UTF_8),
        "八".getBytes(UTF_8), "九".getBytes(UTF_8), "五十".getBytes(UTF_8))).build();
    testNonAscii(va);

    var authsSet = Set.of("五", "六", "八", "九", "五十");
    va = AccessEvaluator.builder()
        .authorizations(auth -> authsSet.contains(new String(auth, UTF_8))).build();
    testNonAscii(va);
  }

  private static void testNonAscii(AccessEvaluator va) {
    List<String> visible = new ArrayList<>();
    visible.add(quote("五") + "|" + quote("四"));
    visible.add(quote("五") + "&(" + quote("四") + "|" + quote("九") + ")");
    visible.add("\"五\"&(\"四\"|\"五十\")");

    for (String marking : visible) {
      assertTrue(va.isAccessible(marking), marking);
      assertTrue(va.isAccessible(marking.getBytes(UTF_8)), marking);
    }

    List<String> invisible = new ArrayList<>();
    invisible.add(quote("五") + "&" + quote("四"));
    invisible.add(quote("五") + "&(" + quote("四") + "|" + quote("三") + ")");
    invisible.add("\"五\"&(\"四\"|\"三\")");

    for (String marking : invisible) {
      assertFalse(va.isAccessible(marking), marking);
      assertFalse(va.isAccessible(marking.getBytes(UTF_8)), marking);
    }
  }

  private static String unescape(String s) {
    return new String(AccessEvaluatorImpl.unescape(new BytesWrapper(s.getBytes(UTF_8))), UTF_8);
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

  @Test
  public void testMultipleAuthorizationSets() {
    Collection<Set<String>> authSets = List.of(Set.of("A", "B"), Set.of("C", "D"));
    var ae = AccessEvaluator.builder().authorizations(authSets).build();

    assertFalse(ae.isAccessible("A"));
    assertFalse(ae.isAccessible("A&B"));
    assertFalse(ae.isAccessible("C&D"));
    assertFalse(ae.isAccessible("A&C"));
    assertFalse(ae.isAccessible("B&C"));
    assertFalse(ae.isAccessible("A&B&C&D"));
    assertFalse(ae.isAccessible("(A&C)|(B&D)"));
    assertTrue(ae.isAccessible(""));
    assertTrue(ae.isAccessible("B|C"));
    assertTrue(ae.isAccessible("(A&B)|(C&D)"));
  }

  // TODO need to copy all test from Accumulo
}
