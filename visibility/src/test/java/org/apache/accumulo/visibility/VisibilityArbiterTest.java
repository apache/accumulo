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
package org.apache.accumulo.visibility;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.visibility.VisibilityExpression.quote;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class VisibilityArbiterTest {
  @Test
  public void testVisibilityArbiter() {
    for (int cacheSize : List.of(0, 100)) {
      var arbiter =
          VisibilityArbiter.builder().authorizations("A1", "Z9").cacheSize(cacheSize).build();
      assertTrue(arbiter.isVisible("A1"));
      assertTrue(arbiter.isVisible("Z9"));
      assertTrue(arbiter.isVisible("A1|G2"));
      assertTrue(arbiter.isVisible("G2|A1"));
      assertTrue(arbiter.isVisible("Z9|G2"));
      assertTrue(arbiter.isVisible("G2|A1"));
      assertTrue(arbiter.isVisible("Z9|A1"));
      assertTrue(arbiter.isVisible("A1|Z9"));
      assertTrue(arbiter.isVisible("(A1|G2)&(Z9|G5)"));

      assertFalse(arbiter.isVisible("Z8"));
      assertFalse(arbiter.isVisible("A2"));
      assertFalse(arbiter.isVisible("A2|Z8"));
      assertFalse(arbiter.isVisible("A1&Z8"));
      assertFalse(arbiter.isVisible("Z8&A1"));

      // rerun some of the same labels
      assertTrue(arbiter.isVisible("Z9|A1"));
      assertTrue(arbiter.isVisible("(A1|G2)&(Z9|G5)"));
      assertFalse(arbiter.isVisible("A2|Z8"));
      assertFalse(arbiter.isVisible("A1&Z8"));
    }
  }

  @Test
  public void testIncorrectExpression() {
    var evaluator = VisibilityArbiter.builder().authorizations("A1", "Z9").build();
    assertThrows(IllegalVisibilityException.class, () -> evaluator.isVisible("(A"));
    assertThrows(IllegalVisibilityException.class, () -> evaluator.isVisible("A)"));
    assertThrows(IllegalVisibilityException.class, () -> evaluator.isVisible("((A)"));
    assertThrows(IllegalVisibilityException.class, () -> evaluator.isVisible("A$B"));
    assertThrows(IllegalVisibilityException.class, () -> evaluator.isVisible("(A|(B&()))"));
  }

  // copied from VisibilityEvaluatorTest in Accumulo and modified, need to copy more test from that
  // class
  @Test
  public void testVisibilityEvaluator() {
    var ct = VisibilityArbiter.builder().authorizations("one", "two", "three", "four").build();

    // test for empty vis
    assertTrue(ct.isVisible(""));

    // test for and
    assertTrue(ct.isVisible("one&two"), "'and' test");

    // test for or
    assertTrue(ct.isVisible("foor|four"), "'or' test");

    // test for and and or
    assertTrue(ct.isVisible("(one&two)|(foo&bar)"), "'and' and 'or' test");

    // test for false negatives
    for (String marking : new String[] {"one", "one|five", "five|one", "(one)",
        "(one&two)|(foo&bar)", "(one|foo)&three", "one|foo|bar", "(one|foo)|bar",
        "((one|foo)|bar)&two"}) {
      assertTrue(ct.isVisible(marking), marking);
      assertTrue(ct.isVisible(marking.getBytes(UTF_8)), marking);
    }

    // test for false positives
    for (String marking : new String[] {"five", "one&five", "five&one", "((one|foo)|bar)&goober"}) {
      assertFalse(ct.isVisible(marking), marking);
      assertFalse(ct.isVisible(marking.getBytes(UTF_8)), marking);
    }
  }

  @Test
  public void testQuotedExpressions() {
    runQuoteTest(VisibilityArbiter.builder().authorizations("A#C", "A\"C", "A\\C", "AC").build());

    var authsSet = Set.of("A#C", "A\"C", "A\\C", "AC");
    // construct VisibilityEvaluator using another constructor and run test again
    runQuoteTest(VisibilityArbiter.builder()
        .authorizations(auth -> authsSet.contains(new String(auth, UTF_8))).build());
  }

  private void runQuoteTest(VisibilityArbiter va) {
    assertTrue(va.isVisible(quote("A#C") + "|" + quote("A?C")));
    assertTrue(
        va.isVisible(VisibilityExpression.parse(quote("A#C") + "|" + quote("A?C")).normalize()));
    assertTrue(va.isVisible(quote("A\"C") + "&" + quote("A\\C")));
    assertTrue(
        va.isVisible(VisibilityExpression.parse(quote("A\"C") + "&" + quote("A\\C")).normalize()));
    assertTrue(va.isVisible("(" + quote("A\"C") + "|B)&(" + quote("A#C") + "|D)"));

    assertFalse(va.isVisible(quote("A#C") + "&B"));

    assertTrue(va.isVisible(quote("A#C")));
    assertTrue(va.isVisible("(" + quote("A#C") + ")"));
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

    var va = VisibilityArbiter.builder().authorizations("五", "六", "八", "九", "五十").build();
    testNonAscii(va);

    va = VisibilityArbiter.builder().authorizations(Set.of("五", "六", "八", "九", "五十")).build();
    testNonAscii(va);

    va = VisibilityArbiter.builder().authorizations(List.of("五".getBytes(UTF_8),
        "六".getBytes(UTF_8), "八".getBytes(UTF_8), "九".getBytes(UTF_8), "五十".getBytes(UTF_8)))
        .build();
    testNonAscii(va);

    var authsSet = Set.of("五", "六", "八", "九", "五十");
    va = VisibilityArbiter.builder()
        .authorizations(auth -> authsSet.contains(new String(auth, UTF_8))).build();
    testNonAscii(va);
  }

  private static void testNonAscii(VisibilityArbiter va) {
    List<String> visible = new ArrayList<>();
    visible.add(quote("五") + "|" + quote("四"));
    visible.add(quote("五") + "&(" + quote("四") + "|" + quote("九") + ")");
    visible.add("\"五\"&(\"四\"|\"五十\")");

    for (String marking : visible) {
      assertTrue(va.isVisible(marking), marking);
      assertTrue(va.isVisible(marking.getBytes(UTF_8)), marking);
    }

    List<String> invisible = new ArrayList<>();
    invisible.add(quote("五") + "&" + quote("四"));
    invisible.add(quote("五") + "&(" + quote("四") + "|" + quote("三") + ")");
    invisible.add("\"五\"&(\"四\"|\"三\")");

    for (String marking : invisible) {
      assertFalse(va.isVisible(marking), marking);
      assertFalse(va.isVisible(marking.getBytes(UTF_8)), marking);
    }
  }

  private static String unescape(String s) {
    return new String(VisibilityArbiterImpl.unescape(new BytesWrapper(s.getBytes(UTF_8))), UTF_8);
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
