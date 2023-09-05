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
      assertTrue(arbiter.canAccess("A1"));
      assertTrue(arbiter.canAccess("Z9"));
      assertTrue(arbiter.canAccess("A1|G2"));
      assertTrue(arbiter.canAccess("G2|A1"));
      assertTrue(arbiter.canAccess("Z9|G2"));
      assertTrue(arbiter.canAccess("G2|A1"));
      assertTrue(arbiter.canAccess("Z9|A1"));
      assertTrue(arbiter.canAccess("A1|Z9"));
      assertTrue(arbiter.canAccess("(A1|G2)&(Z9|G5)"));

      assertFalse(arbiter.canAccess("Z8"));
      assertFalse(arbiter.canAccess("A2"));
      assertFalse(arbiter.canAccess("A2|Z8"));
      assertFalse(arbiter.canAccess("A1&Z8"));
      assertFalse(arbiter.canAccess("Z8&A1"));

      // rerun some of the same labels
      assertTrue(arbiter.canAccess("Z9|A1"));
      assertTrue(arbiter.canAccess("(A1|G2)&(Z9|G5)"));
      assertFalse(arbiter.canAccess("A2|Z8"));
      assertFalse(arbiter.canAccess("A1&Z8"));
    }
  }

  @Test
  public void testIncorrectExpression() {
    var evaluator = AccessEvaluator.builder().authorizations("A1", "Z9").build();

    for (String marking : List.of("()", "()|()", "()&()", "&", "|", "(&)", "(|)", "A|", "|A", "A&",
        "&A", "A|(B|)", "A|(|B)", "A|(B&)", "A|(&B)", "((A)", "(A", "A)", "((A)", ")", "))", "A|B)",
        "(A|B))", "A&B)", "(A&B))", "A&)", "A|)", "(&A", "(|B", "A$B", "(A|(B&()))", "A|B&C",
        "A&B|C", "(A&B|C)|(C&Z)", "(A&B|C)&(C&Z)", "(A&B|C)|(D|C&Z)", "(A&B|C)&(D|C&Z)", "\"",
        "\"\\c\"", "\"\\\"", "\"\"\"", "\"\"\"&A")) {
      assertThrows(IllegalAccessExpressionException.class, () -> evaluator.canAccess(marking),
          marking);
    }

    // Create a quoted string that is empty
    String eq = "\"\"";

    for (String marking : List.of(eq, eq + "|A", "A|" + eq, eq + "&A", "A&" + eq,
        "A&(" + eq + "|B)", "(" + eq + ")")) {
      assertThrows(IllegalAccessExpressionException.class, () -> evaluator.canAccess(marking),
          marking);
    }
  }

  // copied from VisibilityEvaluatorTest in Accumulo and modified, need to copy more test from that
  // class
  @Test
  public void testVisibilityEvaluator() {
    var ct = AccessEvaluator.builder().authorizations("one", "two", "three", "four").build();

    // test for empty vis
    assertTrue(ct.canAccess(""));

    // test for and
    assertTrue(ct.canAccess("one&two"), "'and' test");

    // test for or
    assertTrue(ct.canAccess("foor|four"), "'or' test");

    // test for and and or
    assertTrue(ct.canAccess("(one&two)|(foo&bar)"), "'and' and 'or' test");

    // test for false negatives
    for (String marking : new String[] {"one", "one|five", "five|one", "(one)",
        "(one&two)|(foo&bar)", "(one|foo)&three", "one|foo|bar", "(one|foo)|bar",
        "((one|foo)|bar)&two"}) {
      assertTrue(ct.canAccess(marking), marking);
      assertTrue(ct.canAccess(marking.getBytes(UTF_8)), marking);
    }

    // test for false positives
    for (String marking : new String[] {"five", "one&five", "five&one", "((one|foo)|bar)&goober"}) {
      assertFalse(ct.canAccess(marking), marking);
      assertFalse(ct.canAccess(marking.getBytes(UTF_8)), marking);
    }
  }

  @Test
  public void testQuotedExpressions() {
    runQuoteTest(AccessEvaluator.builder().authorizations("A#C", "A\"C", "A\\C", "AC").build());

    var authsSet = Set.of("A#C", "A\"C", "A\\C", "AC");
    // construct VisibilityEvaluator using another constructor and run test again
    runQuoteTest(AccessEvaluator.builder().authorizations(authsSet::contains).build());
  }

  private void runQuoteTest(AccessEvaluator va) {
    assertTrue(va.canAccess(quote("A#C") + "|" + quote("A?C")));
    assertTrue(va.canAccess(AccessExpression.of(quote("A#C") + "|" + quote("A?C")).normalize()));
    assertTrue(va.canAccess(quote("A\"C") + "&" + quote("A\\C")));
    assertTrue(va.canAccess(AccessExpression.of(quote("A\"C") + "&" + quote("A\\C")).normalize()));
    assertTrue(va.canAccess("(" + quote("A\"C") + "|B)&(" + quote("A#C") + "|D)"));

    assertFalse(va.canAccess(quote("A#C") + "&B"));

    assertTrue(va.canAccess(quote("A#C")));
    assertTrue(va.canAccess("(" + quote("A#C") + ")"));
  }

  @Test
  public void testSpecialChars() {
    // test the non alphanumeric characters that can occur unquoted as an authorization in an
    // expression

    var evaluator = AccessEvaluator.builder()
        .authorizations("a_b", "a-c", "a/d", "a:e", "a.f", "a_b-c/d:e.f").build();

    // test for false negatives
    for (String marking : new String[] {"a_b", "\"a_b\"", "a-c", "\"a-c\"", "a/d", "\"a/d\"", "a:e",
        "\"a:e\"", "a.f", "\"a.f\"", "a_b|a_z", "a-z|a-c", "a/d|a/z", "a:e|a:z", "a.z|a.f",
        "a_b&a-c&a/d&a:e&a.f", "(a-z|a-c)&(a/d|a/z)", "a_b-c/d:e.f", "a_b-c/d:e.f&a/d"}) {
      assertTrue(evaluator.canAccess(marking), marking);
      assertTrue(evaluator.canAccess(marking.getBytes(UTF_8)), marking);
    }

    // test for false positives
    for (String marking : new String[] {"a_c", "b_b", "a-b", "a/c", "a:f", "a.e", "a_b&a_z",
        "a_b&a-b&a/d&a:e&a.f", "a_b-c/d:e.z", "a_b-c/d:e.f&a/c"}) {
      assertFalse(evaluator.canAccess(marking), marking);
      assertFalse(evaluator.canAccess(marking.getBytes(UTF_8)), marking);
    }

    var evaluator2 = AccessEvaluator.builder().authorizations("_", "-", "/", ":", ".").build();
    for (String marking : new String[] {"_", "\"_\"", "-", "/", ":", ".", "_&-", "_&(a|:)",
        "/&:&."}) {
      assertTrue(evaluator2.canAccess(marking), marking);
      assertTrue(evaluator2.canAccess(marking.getBytes(UTF_8)), marking);
    }

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
  public void testEmpty() {

    var expression1 = AccessExpression.of("");
    var expression2 = AccessExpression.of("A1");

    var evaluator = AccessEvaluator.builder().authorizations("A1").build();
    assertTrue(evaluator.canAccess(""));
    assertTrue(evaluator.canAccess("".getBytes(UTF_8)));
    assertTrue(evaluator.canAccess(expression1));
    assertTrue(evaluator.canAccess("A1"));
    assertTrue(evaluator.canAccess("A1".getBytes(UTF_8)));
    assertTrue(evaluator.canAccess(expression2));

    var evaluator2 = AccessEvaluator.builder().authorizations().build();
    assertTrue(evaluator2.canAccess(""), "");
    assertTrue(evaluator2.canAccess("".getBytes(UTF_8)), "");
    assertTrue(evaluator2.canAccess(expression1), "");
    assertFalse(evaluator2.canAccess("A1"));
    assertFalse(evaluator2.canAccess("A1".getBytes(UTF_8)));
    assertFalse(evaluator2.canAccess(expression2));

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

    va = AccessEvaluator.builder().authorizations("五", "六", "八", "九", "五十").build();
    testNonAscii(va);

    var authsSet = Set.of("五", "六", "八", "九", "五十");
    va = AccessEvaluator.builder().authorizations(authsSet::contains).build();
    testNonAscii(va);
  }

  private static void testNonAscii(AccessEvaluator va) {
    List<String> visible = new ArrayList<>();
    visible.add(quote("五") + "|" + quote("四"));
    visible.add(quote("五") + "&(" + quote("四") + "|" + quote("九") + ")");
    visible.add("\"五\"&(\"四\"|\"五十\")");

    for (String marking : visible) {
      assertTrue(va.canAccess(marking), marking);
      assertTrue(va.canAccess(marking.getBytes(UTF_8)), marking);
    }

    List<String> invisible = new ArrayList<>();
    invisible.add(quote("五") + "&" + quote("四"));
    invisible.add(quote("五") + "&(" + quote("四") + "|" + quote("三") + ")");
    invisible.add("\"五\"&(\"四\"|\"三\")");

    for (String marking : invisible) {
      assertFalse(va.canAccess(marking), marking);
      assertFalse(va.canAccess(marking.getBytes(UTF_8)), marking);
    }
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

  @Test
  public void testMultipleAuthorizationSets() {
    Collection<Authorizations> authSets =
        List.of(Authorizations.of("A", "B"), Authorizations.of("C", "D"));
    var evaluator = AccessEvaluator.builder().authorizations(authSets).build();

    assertFalse(evaluator.canAccess("A"));
    assertFalse(evaluator.canAccess("A&B"));
    assertFalse(evaluator.canAccess("C&D"));
    assertFalse(evaluator.canAccess("A&C"));
    assertFalse(evaluator.canAccess("B&C"));
    assertFalse(evaluator.canAccess("A&B&C&D"));
    assertFalse(evaluator.canAccess("(A&C)|(B&D)"));
    assertTrue(evaluator.canAccess(""));
    assertTrue(evaluator.canAccess("B|C"));
    assertTrue(evaluator.canAccess("(A&B)|(C&D)"));
    assertTrue(evaluator.canAccess("(A&B)|(C)"));
  }

  // TODO need to copy all test from Accumulo
}
