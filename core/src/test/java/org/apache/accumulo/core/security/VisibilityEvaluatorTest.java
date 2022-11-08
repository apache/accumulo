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
package org.apache.accumulo.core.security;

import static org.apache.accumulo.core.security.ColumnVisibility.quote;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.util.ByteArraySet;
import org.junit.jupiter.api.Test;

public class VisibilityEvaluatorTest {

  @Test
  public void testVisibilityEvaluator() throws VisibilityParseException {
    VisibilityEvaluator ct = new VisibilityEvaluator(
        new Authorizations(ByteArraySet.fromStrings("one", "two", "three", "four")));

    // test for empty vis
    assertTrue(ct.evaluate(new ColumnVisibility(new byte[0])));

    // test for and
    assertTrue(ct.evaluate(new ColumnVisibility("one&two")), "'and' test");

    // test for or
    assertTrue(ct.evaluate(new ColumnVisibility("foor|four")), "'or' test");

    // test for and and or
    assertTrue(ct.evaluate(new ColumnVisibility("(one&two)|(foo&bar)")), "'and' and 'or' test");

    // test for false negatives
    for (String marking : new String[] {"one", "one|five", "five|one", "(one)",
        "(one&two)|(foo&bar)", "(one|foo)&three", "one|foo|bar", "(one|foo)|bar",
        "((one|foo)|bar)&two"}) {
      assertTrue(ct.evaluate(new ColumnVisibility(marking)), marking);
    }

    // test for false positives
    for (String marking : new String[] {"five", "one&five", "five&one", "((one|foo)|bar)&goober"}) {
      assertFalse(ct.evaluate(new ColumnVisibility(marking)), marking);
    }
  }

  @Test
  public void testQuotedExpressions() throws VisibilityParseException {

    Authorizations auths = new Authorizations("A#C", "A\"C", "A\\C", "AC");
    VisibilityEvaluator ct = new VisibilityEvaluator(auths);
    runQuoteTest(ct);

    // construct VisibilityEvaluator using another constructor and run test again
    ct = new VisibilityEvaluator((AuthorizationContainer) auths);
    runQuoteTest(ct);
  }

  private void runQuoteTest(VisibilityEvaluator ct) throws VisibilityParseException {
    assertTrue(ct.evaluate(new ColumnVisibility(quote("A#C") + "|" + quote("A?C"))));
    assertTrue(ct.evaluate(
        new ColumnVisibility(new ColumnVisibility(quote("A#C") + "|" + quote("A?C")).flatten())));
    assertTrue(ct.evaluate(new ColumnVisibility(quote("A\"C") + "&" + quote("A\\C"))));
    assertTrue(ct.evaluate(
        new ColumnVisibility(new ColumnVisibility(quote("A\"C") + "&" + quote("A\\C")).flatten())));
    assertTrue(
        ct.evaluate(new ColumnVisibility("(" + quote("A\"C") + "|B)&(" + quote("A#C") + "|D)")));

    assertFalse(ct.evaluate(new ColumnVisibility(quote("A#C") + "&B")));

    assertTrue(ct.evaluate(new ColumnVisibility(quote("A#C"))));
    assertTrue(ct.evaluate(new ColumnVisibility("(" + quote("A#C") + ")")));
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
  public void testUnescape() {
    assertEquals("a\"b", VisibilityEvaluator.unescape(new ArrayByteSequence("a\\\"b")).toString());
    assertEquals("a\\b", VisibilityEvaluator.unescape(new ArrayByteSequence("a\\\\b")).toString());
    assertEquals("a\\\"b",
        VisibilityEvaluator.unescape(new ArrayByteSequence("a\\\\\\\"b")).toString());
    assertEquals("\\\"",
        VisibilityEvaluator.unescape(new ArrayByteSequence("\\\\\\\"")).toString());
    assertEquals("a\\b\\c\\d",
        VisibilityEvaluator.unescape(new ArrayByteSequence("a\\\\b\\\\c\\\\d")).toString());

    final String message = "Expected failure to unescape invalid escape sequence";
    final var invalidEscapeSeqList = List.of(new ArrayByteSequence("a\\b"),
        new ArrayByteSequence("a\\b\\c"), new ArrayByteSequence("a\"b\\"));

    invalidEscapeSeqList.forEach(seq -> assertThrows(IllegalArgumentException.class,
        () -> VisibilityEvaluator.unescape(seq), message));
  }

  @Test
  public void testNonAscii() throws VisibilityParseException {
    VisibilityEvaluator ct = new VisibilityEvaluator(new Authorizations("五", "六", "八", "九", "五十"));

    assertTrue(ct.evaluate(new ColumnVisibility(quote("五") + "|" + quote("四"))));
    assertFalse(ct.evaluate(new ColumnVisibility(quote("五") + "&" + quote("四"))));
    assertTrue(
        ct.evaluate(new ColumnVisibility(quote("五") + "&(" + quote("四") + "|" + quote("九") + ")")));
    assertTrue(ct.evaluate(new ColumnVisibility("\"五\"&(\"四\"|\"五十\")")));
    assertFalse(
        ct.evaluate(new ColumnVisibility(quote("五") + "&(" + quote("四") + "|" + quote("三") + ")")));
    assertFalse(ct.evaluate(new ColumnVisibility("\"五\"&(\"四\"|\"三\")")));
  }
}
