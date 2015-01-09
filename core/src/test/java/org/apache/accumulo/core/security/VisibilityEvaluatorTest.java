/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.security;

import static org.apache.accumulo.core.security.ColumnVisibility.quote;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.ByteArraySet;
import org.junit.Test;

public class VisibilityEvaluatorTest {

  @Test
  public void testVisibilityEvaluator() throws VisibilityParseException {
    VisibilityEvaluator ct = new VisibilityEvaluator(new Authorizations(ByteArraySet.fromStrings("one", "two", "three", "four")));

    // test for empty vis
    assertTrue(ct.evaluate(new ColumnVisibility(new byte[0])));

    // test for and
    assertTrue("'and' test", ct.evaluate(new ColumnVisibility("one&two")));

    // test for or
    assertTrue("'or' test", ct.evaluate(new ColumnVisibility("foor|four")));

    // test for and and or
    assertTrue("'and' and 'or' test", ct.evaluate(new ColumnVisibility("(one&two)|(foo&bar)")));

    // test for false negatives
    for (String marking : new String[] {"one", "one|five", "five|one", "(one)", "(one&two)|(foo&bar)", "(one|foo)&three", "one|foo|bar", "(one|foo)|bar",
        "((one|foo)|bar)&two"}) {
      assertTrue(marking, ct.evaluate(new ColumnVisibility(marking)));
    }

    // test for false positives
    for (String marking : new String[] {"five", "one&five", "five&one", "((one|foo)|bar)&goober"}) {
      assertFalse(marking, ct.evaluate(new ColumnVisibility(marking)));
    }

    // test missing separators; these should throw an exception
    for (String marking : new String[] {"one(five)", "(five)one", "(one)(two)", "a|(b(c))"}) {
      try {
        ct.evaluate(new ColumnVisibility(marking));
        fail(marking + " failed to throw");
      } catch (BadArgumentException e) {
        // all is good
      }
    }

    // test unexpected separator
    for (String marking : new String[] {"&(five)", "|(five)", "(five)&", "five|", "a|(b)&", "(&five)", "(five|)"}) {
      try {
        ct.evaluate(new ColumnVisibility(marking));
        fail(marking + " failed to throw");
      } catch (BadArgumentException e) {
        // all is good
      }
    }

    // test mismatched parentheses
    for (String marking : new String[] {"(", ")", "(a&b", "b|a)"}) {
      try {
        ct.evaluate(new ColumnVisibility(marking));
        fail(marking + " failed to throw");
      } catch (BadArgumentException e) {
        // all is good
      }
    }
  }

  @Test
  public void testQuotedExpressions() throws VisibilityParseException {
    VisibilityEvaluator ct = new VisibilityEvaluator(new Authorizations("A#C", "A\"C", "A\\C", "AC"));

    assertTrue(ct.evaluate(new ColumnVisibility(quote("A#C") + "|" + quote("A?C"))));
    assertTrue(ct.evaluate(new ColumnVisibility(new ColumnVisibility(quote("A#C") + "|" + quote("A?C")).flatten())));
    assertTrue(ct.evaluate(new ColumnVisibility(quote("A\"C") + "&" + quote("A\\C"))));
    assertTrue(ct.evaluate(new ColumnVisibility(new ColumnVisibility(quote("A\"C") + "&" + quote("A\\C")).flatten())));
    assertTrue(ct.evaluate(new ColumnVisibility("(" + quote("A\"C") + "|B)&(" + quote("A#C") + "|D)")));

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
  public void testNonAscii() throws VisibilityParseException {
    VisibilityEvaluator ct = new VisibilityEvaluator(new Authorizations("五", "六", "八", "九", "五十"));

    assertTrue(ct.evaluate(new ColumnVisibility(quote("五") + "|" + quote("四"))));
    assertFalse(ct.evaluate(new ColumnVisibility(quote("五") + "&" + quote("四"))));
    assertTrue(ct.evaluate(new ColumnVisibility(quote("五") + "&(" + quote("四") + "|" + quote("九") + ")")));
    assertTrue(ct.evaluate(new ColumnVisibility("\"五\"&(\"四\"|\"五十\")")));
    assertFalse(ct.evaluate(new ColumnVisibility(quote("五") + "&(" + quote("四") + "|" + quote("三") + ")")));
    assertFalse(ct.evaluate(new ColumnVisibility("\"五\"&(\"四\"|\"三\")")));
  }
}
