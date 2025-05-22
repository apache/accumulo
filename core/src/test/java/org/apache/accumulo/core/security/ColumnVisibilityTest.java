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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class ColumnVisibilityTest {

  private void shouldThrow(String... strings) {
    for (String s : strings) {
      final byte[] sBytes = s.getBytes(UTF_8);
      assertThrows(IllegalArgumentException.class, () -> new ColumnVisibility(sBytes),
          "Should throw: " + s);
    }
  }

  private void shouldNotThrow(String... strings) {
    for (String s : strings) {
      new ColumnVisibility(s.getBytes(UTF_8));
    }
  }

  @Test
  public void testEmpty() {
    // empty visibility is valid
    ColumnVisibility a = new ColumnVisibility();
    ColumnVisibility b = new ColumnVisibility(new byte[0]);
    ColumnVisibility c = new ColumnVisibility("");
    ColumnVisibility d = new ColumnVisibility(new Text());

    assertEquals(a, b);
    assertEquals(a, c);
    assertEquals(a, d);
  }

  @Test
  public void testSimple() {
    shouldNotThrow("test", "(one)");
  }

  @Test
  public void testCompound() {
    shouldNotThrow("a|b", "a&b", "ab&bc");
    shouldNotThrow("A&B&C&D&E", "A|B|C|D|E", "(A|B|C)", "(A)|B|(C)", "A&(B)&(C)", "A&B&(L)");
    shouldNotThrow("_&-&:");
  }

  @Test
  public void testBadCharacters() {
    shouldThrow("=", "*", "^", "%", "@");
    shouldThrow("a*b");
  }

  @Test
  public void testComplexCompound() {
    shouldNotThrow("(a|b)&(x|y)");
    shouldNotThrow("a&(x|y)", "(a|b)&(x|y)", "A&(L|M)", "B&(L|M)", "A&B&(L|M)");
    shouldNotThrow("A&FOO&(L|M)", "(A|B)&FOO&(L|M)", "A&B&(L|M|FOO)", "((A|B|C)|foo)&bar");
    shouldNotThrow("(one&two)|(foo&bar)", "(one|foo)&three", "one|foo|bar", "(one|foo)|bar",
        "((one|foo)|bar)&two");
  }

  @Test
  public void testDanglingOperators() {
    shouldThrow("a|b&");
    shouldThrow("(|a)");
    shouldThrow("|");
    shouldThrow("a|", "|a", "|", "&");
    shouldThrow("&(five)", "|(five)", "(five)&", "five|", "a|(b)&", "(&five)", "(five|)");
  }

  @Test
  public void testMissingSeparators() {
    shouldThrow("one(five)", "(five)one", "(one)(two)", "a|(b(c))");
  }

  @Test
  public void testMismatchedParentheses() {
    shouldThrow("(", ")", "(a&b", "b|a)", "A|B)");
  }

  @Test
  public void testMixedOperators() {
    shouldThrow("(A&B)|(C&D)&(E)");
    shouldThrow("a|b&c", "A&B&C|D", "(A&B)|(C&D)&(E)");
  }

  @Test
  public void testQuotes() {
    shouldThrow("\"\"");
    shouldThrow("\"A\"A");
    shouldThrow("\"A\"\"B\"");
    shouldThrow("(A)\"B\"");
    shouldThrow("\"A\"(B)");
    shouldThrow("\"A");
    shouldThrow("\"");
    shouldThrow("\"B");
    shouldThrow("A&\"B");
    shouldThrow("A&\"B\\'");
    shouldThrow("A&\"B\\");

    shouldNotThrow("\"A\"");
    shouldNotThrow("(\"A\")");
    shouldNotThrow("A&\"B.D\"");
    shouldNotThrow("A&\"B\\\\D\"");
    shouldNotThrow("A&\"B\\\"D\"");
  }
}
