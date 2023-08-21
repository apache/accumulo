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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

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
    assertThrows(IllegalArgumentException.class, () -> evaluator.isVisible("(A"));
    assertThrows(IllegalArgumentException.class, () -> evaluator.isVisible("A)"));
    assertThrows(IllegalArgumentException.class, () -> evaluator.isVisible("((A)"));
    assertThrows(IllegalArgumentException.class, () -> evaluator.isVisible("A$B"));
    assertThrows(IllegalArgumentException.class, () -> evaluator.isVisible("(A|(B&()))"));
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
    }

    // test for false positives
    for (String marking : new String[] {"five", "one&five", "five&one", "((one|foo)|bar)&goober"}) {
      assertFalse(ct.isVisible(marking), marking);
    }
  }

  // TODO need to copy all test from Accumulo
}
