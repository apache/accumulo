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
package org.apache.accumulo.core.clientImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;

import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

public class ConditionalComparatorTest {
  @Test
  public void testComparator() {
    Condition c1 = new Condition("a", "b");
    Condition c2 = new Condition("a", "c");
    Condition c3 = new Condition("b", "c");
    Condition c4 = new Condition("a", "b").setTimestamp(5);
    Condition c5 = new Condition("a", "b").setTimestamp(6);
    Condition c6 = new Condition("a", "b").setVisibility(new ColumnVisibility("A&B"));
    Condition c7 = new Condition("a", "b").setVisibility(new ColumnVisibility("A&C"));

    Comparator<Condition> comparator = ConditionalWriterImpl.CONDITION_COMPARATOR;

    assertEquals(0, comparator.compare(c1, c1));
    assertTrue(comparator.compare(c1, c2) < 0);
    assertTrue(comparator.compare(c2, c1) > 0);
    assertTrue(comparator.compare(c1, c3) < 0);
    assertTrue(comparator.compare(c3, c1) > 0);
    assertTrue(comparator.compare(c1, c4) < 0);
    assertTrue(comparator.compare(c4, c1) > 0);
    assertTrue(comparator.compare(c5, c4) < 0);
    assertTrue(comparator.compare(c4, c5) > 0);
    assertTrue(comparator.compare(c1, c7) < 0);
    assertTrue(comparator.compare(c7, c1) > 0);
    assertTrue(comparator.compare(c6, c7) < 0);
    assertTrue(comparator.compare(c7, c6) > 0);
  }
}
