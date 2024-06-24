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
package org.apache.accumulo.core.data;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConditionalMutationTest {
  private static final byte[] ROW = "row".getBytes(UTF_8);
  private static final String VALUE = "val01";
  private static final String FAMILY = "family";
  private static final String FAMILY2 = "family2";
  private static final String FAMILY3 = "family3";
  private static final String QUALIFIER = "qualifier";
  private static final String QUALIFIER2 = "qualifier2";
  private static final String QUALIFIER3 = "qualifier3";
  private static final ColumnVisibility CVIS1 = new ColumnVisibility("A");
  private static final ColumnVisibility CVIS2 = new ColumnVisibility("B|C");
  private static final long TIMESTAMP = 1234567890;

  private Condition c1, c2;
  private ConditionalMutation cm;

  @BeforeEach
  public void setUp() {
    c1 = new Condition(FAMILY, QUALIFIER);
    c2 = new Condition(FAMILY, QUALIFIER2);
    assertNotEquals(c1, c2);
    cm = new ConditionalMutation(ROW, c1, c2);
  }

  @Test
  public void testConstruction_ByteArray() {
    assertArrayEquals(ROW, cm.getRow());
    List<Condition> cs = cm.getConditions();
    assertEquals(2, cs.size());
    assertEquals(c1, cs.get(0));
    assertEquals(c2, cs.get(1));
  }

  @Test
  public void testConstruction_ByteArray_StartAndLength() {
    cm = new ConditionalMutation(ROW, 1, 1, c1, c2);
    assertArrayEquals("o".getBytes(UTF_8), cm.getRow());
    List<Condition> cs = cm.getConditions();
    assertEquals(2, cs.size());
    assertEquals(c1, cs.get(0));
    assertEquals(c2, cs.get(1));
  }

  @Test
  public void testConstruction_Text() {
    cm = new ConditionalMutation(new Text(ROW), c1, c2);
    assertArrayEquals(ROW, cm.getRow());
    List<Condition> cs = cm.getConditions();
    assertEquals(2, cs.size());
    assertEquals(c1, cs.get(0));
    assertEquals(c2, cs.get(1));
  }

  @Test
  public void testConstruction_CharSequence() {
    cm = new ConditionalMutation(new String(ROW, UTF_8), c1, c2);
    assertArrayEquals(ROW, cm.getRow());
    List<Condition> cs = cm.getConditions();
    assertEquals(2, cs.size());
    assertEquals(c1, cs.get(0));
    assertEquals(c2, cs.get(1));
  }

  @Test
  public void testConstruction_ByteSequence() {
    cm = new ConditionalMutation(new ArrayByteSequence(ROW), c1, c2);
    assertArrayEquals(ROW, cm.getRow());
    List<Condition> cs = cm.getConditions();
    assertEquals(2, cs.size());
    assertEquals(c1, cs.get(0));
    assertEquals(c2, cs.get(1));
  }

  @Test
  public void testCopyConstructor() {
    ConditionalMutation cm2 = new ConditionalMutation(cm);
    assertArrayEquals(cm.getRow(), cm2.getRow());
    assertEquals(cm.getConditions(), cm2.getConditions());
  }

  @Test
  public void testAddCondition() {
    Condition c3 = new Condition(FAMILY, QUALIFIER3);
    cm.addCondition(c3);
    List<Condition> cs = cm.getConditions();
    assertEquals(3, cs.size());
    assertEquals(c1, cs.get(0));
    assertEquals(c2, cs.get(1));
    assertEquals(c3, cs.get(2));
  }

  @Test
  public void testPrettyPrint() {
    c1 = new Condition(FAMILY, QUALIFIER);
    cm = new ConditionalMutation(ROW, c1);
    cm.put("name", "last", CVIS1, "doe");
    cm.put("name", "first", CVIS1, "john");
    cm.put("tx", "seq", CVIS1, "1");
    String pp = cm.prettyPrint();
    assertTrue(pp.contains("mutation: " + new String(ROW, UTF_8)));
    assertTrue(pp.contains("update: name:last value doe"));
    assertTrue(pp.contains("update: name:first value john"));
    assertTrue(pp.contains("update: tx:seq value 1"));
    assertTrue(pp.contains("condition: " + FAMILY + ":" + QUALIFIER));
    assertFalse(pp.contains("value:"));
    // add a value
    c1.setValue(VALUE);
    pp = cm.prettyPrint();
    assertTrue(pp.contains("condition: " + FAMILY + ":" + QUALIFIER + " value: " + VALUE));

    // multiple conditions
    c1 = new Condition(FAMILY, QUALIFIER).setValue(VALUE);
    c2 = new Condition(FAMILY2, QUALIFIER2).setTimestamp(TIMESTAMP);
    Condition c3 = new Condition(FAMILY3, QUALIFIER3).setVisibility(CVIS1);
    cm = new ConditionalMutation(ROW, c1, c2, c3);
    cm.put("name", "last", CVIS1, "doe");
    cm.put("name", "first", CVIS1, "john");
    pp = cm.prettyPrint();
    assertTrue(pp.contains("mutation: " + new String(ROW, UTF_8)));
    assertTrue(pp.contains("update: name:last value doe"));
    assertTrue(pp.contains("update: name:first value john"));
    assertTrue(pp.contains("condition: " + FAMILY + ":" + QUALIFIER + " value: " + VALUE));
    assertTrue(pp
        .contains("condition: " + FAMILY2 + ":" + QUALIFIER2 + " timestamp: '" + TIMESTAMP + "'"));
    assertTrue(pp.contains(
        "condition: " + FAMILY3 + ":" + QUALIFIER3 + " visibility: '" + c3.getVisibility() + "'"));

    // iterators
    IteratorSetting is1 = new IteratorSetting(5, VersioningIterator.class);
    c1.setIterators(is1);
    cm = new ConditionalMutation(ROW, c1);
    cm.put("name", "last", CVIS1, "doe");
    cm.put("name", "first", CVIS1, "john");
    pp = cm.prettyPrint();
    assertTrue(pp.contains("mutation: " + new String(ROW, UTF_8)));
    assertTrue(pp.contains("update: name:last value doe"));
    assertTrue(pp.contains("update: name:first value john"));
    IteratorSetting[] iters = c1.getIterators();
    assertTrue(pp.contains("condition: " + FAMILY + ":" + QUALIFIER + " value: " + VALUE
        + " iterator: '" + iters[0] + "'"));

    // multiple iterators
    IteratorSetting is2 = new IteratorSetting(6, SummingCombiner.class);
    HashMap<String,String> map = new HashMap<>();
    map.put("prop1", "val1");
    map.put("prop2", "val2");
    is2.addOptions(map);
    c1.setIterators(is1, is2);
    pp = cm.prettyPrint();
    assertTrue(pp.contains("mutation: " + new String(ROW, UTF_8)));
    assertTrue(pp.contains("update: name:last value doe"));
    assertTrue(pp.contains("update: name:first value john"));
    iters = c1.getIterators();
    assertTrue(pp.contains("condition: " + FAMILY + ":" + QUALIFIER + " value: " + VALUE
        + " iterator: '" + iters[0] + "' '" + iters[1] + "'"));

    // all conditions together
    c1 = new Condition(FAMILY2, QUALIFIER2).setValue(VALUE).setVisibility(CVIS2)
        .setTimestamp(TIMESTAMP).setIterators(is2);
    cm = new ConditionalMutation(ROW, c1);
    cm.put("name", "last", CVIS1, "doe");
    cm.put("name", "first", CVIS1, "john");
    pp = cm.prettyPrint();
    assertTrue(pp.contains("mutation: " + new String(ROW, UTF_8)));
    assertTrue(pp.contains("update: name:last value doe"));
    assertTrue(pp.contains("update: name:first value john"));
    iters = c1.getIterators();
    assertTrue(pp.contains("condition: " + FAMILY2 + ":" + QUALIFIER2 + " value: " + VALUE
        + " visibility: '" + c1.getVisibility() + "' timestamp: '" + TIMESTAMP + "' iterator: '"
        + iters[0] + "'"));
  }

  @Test
  public void testEquals() {
    // reflexivity
    assertTrue(cm.equals(cm));

    // non-nullity
    assertNotEquals(cm, (Object) null);

    // symmetry
    ConditionalMutation cm2 = new ConditionalMutation(ROW, c1, c2);
    assertTrue(cm.equals(cm2));
    assertTrue(cm2.equals(cm));

    ConditionalMutation cm3 = new ConditionalMutation("row2".getBytes(UTF_8), c1, c2);
    assertFalse(cm.equals(cm3));
    cm3 = new ConditionalMutation(ROW, c2, c1);
    assertNotEquals(cm.getConditions(), cm3.getConditions());
    assertFalse(cm.equals(cm3));
  }

  @Test
  public void testEquals_Mutation() {
    Mutation m = new Mutation(ROW);
    assertFalse(m.equals(cm));
    assertFalse(cm.equals(m));
  }

  @Test
  public void testHashcode() {
    ConditionalMutation cm2 = new ConditionalMutation(ROW, c1, c2);
    assertTrue(cm.equals(cm2));
    assertEquals(cm2.hashCode(), cm.hashCode());
  }
}
