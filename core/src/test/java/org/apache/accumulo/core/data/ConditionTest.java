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
package org.apache.accumulo.core.data;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class ConditionTest {
  private static final ByteSequence EMPTY = new ArrayByteSequence(new byte[0]);
  private static final String FAMILY = "family";
  private static final String QUALIFIER = "qualifier";
  private static final String VISIBILITY = "visibility";
  private static final String VALUE = "value";
  private static final IteratorSetting[] ITERATORS = {new IteratorSetting(1, "first", "someclass"), new IteratorSetting(2, "second", "someotherclass"),
      new IteratorSetting(3, "third", "yetanotherclass")};

  private String toString(ByteSequence bs) {
    if (bs == null) {
      return null;
    }
    return new String(bs.toArray(), UTF_8);
  }

  private Condition c;

  @Before
  public void setUp() throws Exception {
    c = new Condition(FAMILY, QUALIFIER);
  }

  @Test
  public void testConstruction_CharSequence() {
    assertEquals(FAMILY, toString(c.getFamily()));
    assertEquals(QUALIFIER, toString(c.getQualifier()));
    assertEquals(EMPTY, c.getVisibility());
  }

  @Test
  public void testConstruction_ByteArray() {
    c = new Condition(FAMILY.getBytes(UTF_8), QUALIFIER.getBytes(UTF_8));
    assertEquals(FAMILY, toString(c.getFamily()));
    assertEquals(QUALIFIER, toString(c.getQualifier()));
    assertEquals(EMPTY, c.getVisibility());
  }

  @Test
  public void testConstruction_Text() {
    c = new Condition(new Text(FAMILY), new Text(QUALIFIER));
    assertEquals(FAMILY, toString(c.getFamily()));
    assertEquals(QUALIFIER, toString(c.getQualifier()));
    assertEquals(EMPTY, c.getVisibility());
  }

  @Test
  public void testConstruction_ByteSequence() {
    c = new Condition(new ArrayByteSequence(FAMILY.getBytes(UTF_8)), new ArrayByteSequence(QUALIFIER.getBytes(UTF_8)));
    assertEquals(FAMILY, toString(c.getFamily()));
    assertEquals(QUALIFIER, toString(c.getQualifier()));
    assertEquals(EMPTY, c.getVisibility());
  }

  @Test
  public void testGetSetTimestamp() {
    c.setTimestamp(1234L);
    assertEquals(Long.valueOf(1234L), c.getTimestamp());
  }

  @Test
  public void testSetValue_CharSequence() {
    c.setValue(VALUE);
    assertEquals(VALUE, toString(c.getValue()));
  }

  @Test
  public void testSetValue_ByteArray() {
    c.setValue(VALUE.getBytes(UTF_8));
    assertEquals(VALUE, toString(c.getValue()));
  }

  @Test
  public void testSetValue_Text() {
    c.setValue(new Text(VALUE));
    assertEquals(VALUE, toString(c.getValue()));
  }

  @Test
  public void testSetValue_ByteSequence() {
    c.setValue(new ArrayByteSequence(VALUE.getBytes(UTF_8)));
    assertEquals(VALUE, toString(c.getValue()));
  }

  @Test
  public void testGetSetVisibility() {
    ColumnVisibility vis = new ColumnVisibility(VISIBILITY);
    c.setVisibility(vis);
    assertEquals(VISIBILITY, toString(c.getVisibility()));
  }

  @Test
  public void testGetSetIterators() {
    c.setIterators(ITERATORS);
    assertArrayEquals(ITERATORS, c.getIterators());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetIterators_DuplicateName() {
    IteratorSetting[] iterators = {new IteratorSetting(1, "first", "someclass"), new IteratorSetting(2, "second", "someotherclass"),
        new IteratorSetting(3, "first", "yetanotherclass")};
    c.setIterators(iterators);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetIterators_DuplicatePriority() {
    IteratorSetting[] iterators = {new IteratorSetting(1, "first", "someclass"), new IteratorSetting(2, "second", "someotherclass"),
        new IteratorSetting(1, "third", "yetanotherclass")};
    c.setIterators(iterators);
  }

  @Test
  public void testEquals() {
    ColumnVisibility cvis = new ColumnVisibility(VISIBILITY);
    c.setVisibility(cvis);
    c.setValue(VALUE);
    c.setTimestamp(1234L);
    c.setIterators(ITERATORS);

    // reflexivity
    assertTrue(c.equals(c));

    // non-nullity
    assertFalse(c.equals(null));

    // symmetry
    Condition c2 = new Condition(FAMILY, QUALIFIER);
    c2.setVisibility(cvis);
    c2.setValue(VALUE);
    c2.setTimestamp(1234L);
    c2.setIterators(ITERATORS);
    assertTrue(c.equals(c2));
    assertTrue(c2.equals(c));

    Condition c3 = new Condition("nope", QUALIFIER);
    c3.setVisibility(cvis);
    c3.setValue(VALUE);
    c3.setTimestamp(1234L);
    c3.setIterators(ITERATORS);
    assertFalse(c.equals(c3));
    assertFalse(c3.equals(c));
    c3 = new Condition(FAMILY, "nope");
    c3.setVisibility(cvis);
    c3.setValue(VALUE);
    c3.setTimestamp(1234L);
    c3.setIterators(ITERATORS);
    assertFalse(c.equals(c3));
    assertFalse(c3.equals(c));

    c2.setVisibility(new ColumnVisibility("sekrit"));
    assertFalse(c.equals(c2));
    assertFalse(c2.equals(c));
    c2.setVisibility(cvis);
    c2.setValue(EMPTY);
    assertFalse(c.equals(c2));
    assertFalse(c2.equals(c));
    c2.setValue(VALUE);
    c2.setTimestamp(2345L);
    assertFalse(c.equals(c2));
    assertFalse(c2.equals(c));
    c2.setTimestamp(1234L);
    c2.setIterators(new IteratorSetting[0]);
    assertFalse(c.equals(c2));
    assertFalse(c2.equals(c));
    c2.setIterators(ITERATORS);
    assertTrue(c.equals(c2));
    assertTrue(c2.equals(c));

    // set everything but vis, so its null
    Condition c4 = new Condition(FAMILY, QUALIFIER);
    c4.setValue(VALUE);
    c4.setTimestamp(1234L);
    c4.setIterators(ITERATORS);

    assertFalse(c.equals(c4));
    assertFalse(c4.equals(c));

    // set everything but timestamp, so its null
    Condition c5 = new Condition(FAMILY, QUALIFIER);
    c5.setVisibility(cvis);
    c5.setValue(VALUE);
    c5.setIterators(ITERATORS);

    assertFalse(c.equals(c5));
    assertFalse(c5.equals(c));

    // set everything but value
    Condition c6 = new Condition(FAMILY, QUALIFIER);
    c6.setVisibility(cvis);
    c6.setTimestamp(1234L);
    c6.setIterators(ITERATORS);

    assertFalse(c.equals(c6));
    assertFalse(c6.equals(c));

    // test w/ no optional fields set
    Condition c7 = new Condition(FAMILY, QUALIFIER);
    Condition c8 = new Condition(FAMILY, QUALIFIER);
    assertTrue(c7.equals(c8));
    assertTrue(c8.equals(c7));

  }

  @Test
  public void testHashCode() {
    ColumnVisibility cvis = new ColumnVisibility(VISIBILITY);
    c.setVisibility(cvis);
    c.setValue(VALUE);
    c.setTimestamp(1234L);
    c.setIterators(ITERATORS);
    int hc1 = c.hashCode();

    Condition c2 = new Condition(FAMILY, QUALIFIER);
    c2.setVisibility(cvis);
    c2.setValue(VALUE);
    c2.setTimestamp(1234L);
    c2.setIterators(ITERATORS);
    assertTrue(c.equals(c2));
    assertEquals(hc1, c2.hashCode());
  }
}
