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
package org.apache.accumulo.core.metadata.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.client.admin.TimeType;
import org.junit.jupiter.api.Test;

public class MetadataTimeTest {

  private static final MetadataTime m1234 = new MetadataTime(1234, TimeType.MILLIS);
  private static final MetadataTime m5678 = new MetadataTime(5678, TimeType.MILLIS);
  private static final MetadataTime l1234 = new MetadataTime(1234, TimeType.LOGICAL);
  private static final MetadataTime l5678 = new MetadataTime(5678, TimeType.LOGICAL);

  @Test
  public void testGetInstance_InvalidType() {
    assertThrows(IllegalArgumentException.class, () -> MetadataTime.parse("X1234"));
  }

  @Test
  public void testGetInstance_Logical_ParseFailure() {
    assertThrows(IllegalArgumentException.class, () -> MetadataTime.parse("LABCD"));
  }

  @Test
  public void testGetInstance_Millis_ParseFailure() {
    assertThrows(IllegalArgumentException.class, () -> MetadataTime.parse("MABCD"));
  }

  @Test
  public void testGetInstance_nullArgument() {
    assertThrows(IllegalArgumentException.class, () -> MetadataTime.parse(null));
  }

  @Test
  public void testGetInstance_Invalid_timestr() {
    assertThrows(IllegalArgumentException.class, () -> MetadataTime.parse(""));
    assertThrows(IllegalArgumentException.class, () -> MetadataTime.parse("X"));
  }

  @Test
  public void testGetInstance_Millis() {
    assertEquals(1234, m1234.getTime());
    assertEquals(TimeType.MILLIS, m1234.getType());
  }

  @Test
  public void testGetInstance_Logical() {
    assertEquals(1234, l1234.getTime());
    assertEquals(TimeType.LOGICAL, l1234.getType());

  }

  @Test
  public void testEquality() {
    assertEquals(m1234, new MetadataTime(1234, TimeType.MILLIS));
    assertNotEquals(m1234, l1234);
    assertNotEquals(l1234, l5678);
  }

  @Test
  public void testValueOfM() {
    assertEquals(TimeType.MILLIS, MetadataTime.getType('M'));
  }

  @Test
  public void testValueOfL() {
    assertEquals(TimeType.LOGICAL, MetadataTime.getType('L'));
  }

  @Test
  public void testValueOfOtherChar() {
    assertThrows(IllegalArgumentException.class, () -> MetadataTime.getType('x'));
  }

  @Test
  public void testgetCodeforTimeType() {
    assertEquals('M', MetadataTime.getCode(TimeType.MILLIS));
    assertEquals('L', MetadataTime.getCode(TimeType.LOGICAL));
  }

  @Test
  public void testgetCodeforMillis() {
    assertEquals('M', m1234.getCode());
  }

  @Test
  public void testgetCodeforLogical() {
    assertEquals('L', l1234.getCode());
  }

  @Test
  public void testenCode() {
    assertEquals("M21", new MetadataTime(21, TimeType.MILLIS).encode());
    assertEquals("L45678", new MetadataTime(45678, TimeType.LOGICAL).encode());
  }

  @Test
  public void testCompareTypesDiffer1() {
    assertThrows(IllegalArgumentException.class, () -> m1234.compareTo(l1234));
  }

  @Test
  public void testCompareTypesDiffer2() {
    assertThrows(IllegalArgumentException.class, () -> l1234.compareTo(m1234));
  }

  @Test
  public void testCompareSame() {
    assertTrue(m1234.compareTo(m1234) == 0);
    assertTrue(l1234.compareTo(l1234) == 0);
  }

  @Test
  public void testCompare1() {
    assertTrue(m1234.compareTo(m5678) < 0);
    assertTrue(l1234.compareTo(l5678) < 0);
  }

  @Test
  public void testCompare2() {
    assertTrue(m5678.compareTo(m1234) > 0);
    assertTrue(l5678.compareTo(l1234) > 0);
  }
}
