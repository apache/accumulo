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
package org.apache.accumulo.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.client.admin.TimeType;
import org.junit.Test;

public class MetadataTimeTest {

  @Test(expected = IllegalArgumentException.class)
  public void testGetInstance_InvalidType() {
    MetadataTime.parse("X1234");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetInstance_Logical_ParseFailure() {
    MetadataTime.parse("LABCD");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetInstance_Millis_ParseFailure() {
    MetadataTime.parse("MABCD");
  }

  @Test
  public void testGetInstance_Millis() {
    MetadataTime mTime = new MetadataTime(1234, TimeType.MILLIS);
    assertEquals(1234, mTime.getTime());
    assertEquals(TimeType.MILLIS, mTime.getType());
  }

  @Test
  public void testGetInstance_Logical() {
    MetadataTime lTime = new MetadataTime(1234, TimeType.LOGICAL);
    assertEquals(1234, lTime.getTime());
    assertEquals(TimeType.LOGICAL, lTime.getType());

  }

  @Test
  public void testEquality() {
    assertEquals(new MetadataTime(21, TimeType.MILLIS), MetadataTime.parse("M21"));
    assertNotEquals(new MetadataTime(21, TimeType.MILLIS), MetadataTime.parse("L21"));
    assertNotEquals(new MetadataTime(21, TimeType.LOGICAL), new MetadataTime(44, TimeType.LOGICAL));
  }

  @Test
  public void testValueOfM() {
    assertEquals(TimeType.MILLIS, MetadataTime.getType('M'));
  }

  @Test
  public void testValueOfL() {
    assertEquals(TimeType.LOGICAL, MetadataTime.getType('L'));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValueOfOtherChar() {
    MetadataTime.getType('x');
  }

  @Test
  public void testgetCodeforTimeType() {
    assertEquals('M', MetadataTime.getCode(TimeType.MILLIS));
    assertEquals('L', MetadataTime.getCode(TimeType.LOGICAL));
  }

  @Test
  public void testgetCodeforMillis() {
    MetadataTime mTime = new MetadataTime(0, TimeType.MILLIS);
    assertEquals('M', mTime.getCode());
  }

  @Test
  public void testgetCodeforLogical() {
    MetadataTime mTime = new MetadataTime(0, TimeType.LOGICAL);
    assertEquals('L', mTime.getCode());
  }

  @Test
  public void testenCode() {
    assertEquals("M21", new MetadataTime(21, TimeType.MILLIS).encode());
    assertEquals("L45678", new MetadataTime(45678, TimeType.LOGICAL).encode());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCompareTypesDiffer1() {
    MetadataTime mTime = new MetadataTime(1234, TimeType.MILLIS);
    MetadataTime lTime = new MetadataTime(1234, TimeType.LOGICAL);
    mTime.compareTo(lTime);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCompareTypesDiffer2() {
    MetadataTime mTime = new MetadataTime(1234, TimeType.MILLIS);
    MetadataTime lTime = new MetadataTime(1234, TimeType.LOGICAL);
    lTime.compareTo(mTime);
  }

  @Test
  public void testCompareSame() {
    MetadataTime mTime1 = new MetadataTime(1234, TimeType.MILLIS);
    MetadataTime mTime2 = new MetadataTime(1234, TimeType.MILLIS);
    assertTrue(mTime1.compareTo(mTime2) == 0);

    MetadataTime lTime1 = new MetadataTime(1234, TimeType.LOGICAL);
    MetadataTime lTime2 = new MetadataTime(1234, TimeType.LOGICAL);
    assertTrue(lTime1.compareTo(lTime2) == 0);
  }

  @Test
  public void testCompare1() {
    MetadataTime mTime1 = new MetadataTime(1234, TimeType.MILLIS);
    MetadataTime mTime2 = new MetadataTime(5678, TimeType.MILLIS);
    assertTrue(mTime1.compareTo(mTime2) < 0);

    MetadataTime lTime1 = new MetadataTime(1234, TimeType.LOGICAL);
    MetadataTime lTime2 = new MetadataTime(5678, TimeType.LOGICAL);
    assertTrue(lTime1.compareTo(lTime2) < 0);
  }

  @Test
  public void testCompare2() {
    MetadataTime mTime1 = new MetadataTime(1234, TimeType.MILLIS);
    MetadataTime mTime2 = new MetadataTime(5678, TimeType.MILLIS);
    assertTrue(mTime2.compareTo(mTime1) > 0);

    MetadataTime lTime1 = new MetadataTime(1234, TimeType.LOGICAL);
    MetadataTime lTime2 = new MetadataTime(5678, TimeType.LOGICAL);
    assertTrue(lTime2.compareTo(lTime1) > 0);
  }
}
