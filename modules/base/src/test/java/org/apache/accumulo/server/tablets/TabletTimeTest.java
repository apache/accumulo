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
package org.apache.accumulo.server.tablets;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.tablets.TabletTime.LogicalTime;
import org.apache.accumulo.server.tablets.TabletTime.MillisTime;
import org.junit.Before;
import org.junit.Test;

public class TabletTimeTest {
  private static final long TIME = 1234L;
  private MillisTime mtime;

  @Before
  public void setUp() throws Exception {
    mtime = new MillisTime(TIME);
  }

  @Test
  public void testGetTimeID() {
    assertEquals('L', TabletTime.getTimeID(TimeType.LOGICAL));
    assertEquals('M', TabletTime.getTimeID(TimeType.MILLIS));
  }

  @Test
  public void testSetSystemTimes() {
    ServerMutation m = createMock(ServerMutation.class);
    long lastCommitTime = 1234L;
    m.setSystemTimestamp(lastCommitTime);
    replay(m);
    mtime.setSystemTimes(m, lastCommitTime);
    verify(m);
  }

  @Test
  public void testGetInstance_Logical() {
    TabletTime t = TabletTime.getInstance("L1234");
    assertEquals(LogicalTime.class, t.getClass());
    assertEquals("L1234", t.getMetadataValue());
  }

  @Test
  public void testGetInstance_Millis() {
    TabletTime t = TabletTime.getInstance("M1234");
    assertEquals(MillisTime.class, t.getClass());
    assertEquals("M1234", t.getMetadataValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetInstance_InvalidType() {
    TabletTime.getInstance("X1234");
  }

  @Test(expected = NumberFormatException.class)
  public void testGetInstance_Logical_ParseFailure() {
    TabletTime.getInstance("LABCD");
  }

  @Test(expected = NumberFormatException.class)
  public void testGetInstance_Millis_ParseFailure() {
    TabletTime.getInstance("MABCD");
  }

  @Test
  public void testMaxMetadataTime_Logical() {
    assertEquals("L5678", TabletTime.maxMetadataTime("L1234", "L5678"));
    assertEquals("L5678", TabletTime.maxMetadataTime("L5678", "L1234"));
    assertEquals("L5678", TabletTime.maxMetadataTime("L5678", "L5678"));
  }

  @Test
  public void testMaxMetadataTime_Millis() {
    assertEquals("M5678", TabletTime.maxMetadataTime("M1234", "M5678"));
    assertEquals("M5678", TabletTime.maxMetadataTime("M5678", "M1234"));
    assertEquals("M5678", TabletTime.maxMetadataTime("M5678", "M5678"));
  }

  @Test
  public void testMaxMetadataTime_Null1() {
    assertEquals("L5678", TabletTime.maxMetadataTime(null, "L5678"));
    assertEquals("M5678", TabletTime.maxMetadataTime(null, "M5678"));
  }

  @Test
  public void testMaxMetadataTime_Null2() {
    assertEquals("L5678", TabletTime.maxMetadataTime("L5678", null));
    assertEquals("M5678", TabletTime.maxMetadataTime("M5678", null));
  }

  @Test
  @org.junit.Ignore
  public void testMaxMetadataTime_Null3() {
    assertNull(TabletTime.maxMetadataTime(null, null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxMetadataTime_Null1_Invalid() {
    TabletTime.maxMetadataTime(null, "X5678");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxMetadataTime_Null2_Invalid() {
    TabletTime.maxMetadataTime("X5678", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxMetadataTime_Invalid1() {
    TabletTime.maxMetadataTime("X1234", "L5678");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxMetadataTime_Invalid2() {
    TabletTime.maxMetadataTime("L1234", "X5678");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxMetadataTime_DifferentTypes1() {
    TabletTime.maxMetadataTime("L1234", "M5678");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxMetadataTime_DifferentTypes2() {
    TabletTime.maxMetadataTime("X1234", "Y5678");
  }

  @Test(expected = NumberFormatException.class)
  public void testMaxMetadataTime_ParseFailure1() {
    TabletTime.maxMetadataTime("L1234", "LABCD");
  }

  @Test(expected = NumberFormatException.class)
  public void testMaxMetadataTime_ParseFailure2() {
    TabletTime.maxMetadataTime("LABCD", "L5678");
  }
}
