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
package org.apache.accumulo.server.tablets;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.tablets.TabletTime.LogicalTime;
import org.apache.accumulo.server.tablets.TabletTime.MillisTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TabletTimeTest {
  private static final long TIME = 1234L;
  private MillisTime mtime;
  private static final MetadataTime m1234 = new MetadataTime(1234, TimeType.MILLIS);
  private static final MetadataTime m5678 = new MetadataTime(5678, TimeType.MILLIS);
  private static final MetadataTime l1234 = new MetadataTime(1234, TimeType.LOGICAL);
  private static final MetadataTime l5678 = new MetadataTime(5678, TimeType.LOGICAL);

  @BeforeEach
  public void setUp() {
    mtime = new MillisTime(TIME);
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
    TabletTime t = TabletTime.getInstance(new MetadataTime(1234, TimeType.LOGICAL));
    assertEquals(LogicalTime.class, t.getClass());
    assertEquals("L1234", t.getMetadataTime().encode());
  }

  @Test
  public void testGetInstance_Millis() {
    TabletTime t = TabletTime.getInstance(new MetadataTime(1234, TimeType.MILLIS));
    assertEquals(MillisTime.class, t.getClass());
    assertEquals("M1234", t.getMetadataTime().encode());
  }

  @Test
  public void testMaxMetadataTime_Logical() {
    assertEquals(l5678, TabletTime.maxMetadataTime(l1234, l5678));
    assertEquals(l5678, TabletTime.maxMetadataTime(l5678, l1234));
    assertEquals(l5678, TabletTime.maxMetadataTime(l5678, l5678));
  }

  @Test
  public void testMaxMetadataTime_Millis() {
    assertEquals(m5678, TabletTime.maxMetadataTime(m1234, m5678));
    assertEquals(m5678, TabletTime.maxMetadataTime(m5678, m1234));
    assertEquals(m5678, TabletTime.maxMetadataTime(m5678, m5678));
  }

  @Test
  public void testMaxMetadataTime_Null1() {
    assertEquals(l5678, TabletTime.maxMetadataTime(null, l5678));
    assertEquals(m5678, TabletTime.maxMetadataTime(null, m5678));
  }

  @Test
  public void testMaxMetadataTime_Null2() {
    assertEquals(l5678, TabletTime.maxMetadataTime(l5678, null));
    assertEquals(m5678, TabletTime.maxMetadataTime(m5678, null));
  }

  @Test
  public void testMaxMetadataTime_Null3() {
    MetadataTime nullTime = null;
    assertNull(TabletTime.maxMetadataTime(nullTime, nullTime));
  }

  @Test
  public void testMaxMetadataTime_DifferentTypes1() {
    assertThrows(IllegalArgumentException.class, () -> TabletTime.maxMetadataTime(l1234, m5678));
  }

  @Test
  public void testMaxMetadataTime_DifferentTypes2() {
    assertThrows(IllegalArgumentException.class, () -> TabletTime.maxMetadataTime(m1234, l5678));
  }

}
