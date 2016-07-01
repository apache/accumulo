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

import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.tablets.TabletTime.LogicalTime;
import org.junit.Before;
import org.junit.Test;

public class LogicalTimeTest {
  private static final long TIME = 1234L;
  private LogicalTime ltime;

  @Before
  public void setUp() throws Exception {
    ltime = (LogicalTime) TabletTime.getInstance("L1234");
  }

  @Test
  public void testGetMetadataValue() {
    assertEquals("L1234", ltime.getMetadataValue());
  }

  @Test
  public void testUseMaxTimeFromWALog_Update() {
    ltime.useMaxTimeFromWALog(5678L);
    assertEquals("L5678", ltime.getMetadataValue());
  }

  @Test
  public void testUseMaxTimeFromWALog_NoUpdate() {
    ltime.useMaxTimeFromWALog(0L);
    assertEquals("L1234", ltime.getMetadataValue());
  }

  @Test
  public void testSetUpdateTimes() {
    List<Mutation> ms = new java.util.ArrayList<>();
    ServerMutation m = createMock(ServerMutation.class);
    ServerMutation m2 = createMock(ServerMutation.class);
    m.setSystemTimestamp(1235L);
    replay(m);
    m2.setSystemTimestamp(1236L);
    replay(m2);
    ms.add(m);
    ms.add(m2);
    long currTime = ltime.setUpdateTimes(ms);
    assertEquals(TIME + 2L, currTime);
    verify(m);
    verify(m2);
  }

  @Test
  public void testSetUpdateTimes_NoMutations() {
    List<Mutation> ms = new java.util.ArrayList<>();
    assertEquals(TIME, ltime.setUpdateTimes(ms));
  }

  @Test
  public void testGetTime() {
    assertEquals(TIME, ltime.getTime());
  }

  @Test
  public void testGetAndUpdateTime() {
    long currTime = ltime.getAndUpdateTime();
    assertEquals(TIME + 1L, currTime);
  }
}
