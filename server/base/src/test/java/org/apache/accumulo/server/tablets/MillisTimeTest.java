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

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.tablets.TabletTime.MillisTime;
import org.junit.Before;
import org.junit.Test;

public class MillisTimeTest {
  private static final long TIME = 1234L;
  private MillisTime mtime;

  @Before
  public void setUp() throws Exception {
    mtime = new MillisTime(TIME);
  }

  @Test
  public void testGetMetadataValue() {
    assertEquals("M1234", mtime.getMetadataValue());
  }

  @Test
  public void testUseMaxTimeFromWALog_Yes() {
    mtime.useMaxTimeFromWALog(5678L);
    assertEquals("M5678", mtime.getMetadataValue());
  }

  @Test
  public void testUseMaxTimeFromWALog_No() {
    mtime.useMaxTimeFromWALog(0L);
    assertEquals("M1234", mtime.getMetadataValue());
  }

  @Test
  public void testSetUpdateTimes() {
    List<Mutation> ms = new java.util.ArrayList<>();
    ServerMutation m = createMock(ServerMutation.class);
    m.setSystemTimestamp(anyLong());
    replay(m);
    ms.add(m);
    long currTime = mtime.setUpdateTimes(ms);
    assertTrue(currTime > TIME);
    verify(m);
  }

  @Test
  public void testSetUpdateTimes_NoMutations() {
    List<Mutation> ms = new java.util.ArrayList<>();
    assertEquals(TIME, mtime.setUpdateTimes(ms));
  }

  @Test
  public void testGetTime() {
    assertEquals(TIME, mtime.getTime());
  }

  @Test
  public void testGetAndUpdateTime() {
    long currTime = mtime.getAndUpdateTime();
    assertTrue(currTime > TIME);
    assertEquals(currTime, mtime.getTime());
  }
}
