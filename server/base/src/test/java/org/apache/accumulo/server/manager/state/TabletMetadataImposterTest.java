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
package org.apache.accumulo.server.manager.state;

import static org.easymock.EasyMock.createNiceMock;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TabletMetadataImposterTest {

  private KeyExtent keyExtent;
  private Location future;
  private Location current;
  private Location last;
  private TabletMetadata tls;
  private List<LogEntry> walogs = new ArrayList<>();

  @BeforeEach
  public void setUp() {
    keyExtent = createNiceMock(KeyExtent.class);
    TServerInstance futureInstance = createNiceMock(TServerInstance.class);
    future = Location.future(futureInstance);
    TServerInstance currentInstance = createNiceMock(TServerInstance.class);
    current = Location.current(currentInstance);
    TServerInstance lastInstance = createNiceMock(TServerInstance.class);
    last = Location.last(lastInstance);
    walogs.clear();
    walogs.add(new LogEntry(keyExtent, 1234L, "some file name"));
    EasyMock.replay(keyExtent, futureInstance, currentInstance, lastInstance);
  }

  @Test
  public void testConstruction_NoFuture() throws Exception {
    tls = new TabletMetadataImposter(keyExtent, null, current, last, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertSame(keyExtent, tls.getExtent());
    assertTrue(tls.hasCurrent());
    assertSame(current, tls.getLocation());
    assertSame(last, tls.getLast());
    assertSame(walogs, tls.getLogs());
    assertTrue(tls.hasChopped());
  }

  @Test
  public void testConstruction_NoCurrent() throws Exception {
    tls = new TabletMetadataImposter(keyExtent, future, null, last, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertSame(keyExtent, tls.getExtent());
    assertSame(future, tls.getLocation());
    assertFalse(tls.hasCurrent());
    assertSame(last, tls.getLast());
    assertSame(walogs, tls.getLogs());
    assertTrue(tls.hasChopped());
  }

  @Test
  public void testConstruction_FutureAndCurrent() {
    tls = new TabletMetadataImposter(keyExtent, future, current, last, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertTrue(tls.isFutureAndCurrentLocationSet());
  }

  @Test
  public void testConstruction_NoFuture_NoWalogs() throws Exception {
    tls = new TabletMetadataImposter(keyExtent, null, current, last, null, null, true,
        TabletHostingGoal.ONDEMAND, false);
    assertNotNull(tls.getLogs());
    assertEquals(0, tls.getLogs().size());
  }

  @Test
  public void testGetServer_Current() throws Exception {
    tls = new TabletMetadataImposter(keyExtent, null, current, last, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertTrue(tls.hasCurrent());
    assertSame(current, tls.getLocation());
  }

  @Test
  public void testGetServer_Future() throws Exception {
    tls = new TabletMetadataImposter(keyExtent, future, null, last, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertFalse(tls.hasCurrent());
    assertSame(future, tls.getLocation());
  }

  @Test
  public void testGetServer_Last() throws Exception {
    tls = new TabletMetadataImposter(keyExtent, null, null, last, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertFalse(tls.hasCurrent());
    assertNull(tls.getLocation());
    assertSame(last, tls.getLast());
  }

  @Test
  public void testGetServer_None() throws Exception {
    tls = new TabletMetadataImposter(keyExtent, null, null, null, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertFalse(tls.hasCurrent());
    assertNull(tls.getLocation());
  }

  @Test
  public void testGetState_Unassigned1() throws Exception {
    tls = new TabletMetadataImposter(keyExtent, null, null, null, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertEquals(TabletState.UNASSIGNED, tls.getTabletState(null));
  }

  @Test
  public void testGetState_Unassigned2() throws Exception {
    tls = new TabletMetadataImposter(keyExtent, null, null, last, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertEquals(TabletState.UNASSIGNED, tls.getTabletState(null));
  }

  @Test
  public void testGetState_Assigned() throws Exception {
    Set<TServerInstance> liveServers = new java.util.HashSet<>();
    liveServers.add(future.getServerInstance());
    tls = new TabletMetadataImposter(keyExtent, future, null, last, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertEquals(TabletState.ASSIGNED, tls.getTabletState(liveServers));
  }

  @Test
  public void testGetState_Hosted() throws Exception {
    Set<TServerInstance> liveServers = new java.util.HashSet<>();
    liveServers.add(current.getServerInstance());
    tls = new TabletMetadataImposter(keyExtent, null, current, last, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertEquals(TabletState.HOSTED, tls.getTabletState(liveServers));
  }

  @Test
  public void testGetState_Dead1() throws Exception {
    Set<TServerInstance> liveServers = new java.util.HashSet<>();
    liveServers.add(current.getServerInstance());
    tls = new TabletMetadataImposter(keyExtent, future, null, last, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertEquals(TabletState.ASSIGNED_TO_DEAD_SERVER, tls.getTabletState(liveServers));
  }

  @Test
  public void testGetState_Dead2() throws Exception {
    Set<TServerInstance> liveServers = new java.util.HashSet<>();
    liveServers.add(future.getServerInstance());
    tls = new TabletMetadataImposter(keyExtent, null, current, last, null, walogs, true,
        TabletHostingGoal.ONDEMAND, false);
    assertEquals(TabletState.ASSIGNED_TO_DEAD_SERVER, tls.getTabletState(liveServers));
  }
}
