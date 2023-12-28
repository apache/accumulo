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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TabletLocationStateTest {
  private static final Set<LogEntry> walogs =
      Set.of(LogEntry.fromPath("file:///dir/tserver+9997/" + UUID.randomUUID()));

  private KeyExtent keyExtent;
  private Location future;
  private Location current;
  private Location last;
  private TabletLocationState tls;

  @BeforeEach
  public void setUp() {
    keyExtent = createMock(KeyExtent.class);
    future = Location.future(createMock(TServerInstance.class));
    current = Location.current(createMock(TServerInstance.class));
    last = Location.last(createMock(TServerInstance.class));
  }

  @Test
  public void testConstruction_NoFuture() throws Exception {
    tls = new TabletLocationState(keyExtent, null, current, last, null, walogs);
    assertSame(keyExtent, tls.extent);
    assertNull(tls.future);
    assertSame(current, tls.current);
    assertSame(last, tls.last);
    assertSame(walogs, tls.walogs);
  }

  @Test
  public void testConstruction_NoCurrent() throws Exception {
    tls = new TabletLocationState(keyExtent, future, null, last, null, walogs);
    assertSame(keyExtent, tls.extent);
    assertSame(future, tls.future);
    assertNull(tls.current);
    assertSame(last, tls.last);
    assertSame(walogs, tls.walogs);
  }

  @Test
  public void testConstruction_FutureAndCurrent() {
    expect(keyExtent.toMetaRow()).andReturn(new Text("entry"));
    replay(keyExtent);
    var e = assertThrows(TabletLocationState.BadLocationStateException.class,
        () -> new TabletLocationState(keyExtent, future, current, last, null, walogs));
    assertEquals(new Text("entry"), e.getEncodedEndRow());
  }

  @Test
  public void testConstruction_NoFuture_NoWalogs() throws Exception {
    tls = new TabletLocationState(keyExtent, null, current, last, null, null);
    assertNotNull(tls.walogs);
    assertEquals(0, tls.walogs.size());
  }

  @Test
  public void testGetServer_Current() throws Exception {
    tls = new TabletLocationState(keyExtent, null, current, last, null, walogs);
    assertSame(current, tls.getLocation());
  }

  @Test
  public void testGetServer_Future() throws Exception {
    tls = new TabletLocationState(keyExtent, future, null, last, null, walogs);
    assertSame(future, tls.getLocation());
  }

  @Test
  public void testGetServer_Last() throws Exception {
    tls = new TabletLocationState(keyExtent, null, null, last, null, walogs);
    assertSame(last, tls.getLocation());
  }

  @Test
  public void testGetServer_None() throws Exception {
    tls = new TabletLocationState(keyExtent, null, null, null, null, walogs);
    assertNull(tls.getLocation());
  }

  @Test
  public void testGetState_Unassigned1() throws Exception {
    tls = new TabletLocationState(keyExtent, null, null, null, null, walogs);
    assertEquals(TabletState.UNASSIGNED, tls.getState(null));
  }

  @Test
  public void testGetState_Unassigned2() throws Exception {
    tls = new TabletLocationState(keyExtent, null, null, last, null, walogs);
    assertEquals(TabletState.UNASSIGNED, tls.getState(null));
  }

  @Test
  public void testGetState_Assigned() throws Exception {
    Set<TServerInstance> liveServers = new java.util.HashSet<>();
    liveServers.add(future.getServerInstance());
    tls = new TabletLocationState(keyExtent, future, null, last, null, walogs);
    assertEquals(TabletState.ASSIGNED, tls.getState(liveServers));
  }

  @Test
  public void testGetState_Hosted() throws Exception {
    Set<TServerInstance> liveServers = new java.util.HashSet<>();
    liveServers.add(current.getServerInstance());
    tls = new TabletLocationState(keyExtent, null, current, last, null, walogs);
    assertEquals(TabletState.HOSTED, tls.getState(liveServers));
  }

  @Test
  public void testGetState_Dead1() throws Exception {
    Set<TServerInstance> liveServers = new java.util.HashSet<>();
    liveServers.add(current.getServerInstance());
    tls = new TabletLocationState(keyExtent, future, null, last, null, walogs);
    assertEquals(TabletState.ASSIGNED_TO_DEAD_SERVER, tls.getState(liveServers));
  }

  @Test
  public void testGetState_Dead2() throws Exception {
    Set<TServerInstance> liveServers = new java.util.HashSet<>();
    liveServers.add(future.getServerInstance());
    tls = new TabletLocationState(keyExtent, null, current, last, null, walogs);
    assertEquals(TabletState.ASSIGNED_TO_DEAD_SERVER, tls.getState(liveServers));
  }
}
