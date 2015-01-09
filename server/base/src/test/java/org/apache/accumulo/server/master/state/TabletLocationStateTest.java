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
package org.apache.accumulo.server.master.state;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TabletLocationStateTest {
  private static final Collection<String> innerWalogs = new java.util.HashSet<String>();
  private static final Collection<Collection<String>> walogs = new java.util.HashSet<Collection<String>>();

  @BeforeClass
  public static void setUpClass() {
    walogs.add(innerWalogs);
    innerWalogs.add("somelog");
  }

  private KeyExtent keyExtent;
  private TServerInstance future;
  private TServerInstance current;
  private TServerInstance last;
  private TabletLocationState tls;

  @Before
  public void setUp() throws Exception {
    keyExtent = createMock(KeyExtent.class);
    future = createMock(TServerInstance.class);
    current = createMock(TServerInstance.class);
    last = createMock(TServerInstance.class);
  }

  @Test
  public void testConstruction_NoFuture() throws Exception {
    tls = new TabletLocationState(keyExtent, null, current, last, walogs, true);
    assertSame(keyExtent, tls.extent);
    assertNull(tls.future);
    assertSame(current, tls.current);
    assertSame(last, tls.last);
    assertSame(walogs, tls.walogs);
    assertTrue(tls.chopped);
  }

  @Test
  public void testConstruction_NoCurrent() throws Exception {
    tls = new TabletLocationState(keyExtent, future, null, last, walogs, true);
    assertSame(keyExtent, tls.extent);
    assertSame(future, tls.future);
    assertNull(tls.current);
    assertSame(last, tls.last);
    assertSame(walogs, tls.walogs);
    assertTrue(tls.chopped);
  }

  @Test(expected = TabletLocationState.BadLocationStateException.class)
  public void testConstruction_FutureAndCurrent() throws Exception {
    expect(keyExtent.getMetadataEntry()).andReturn(new Text("entry"));
    replay(keyExtent);
    try {
      new TabletLocationState(keyExtent, future, current, last, walogs, true);
    } catch (TabletLocationState.BadLocationStateException e) {
      assertEquals(new Text("entry"), e.getEncodedEndRow());
      throw (e);
    }
  }

  @Test
  public void testConstruction_NoFuture_NoWalogs() throws Exception {
    tls = new TabletLocationState(keyExtent, null, current, last, null, true);
    assertNotNull(tls.walogs);
    assertEquals(0, tls.walogs.size());
  }

  @Test
  public void testGetServer_Current() throws Exception {
    tls = new TabletLocationState(keyExtent, null, current, last, walogs, true);
    assertSame(current, tls.getServer());
  }

  @Test
  public void testGetServer_Future() throws Exception {
    tls = new TabletLocationState(keyExtent, future, null, last, walogs, true);
    assertSame(future, tls.getServer());
  }

  @Test
  public void testGetServer_Last() throws Exception {
    tls = new TabletLocationState(keyExtent, null, null, last, walogs, true);
    assertSame(last, tls.getServer());
  }

  @Test
  public void testGetServer_None() throws Exception {
    tls = new TabletLocationState(keyExtent, null, null, null, walogs, true);
    assertNull(tls.getServer());
  }

  @Test
  public void testGetState_Unassigned1() throws Exception {
    tls = new TabletLocationState(keyExtent, null, null, null, walogs, true);
    assertEquals(TabletState.UNASSIGNED, tls.getState(null));
  }

  @Test
  public void testGetState_Unassigned2() throws Exception {
    tls = new TabletLocationState(keyExtent, null, null, last, walogs, true);
    assertEquals(TabletState.UNASSIGNED, tls.getState(null));
  }

  @Test
  public void testGetState_Assigned() throws Exception {
    Set<TServerInstance> liveServers = new java.util.HashSet<TServerInstance>();
    liveServers.add(future);
    tls = new TabletLocationState(keyExtent, future, null, last, walogs, true);
    assertEquals(TabletState.ASSIGNED, tls.getState(liveServers));
  }

  @Test
  public void testGetState_Hosted() throws Exception {
    Set<TServerInstance> liveServers = new java.util.HashSet<TServerInstance>();
    liveServers.add(current);
    tls = new TabletLocationState(keyExtent, null, current, last, walogs, true);
    assertEquals(TabletState.HOSTED, tls.getState(liveServers));
  }

  @Test
  public void testGetState_Dead1() throws Exception {
    Set<TServerInstance> liveServers = new java.util.HashSet<TServerInstance>();
    liveServers.add(current);
    tls = new TabletLocationState(keyExtent, future, null, last, walogs, true);
    assertEquals(TabletState.ASSIGNED_TO_DEAD_SERVER, tls.getState(liveServers));
  }

  @Test
  public void testGetState_Dead2() throws Exception {
    Set<TServerInstance> liveServers = new java.util.HashSet<TServerInstance>();
    liveServers.add(future);
    tls = new TabletLocationState(keyExtent, null, current, last, walogs, true);
    assertEquals(TabletState.ASSIGNED_TO_DEAD_SERVER, tls.getState(liveServers));
  }
}
