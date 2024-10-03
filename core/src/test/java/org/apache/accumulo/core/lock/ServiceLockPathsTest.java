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
package org.apache.accumulo.core.lock;

import static org.apache.accumulo.core.Constants.DEFAULT_RESOURCE_GROUP_NAME;
import static org.apache.accumulo.core.Constants.ZCOMPACTORS;
import static org.apache.accumulo.core.Constants.ZDEADTSERVERS;
import static org.apache.accumulo.core.Constants.ZGC_LOCK;
import static org.apache.accumulo.core.Constants.ZMANAGER_LOCK;
import static org.apache.accumulo.core.Constants.ZMINI_LOCK;
import static org.apache.accumulo.core.Constants.ZMONITOR_LOCK;
import static org.apache.accumulo.core.Constants.ZSSERVERS;
import static org.apache.accumulo.core.Constants.ZTABLE_LOCKS;
import static org.apache.accumulo.core.Constants.ZTSERVERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ServiceLockPathsTest {

  private static final String ROOT = "/accumulo/instance_id";
  private static final String TEST_RESOURCE_GROUP = "TEST_RG";
  private static final String HOSTNAME = "localhost:9876";
  private static final String HOSTNAME_NO_LOCK = "localhost:9877";
  private static final HostAndPort hp = HostAndPort.fromString(HOSTNAME);

  @Test
  public void testPathGeneration() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();

    EasyMock.replay(ctx);

    ServiceLockPaths paths = new ServiceLockPaths(ctx);
    // Test management process path creation
    ServiceLockPath slp = paths.createGarbageCollectorPath();
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZGC_LOCK, slp.getType());
    assertEquals(ROOT + ZGC_LOCK, slp.toString());

    slp = paths.createManagerPath();
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMANAGER_LOCK, slp.getType());
    assertEquals(ROOT + ZMANAGER_LOCK, slp.toString());

    assertThrows(NullPointerException.class, () -> paths.createMiniPath(null));
    String miniUUID = UUID.randomUUID().toString();
    slp = paths.createMiniPath(miniUUID);
    assertEquals(miniUUID, slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMINI_LOCK, slp.getType());
    assertEquals(ROOT + ZMINI_LOCK + "/" + miniUUID, slp.toString());

    slp = paths.createMonitorPath();
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMONITOR_LOCK, slp.getType());
    assertEquals(ROOT + ZMONITOR_LOCK, slp.toString());

    // Test worker process path creation
    assertThrows(NullPointerException.class, () -> paths.createCompactorPath(null, null));
    assertThrows(NullPointerException.class,
        () -> paths.createCompactorPath(TEST_RESOURCE_GROUP, null));
    slp = paths.createCompactorPath(TEST_RESOURCE_GROUP, hp);
    assertEquals(HOSTNAME, slp.getServer());
    assertEquals(TEST_RESOURCE_GROUP, slp.getResourceGroup());
    assertEquals(ZCOMPACTORS, slp.getType());
    assertEquals(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp.toString());

    assertThrows(NullPointerException.class, () -> paths.createDeadTabletServerPath(null, null));
    assertThrows(NullPointerException.class,
        () -> paths.createDeadTabletServerPath(TEST_RESOURCE_GROUP, null));
    slp = paths.createDeadTabletServerPath(TEST_RESOURCE_GROUP, hp);
    assertEquals(HOSTNAME, slp.getServer());
    assertEquals(TEST_RESOURCE_GROUP, slp.getResourceGroup());
    assertEquals(ZDEADTSERVERS, slp.getType());
    assertEquals(ROOT + ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp.toString());

    assertThrows(NullPointerException.class, () -> paths.createScanServerPath(null, null));
    assertThrows(NullPointerException.class,
        () -> paths.createScanServerPath(TEST_RESOURCE_GROUP, null));
    slp = paths.createScanServerPath(TEST_RESOURCE_GROUP, hp);
    assertEquals(HOSTNAME, slp.getServer());
    assertEquals(TEST_RESOURCE_GROUP, slp.getResourceGroup());
    assertEquals(ZSSERVERS, slp.getType());
    assertEquals(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp.toString());

    assertThrows(NullPointerException.class, () -> paths.createTabletServerPath(null, null));
    assertThrows(NullPointerException.class,
        () -> paths.createTabletServerPath(TEST_RESOURCE_GROUP, null));
    slp = paths.createTabletServerPath(TEST_RESOURCE_GROUP, hp);
    assertEquals(HOSTNAME, slp.getServer());
    assertEquals(TEST_RESOURCE_GROUP, slp.getResourceGroup());
    assertEquals(ZTSERVERS, slp.getType());
    assertEquals(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp.toString());

    EasyMock.verify(ctx);
  }

  @Test
  public void testGetGarbageCollectorNotRunning() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZGC_LOCK)).andReturn(List.of()).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    ServiceLockPath slp = ctx.getServerPaths().getGarbageCollector(true);
    assertNull(slp);

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetGarbageCollectorNoLock() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZGC_LOCK)).andReturn(List.of(HOSTNAME_NO_LOCK))
        .anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    ServiceLockPath slp = ctx.getServerPaths().getGarbageCollector(false);
    assertNotNull(slp);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZGC_LOCK, slp.getType());
    assertEquals(ROOT + ZGC_LOCK, slp.toString());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetGarbageCollector() {

    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    ServiceLockData sld =
        new ServiceLockData(uuid, HOSTNAME, ThriftService.GC, TEST_RESOURCE_GROUP);

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZGC_LOCK)).andReturn(List.of(svcLock1, svcLock2))
        .anyTimes();
    EasyMock
        .expect(zc.get(EasyMock.eq(ROOT + ZGC_LOCK + "/" + svcLock1), EasyMock.isA(ZcStat.class)))
        .andReturn(sld.serialize());
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    ServiceLockPath slp = ctx.getServerPaths().getGarbageCollector(true);
    assertNotNull(slp);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZGC_LOCK, slp.getType());
    assertEquals(ROOT + ZGC_LOCK, slp.toString());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetManagerNotRunning() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZMANAGER_LOCK)).andReturn(List.of()).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    ServiceLockPath slp = ctx.getServerPaths().getManager(true);
    assertNull(slp);

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetManagerNoLock() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZMANAGER_LOCK)).andReturn(List.of(HOSTNAME_NO_LOCK))
        .anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    ServiceLockPath slp = ctx.getServerPaths().getManager(false);
    assertNotNull(slp);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMANAGER_LOCK, slp.getType());
    assertEquals(ROOT + ZMANAGER_LOCK, slp.toString());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetManager() {

    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    ServiceLockData sld =
        new ServiceLockData(uuid, HOSTNAME, ThriftService.MANAGER, TEST_RESOURCE_GROUP);

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZMANAGER_LOCK)).andReturn(List.of(svcLock1, svcLock2))
        .anyTimes();
    EasyMock
        .expect(
            zc.get(EasyMock.eq(ROOT + ZMANAGER_LOCK + "/" + svcLock1), EasyMock.isA(ZcStat.class)))
        .andReturn(sld.serialize());
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    ServiceLockPath slp = ctx.getServerPaths().getManager(true);
    assertNotNull(slp);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMANAGER_LOCK, slp.getType());
    assertEquals(ROOT + ZMANAGER_LOCK, slp.toString());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetMonitorNotRunning() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZMONITOR_LOCK)).andReturn(List.of()).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    ServiceLockPath slp = ctx.getServerPaths().getMonitor(true);
    assertNull(slp);

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetMonitorNoLock() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZMONITOR_LOCK)).andReturn(List.of(HOSTNAME_NO_LOCK))
        .anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    ServiceLockPath slp = ctx.getServerPaths().getMonitor(false);
    assertNotNull(slp);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMONITOR_LOCK, slp.getType());
    assertEquals(ROOT + ZMONITOR_LOCK, slp.toString());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetMonitor() {

    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    ServiceLockData sld =
        new ServiceLockData(uuid, HOSTNAME, ThriftService.NONE, TEST_RESOURCE_GROUP);

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZMONITOR_LOCK)).andReturn(List.of(svcLock1, svcLock2))
        .anyTimes();
    EasyMock
        .expect(
            zc.get(EasyMock.eq(ROOT + ZMONITOR_LOCK + "/" + svcLock1), EasyMock.isA(ZcStat.class)))
        .andReturn(sld.serialize());
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    ServiceLockPath slp = ctx.getServerPaths().getMonitor(true);
    assertNotNull(slp);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMONITOR_LOCK, slp.getType());
    assertEquals(ROOT + ZMONITOR_LOCK, slp.toString());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetCompactorsNotRunning() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZCOMPACTORS)).andReturn(List.of()).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class,
        () -> ctx.getServerPaths().getCompactor(null, null, true));
    assertThrows(NullPointerException.class,
        () -> ctx.getServerPaths().getCompactor(Optional.of(TEST_RESOURCE_GROUP), null, true));
    assertTrue(
        ctx.getServerPaths().getCompactor(Optional.empty(), Optional.empty(), true).isEmpty());
    assertTrue(ctx.getServerPaths()
        .getCompactor(Optional.of(TEST_RESOURCE_GROUP), Optional.empty(), true).isEmpty());
    assertTrue(ctx.getServerPaths()
        .getCompactor(Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp), true).isEmpty());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetCompactors() {

    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    ServiceLockData sld1 =
        new ServiceLockData(uuid, HOSTNAME, ThriftService.COMPACTOR, TEST_RESOURCE_GROUP);
    ServiceLockData sld2 =
        new ServiceLockData(uuid, HOSTNAME, ThriftService.COMPACTOR, DEFAULT_RESOURCE_GROUP_NAME);

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZCOMPACTORS))
        .andReturn(List.of(TEST_RESOURCE_GROUP, DEFAULT_RESOURCE_GROUP_NAME)).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP))
        .andReturn(List.of(HOSTNAME, HOSTNAME_NO_LOCK)).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZCOMPACTORS + "/" + DEFAULT_RESOURCE_GROUP_NAME))
        .andReturn(List.of(HOSTNAME, HOSTNAME_NO_LOCK)).anyTimes();
    EasyMock
        .expect(
            zc.getChildren(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME_NO_LOCK))
        .andReturn(List.of()).anyTimes();
    EasyMock
        .expect(zc.getChildren(
            ROOT + ZCOMPACTORS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME_NO_LOCK))
        .andReturn(List.of()).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    EasyMock
        .expect(
            zc.getChildren(ROOT + ZCOMPACTORS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    EasyMock.expect(zc.get(
        EasyMock
            .eq(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld1.serialize()).anyTimes();
    EasyMock
        .expect(zc.get(EasyMock.eq(ROOT + ZCOMPACTORS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/"
            + HOSTNAME + "/" + svcLock1), EasyMock.isA(ZcStat.class)))
        .andReturn(sld2.serialize()).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    // query for all
    Set<ServiceLockPath> results =
        ctx.getServerPaths().getCompactor(Optional.empty(), Optional.empty(), false);
    assertEquals(4, results.size());
    for (ServiceLockPath path : results) {
      assertEquals(ZCOMPACTORS, path.getType());
      assertTrue(path.getServer().equals(HOSTNAME) || path.getServer().equals(HOSTNAME_NO_LOCK));
      assertTrue(path.getResourceGroup().equals(DEFAULT_RESOURCE_GROUP_NAME)
          || path.getResourceGroup().equals(TEST_RESOURCE_GROUP));
      assertTrue(path.toString()
          .equals(ROOT + ZCOMPACTORS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME)
          || path.toString().equals(
              ROOT + ZCOMPACTORS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME_NO_LOCK)
          || path.toString().equals(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME)
          || path.toString()
              .equals(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME_NO_LOCK));
    }

    // query for all with locks
    results = ctx.getServerPaths().getCompactor(Optional.empty(), Optional.empty(), true);
    assertEquals(2, results.size());
    Iterator<ServiceLockPath> iter = results.iterator();
    ServiceLockPath slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZCOMPACTORS, slp1.getType());
    if (slp1.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME,
          slp1.toString());
    } else if (slp1.getResourceGroup().equals(DEFAULT_RESOURCE_GROUP_NAME)) {
      assertEquals(ROOT + ZCOMPACTORS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME,
          slp1.toString());
    } else {
      fail("wrong resource group");
    }
    ServiceLockPath slp2 = iter.next();
    assertEquals(HOSTNAME, slp2.getServer());
    assertEquals(ZCOMPACTORS, slp2.getType());
    if (slp2.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME,
          slp2.toString());
    } else if (slp2.getResourceGroup().equals(DEFAULT_RESOURCE_GROUP_NAME)) {
      assertEquals(ROOT + ZCOMPACTORS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME,
          slp2.toString());
    } else {
      fail("wrong resource group");
    }

    // query for all in non-existent resource group
    results = ctx.getServerPaths().getCompactor(Optional.of("FAKE_RESOURCE_GROUP"),
        Optional.empty(), true);
    assertEquals(0, results.size());

    // query for all in test resource group
    results =
        ctx.getServerPaths().getCompactor(Optional.of(TEST_RESOURCE_GROUP), Optional.empty(), true);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZCOMPACTORS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());

    // query for a specific server
    results =
        ctx.getServerPaths().getCompactor(Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp), true);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZCOMPACTORS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());

    // query for a wrong server
    results = ctx.getServerPaths().getCompactor(Optional.of(TEST_RESOURCE_GROUP),
        Optional.of(HostAndPort.fromString("localhost:1234")), true);
    assertEquals(0, results.size());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetScanServersNotRunning() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZSSERVERS)).andReturn(List.of()).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class,
        () -> ctx.getServerPaths().getScanServer(null, null, true));
    assertThrows(NullPointerException.class,
        () -> ctx.getServerPaths().getScanServer(Optional.of(TEST_RESOURCE_GROUP), null, true));
    assertTrue(
        ctx.getServerPaths().getScanServer(Optional.empty(), Optional.empty(), true).isEmpty());
    assertTrue(ctx.getServerPaths()
        .getScanServer(Optional.of(TEST_RESOURCE_GROUP), Optional.empty(), true).isEmpty());
    assertTrue(ctx.getServerPaths()
        .getScanServer(Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp), true).isEmpty());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetScanServers() {

    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    ServiceLockData sld1 =
        new ServiceLockData(uuid, HOSTNAME, ThriftService.TABLET_SCAN, TEST_RESOURCE_GROUP);
    ServiceLockData sld2 =
        new ServiceLockData(uuid, HOSTNAME, ThriftService.TABLET_SCAN, DEFAULT_RESOURCE_GROUP_NAME);

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZSSERVERS))
        .andReturn(List.of(TEST_RESOURCE_GROUP, DEFAULT_RESOURCE_GROUP_NAME)).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP))
        .andReturn(List.of(HOSTNAME, HOSTNAME_NO_LOCK)).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZSSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME))
        .andReturn(List.of(HOSTNAME, HOSTNAME_NO_LOCK)).anyTimes();
    EasyMock
        .expect(
            zc.getChildren(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME_NO_LOCK))
        .andReturn(List.of()).anyTimes();
    EasyMock
        .expect(zc.getChildren(
            ROOT + ZSSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME_NO_LOCK))
        .andReturn(List.of()).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    EasyMock
        .expect(
            zc.getChildren(ROOT + ZSSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    EasyMock.expect(zc.get(
        EasyMock.eq(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld1.serialize()).anyTimes();
    EasyMock.expect(zc.get(
        EasyMock.eq(
            ROOT + ZSSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld2.serialize()).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    // query for all
    Set<ServiceLockPath> results =
        ctx.getServerPaths().getScanServer(Optional.empty(), Optional.empty(), false);
    assertEquals(4, results.size());
    for (ServiceLockPath path : results) {
      assertEquals(ZSSERVERS, path.getType());
      assertTrue(path.getServer().equals(HOSTNAME) || path.getServer().equals(HOSTNAME_NO_LOCK));
      assertTrue(path.getResourceGroup().equals(DEFAULT_RESOURCE_GROUP_NAME)
          || path.getResourceGroup().equals(TEST_RESOURCE_GROUP));
      assertTrue(path.toString()
          .equals(ROOT + ZSSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME)
          || path.toString()
              .equals(ROOT + ZSSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME_NO_LOCK)
          || path.toString().equals(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME)
          || path.toString()
              .equals(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME_NO_LOCK));
    }

    // query for all with lock
    results = ctx.getServerPaths().getScanServer(Optional.empty(), Optional.empty(), true);
    assertEquals(2, results.size());
    Iterator<ServiceLockPath> iter = results.iterator();
    ServiceLockPath slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZSSERVERS, slp1.getType());
    if (slp1.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());
    } else if (slp1.getResourceGroup().equals(DEFAULT_RESOURCE_GROUP_NAME)) {
      assertEquals(ROOT + ZSSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME,
          slp1.toString());
    } else {
      fail("wrong resource group");
    }
    ServiceLockPath slp2 = iter.next();
    assertEquals(HOSTNAME, slp2.getServer());
    assertEquals(ZSSERVERS, slp2.getType());
    if (slp2.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp2.toString());
    } else if (slp2.getResourceGroup().equals(DEFAULT_RESOURCE_GROUP_NAME)) {
      assertEquals(ROOT + ZSSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME,
          slp2.toString());
    } else {
      fail("wrong resource group");
    }

    // query for all in non-existent resource group
    results = ctx.getServerPaths().getScanServer(Optional.of("FAKE_RESOURCE_GROUP"),
        Optional.empty(), true);
    assertEquals(0, results.size());

    // query for all in test resource group
    results = ctx.getServerPaths().getScanServer(Optional.of(TEST_RESOURCE_GROUP), Optional.empty(),
        true);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZSSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());

    // query for a specific server
    results =
        ctx.getServerPaths().getScanServer(Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp), true);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZSSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());

    // query for a wrong server
    results = ctx.getServerPaths().getScanServer(Optional.of(TEST_RESOURCE_GROUP),
        Optional.of(HostAndPort.fromString("localhost:1234")), true);
    assertEquals(0, results.size());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetTabletServersNotRunning() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZTSERVERS)).andReturn(List.of()).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class,
        () -> ctx.getServerPaths().getTabletServer(null, null, true));
    assertThrows(NullPointerException.class,
        () -> ctx.getServerPaths().getTabletServer(Optional.of(TEST_RESOURCE_GROUP), null, true));
    assertTrue(
        ctx.getServerPaths().getTabletServer(Optional.empty(), Optional.empty(), true).isEmpty());
    assertTrue(ctx.getServerPaths()
        .getTabletServer(Optional.of(TEST_RESOURCE_GROUP), Optional.empty(), true).isEmpty());
    assertTrue(ctx.getServerPaths()
        .getTabletServer(Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp), true).isEmpty());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetTabletServers() {

    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    ServiceLockData sld1 =
        new ServiceLockData(uuid, HOSTNAME, ThriftService.TABLET_SCAN, TEST_RESOURCE_GROUP);
    ServiceLockData sld2 =
        new ServiceLockData(uuid, HOSTNAME, ThriftService.TABLET_SCAN, DEFAULT_RESOURCE_GROUP_NAME);

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZTSERVERS))
        .andReturn(List.of(TEST_RESOURCE_GROUP, DEFAULT_RESOURCE_GROUP_NAME)).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP))
        .andReturn(List.of(HOSTNAME, HOSTNAME_NO_LOCK)).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME))
        .andReturn(List.of(HOSTNAME, HOSTNAME_NO_LOCK)).anyTimes();
    EasyMock
        .expect(
            zc.getChildren(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME_NO_LOCK))
        .andReturn(List.of()).anyTimes();
    EasyMock
        .expect(zc.getChildren(
            ROOT + ZTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME_NO_LOCK))
        .andReturn(List.of()).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    EasyMock
        .expect(
            zc.getChildren(ROOT + ZTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    EasyMock.expect(zc.get(
        EasyMock.eq(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld1.serialize()).anyTimes();
    EasyMock.expect(zc.get(
        EasyMock.eq(
            ROOT + ZTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld2.serialize()).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    // query for all
    Set<ServiceLockPath> results =
        ctx.getServerPaths().getTabletServer(Optional.empty(), Optional.empty(), false);
    assertEquals(4, results.size());
    for (ServiceLockPath path : results) {
      assertEquals(ZTSERVERS, path.getType());
      assertTrue(path.getServer().equals(HOSTNAME) || path.getServer().equals(HOSTNAME_NO_LOCK));
      assertTrue(path.getResourceGroup().equals(DEFAULT_RESOURCE_GROUP_NAME)
          || path.getResourceGroup().equals(TEST_RESOURCE_GROUP));
      assertTrue(path.toString()
          .equals(ROOT + ZTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME)
          || path.toString()
              .equals(ROOT + ZTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME_NO_LOCK)
          || path.toString().equals(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME)
          || path.toString()
              .equals(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME_NO_LOCK));
    }

    // query for all with lock
    results = ctx.getServerPaths().getTabletServer(Optional.empty(), Optional.empty(), true);
    assertEquals(2, results.size());
    Iterator<ServiceLockPath> iter = results.iterator();
    ServiceLockPath slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZTSERVERS, slp1.getType());
    if (slp1.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());
    } else if (slp1.getResourceGroup().equals(DEFAULT_RESOURCE_GROUP_NAME)) {
      assertEquals(ROOT + ZTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME,
          slp1.toString());
    } else {
      fail("wrong resource group");
    }
    ServiceLockPath slp2 = iter.next();
    assertEquals(HOSTNAME, slp2.getServer());
    assertEquals(ZTSERVERS, slp2.getType());
    if (slp2.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp2.toString());
    } else if (slp2.getResourceGroup().equals(DEFAULT_RESOURCE_GROUP_NAME)) {
      assertEquals(ROOT + ZTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME,
          slp2.toString());
    } else {
      fail("wrong resource group");
    }

    // query for all in non-existent resource group
    results = ctx.getServerPaths().getTabletServer(Optional.of("FAKE_RESOURCE_GROUP"),
        Optional.empty(), true);
    assertEquals(0, results.size());

    // query for all in test resource group
    results = ctx.getServerPaths().getTabletServer(Optional.of(TEST_RESOURCE_GROUP),
        Optional.empty(), true);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZTSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());

    // query for a specific server
    results = ctx.getServerPaths().getTabletServer(Optional.of(TEST_RESOURCE_GROUP),
        Optional.of(hp), true);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZTSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());

    // query for a wrong server
    results = ctx.getServerPaths().getTabletServer(Optional.of(TEST_RESOURCE_GROUP),
        Optional.of(HostAndPort.fromString("localhost:1234")), true);
    assertEquals(0, results.size());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetDeadTabletServersNone() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZDEADTSERVERS)).andReturn(List.of()).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class,
        () -> ctx.getServerPaths().getDeadTabletServer(null, null, false));
    assertThrows(NullPointerException.class, () -> ctx.getServerPaths()
        .getDeadTabletServer(Optional.of(TEST_RESOURCE_GROUP), null, false));
    assertTrue(ctx.getServerPaths().getDeadTabletServer(Optional.empty(), Optional.empty(), false)
        .isEmpty());
    assertTrue(ctx.getServerPaths()
        .getDeadTabletServer(Optional.of(TEST_RESOURCE_GROUP), Optional.empty(), false).isEmpty());
    assertTrue(ctx.getServerPaths()
        .getDeadTabletServer(Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp), false).isEmpty());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testGetDeadTabletServers() {

    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    ServiceLockData sld1 =
        new ServiceLockData(uuid, HOSTNAME, ThriftService.TABLET_SCAN, TEST_RESOURCE_GROUP);
    ServiceLockData sld2 =
        new ServiceLockData(uuid, HOSTNAME, ThriftService.TABLET_SCAN, DEFAULT_RESOURCE_GROUP_NAME);

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getZooCache()).andReturn(zc).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZDEADTSERVERS))
        .andReturn(List.of(TEST_RESOURCE_GROUP, DEFAULT_RESOURCE_GROUP_NAME)).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP))
        .andReturn(List.of(HOSTNAME)).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZDEADTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME))
        .andReturn(List.of(HOSTNAME)).anyTimes();
    EasyMock
        .expect(zc.getChildren(ROOT + ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    EasyMock
        .expect(zc
            .getChildren(ROOT + ZDEADTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    EasyMock.expect(zc.get(
        EasyMock
            .eq(ROOT + ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld1.serialize()).anyTimes();
    EasyMock
        .expect(zc.get(EasyMock.eq(ROOT + ZDEADTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/"
            + HOSTNAME + "/" + svcLock1), EasyMock.isA(ZcStat.class)))
        .andReturn(sld2.serialize()).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx, zc);

    // query for all
    Set<ServiceLockPath> results =
        ctx.getServerPaths().getDeadTabletServer(Optional.empty(), Optional.empty(), false);
    assertEquals(2, results.size());
    Iterator<ServiceLockPath> iter = results.iterator();
    ServiceLockPath slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZDEADTSERVERS, slp1.getType());
    if (slp1.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ROOT + ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME,
          slp1.toString());
    } else if (slp1.getResourceGroup().equals(DEFAULT_RESOURCE_GROUP_NAME)) {
      assertEquals(ROOT + ZDEADTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME,
          slp1.toString());
    } else {
      fail("wrong resource group");
    }
    ServiceLockPath slp2 = iter.next();
    assertEquals(HOSTNAME, slp2.getServer());
    assertEquals(ZDEADTSERVERS, slp2.getType());
    if (slp2.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ROOT + ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME,
          slp2.toString());
    } else if (slp2.getResourceGroup().equals(DEFAULT_RESOURCE_GROUP_NAME)) {
      assertEquals(ROOT + ZDEADTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME + "/" + HOSTNAME,
          slp2.toString());
    } else {
      fail("wrong resource group");
    }

    // query for all in non-existent resource group
    results = ctx.getServerPaths().getDeadTabletServer(Optional.of("FAKE_RESOURCE_GROUP"),
        Optional.empty(), false);
    assertEquals(0, results.size());

    // query for all in test resource group
    results = ctx.getServerPaths().getDeadTabletServer(Optional.of(TEST_RESOURCE_GROUP),
        Optional.empty(), false);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZDEADTSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME,
        slp1.toString());

    // query for a specific server
    results = ctx.getServerPaths().getDeadTabletServer(Optional.of(TEST_RESOURCE_GROUP),
        Optional.of(hp), false);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZDEADTSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME,
        slp1.toString());

    // query for a wrong server
    results = ctx.getServerPaths().getDeadTabletServer(Optional.of(TEST_RESOURCE_GROUP),
        Optional.of(HostAndPort.fromString("localhost:1234")), false);
    assertEquals(0, results.size());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testTableLocksPath() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx);

    // Only table lock creation is supported because the existing code
    // uses a ServiceLockPath with it.
    ServiceLockPath slp = ctx.getServerPaths().createTableLocksPath();
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZTABLE_LOCKS, slp.getType());
    assertEquals(ROOT + ZTABLE_LOCKS, slp.toString());

    slp = ctx.getServerPaths().createTableLocksPath("1");
    assertEquals("1", slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZTABLE_LOCKS, slp.getType());
    assertEquals(ROOT + ZTABLE_LOCKS + "/1", slp.toString());

    // There is no get method

    // Parsing is not supported
    assertThrows(IllegalArgumentException.class,
        () -> ServiceLockPaths.parse(Optional.of(ZTABLE_LOCKS), ROOT + ZTABLE_LOCKS));
    assertThrows(IllegalArgumentException.class,
        () -> ServiceLockPaths.parse(Optional.of(ZTABLE_LOCKS), ROOT + ZTABLE_LOCKS + "/1"));

    EasyMock.verify(ctx);

  }

  @Test
  public void testMiniPath() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();
    EasyMock.expect(ctx.getServerPaths()).andReturn(new ServiceLockPaths(ctx)).anyTimes();
    EasyMock.replay(ctx);

    assertThrows(NullPointerException.class, () -> ctx.getServerPaths().createMiniPath(null));

    // Only mini lock creation is supported because the existing code
    // uses a ServiceLockPath with it.
    String miniUUID = UUID.randomUUID().toString();
    ServiceLockPath slp = ctx.getServerPaths().createMiniPath(miniUUID);
    assertEquals(miniUUID, slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMINI_LOCK, slp.getType());
    assertEquals(ROOT + ZMINI_LOCK + "/" + miniUUID, slp.toString());

    // There is no get method

    // Parsing is not supported
    assertThrows(IllegalArgumentException.class,
        () -> ServiceLockPaths.parse(Optional.of(ZMINI_LOCK), ROOT + ZMINI_LOCK));
    slp = ServiceLockPaths.parse(Optional.of(ZMINI_LOCK), ROOT + ZMINI_LOCK + "/" + miniUUID);
    assertEquals(miniUUID, slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMINI_LOCK, slp.getType());
    assertEquals(ROOT + ZMINI_LOCK + "/" + miniUUID, slp.toString());

    EasyMock.verify(ctx);

  }

}
