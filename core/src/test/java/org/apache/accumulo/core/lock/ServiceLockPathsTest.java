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
  private static final HostAndPort hp = HostAndPort.fromString(HOSTNAME);

  @Test
  public void testPathGeneration() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();

    EasyMock.replay(ctx);

    // Test management process path creation
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createGarbageCollectorPath(null));
    ServiceLockPath slp = ServiceLockPaths.createGarbageCollectorPath(ctx);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZGC_LOCK, slp.getType());
    assertEquals(ROOT + ZGC_LOCK, slp.toString());

    assertThrows(NullPointerException.class, () -> ServiceLockPaths.createManagerPath(null));
    slp = ServiceLockPaths.createManagerPath(ctx);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMANAGER_LOCK, slp.getType());
    assertEquals(ROOT + ZMANAGER_LOCK, slp.toString());

    assertThrows(NullPointerException.class, () -> ServiceLockPaths.createMiniPath(null, null));
    String miniUUID = UUID.randomUUID().toString();
    slp = ServiceLockPaths.createMiniPath(ctx, miniUUID);
    assertEquals(miniUUID, slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMINI_LOCK, slp.getType());
    assertEquals(ROOT + ZMINI_LOCK + "/" + miniUUID, slp.toString());

    assertThrows(NullPointerException.class, () -> ServiceLockPaths.createMonitorPath(null));
    slp = ServiceLockPaths.createMonitorPath(ctx);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMONITOR_LOCK, slp.getType());
    assertEquals(ROOT + ZMONITOR_LOCK, slp.toString());

    // Test worker process path creation
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createCompactorPath(null, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createCompactorPath(ctx, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createCompactorPath(ctx, TEST_RESOURCE_GROUP, null));
    slp = ServiceLockPaths.createCompactorPath(ctx, TEST_RESOURCE_GROUP, hp);
    assertEquals(HOSTNAME, slp.getServer());
    assertEquals(TEST_RESOURCE_GROUP, slp.getResourceGroup());
    assertEquals(ZCOMPACTORS, slp.getType());
    assertEquals(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp.toString());

    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createDeadTabletServerPath(null, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createDeadTabletServerPath(ctx, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createDeadTabletServerPath(ctx, TEST_RESOURCE_GROUP, null));
    slp = ServiceLockPaths.createDeadTabletServerPath(ctx, TEST_RESOURCE_GROUP, hp);
    assertEquals(HOSTNAME, slp.getServer());
    assertEquals(TEST_RESOURCE_GROUP, slp.getResourceGroup());
    assertEquals(ZDEADTSERVERS, slp.getType());
    assertEquals(ROOT + ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp.toString());

    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createScanServerPath(null, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createScanServerPath(ctx, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createScanServerPath(ctx, TEST_RESOURCE_GROUP, null));
    slp = ServiceLockPaths.createScanServerPath(ctx, TEST_RESOURCE_GROUP, hp);
    assertEquals(HOSTNAME, slp.getServer());
    assertEquals(TEST_RESOURCE_GROUP, slp.getResourceGroup());
    assertEquals(ZSSERVERS, slp.getType());
    assertEquals(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp.toString());

    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createTabletServerPath(null, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createTabletServerPath(ctx, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.createTabletServerPath(ctx, TEST_RESOURCE_GROUP, null));
    slp = ServiceLockPaths.createTabletServerPath(ctx, TEST_RESOURCE_GROUP, hp);
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
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class, () -> ServiceLockPaths.getGarbageCollector(null));
    ServiceLockPath slp = ServiceLockPaths.getGarbageCollector(ctx);
    assertNull(slp);

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
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class, () -> ServiceLockPaths.getGarbageCollector(null));
    ServiceLockPath slp = ServiceLockPaths.getGarbageCollector(ctx);
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
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class, () -> ServiceLockPaths.getManager(null));
    ServiceLockPath slp = ServiceLockPaths.getManager(ctx);
    assertNull(slp);

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
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class, () -> ServiceLockPaths.getManager(null));
    ServiceLockPath slp = ServiceLockPaths.getManager(ctx);
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
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class, () -> ServiceLockPaths.getMonitor(null));
    ServiceLockPath slp = ServiceLockPaths.getMonitor(ctx);
    assertNull(slp);

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
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class, () -> ServiceLockPaths.getMonitor(null));
    ServiceLockPath slp = ServiceLockPaths.getMonitor(ctx);
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
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class, () -> ServiceLockPaths.getCompactor(null, null, null));
    assertThrows(NullPointerException.class, () -> ServiceLockPaths.getCompactor(ctx, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.getCompactor(ctx, Optional.of(TEST_RESOURCE_GROUP), null));
    assertTrue(ServiceLockPaths.getCompactor(ctx, Optional.empty(), Optional.empty()).isEmpty());
    assertTrue(ServiceLockPaths
        .getCompactor(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.empty()).isEmpty());
    assertTrue(ServiceLockPaths.getCompactor(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp))
        .isEmpty());

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
        .andReturn(List.of(HOSTNAME)).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZCOMPACTORS + "/" + DEFAULT_RESOURCE_GROUP_NAME))
        .andReturn(List.of(HOSTNAME)).anyTimes();
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

    EasyMock.replay(ctx, zc);

    // query for all
    Set<ServiceLockPath> results =
        ServiceLockPaths.getCompactor(ctx, Optional.empty(), Optional.empty());
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
    results =
        ServiceLockPaths.getCompactor(ctx, Optional.of("FAKE_RESOURCE_GROUP"), Optional.empty());
    assertEquals(0, results.size());

    // query for all in test resource group
    results =
        ServiceLockPaths.getCompactor(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.empty());
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZCOMPACTORS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());

    // query for a specific server
    results = ServiceLockPaths.getCompactor(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp));
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZCOMPACTORS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());

    // query for a wrong server
    results = ServiceLockPaths.getCompactor(ctx, Optional.of(TEST_RESOURCE_GROUP),
        Optional.of(HostAndPort.fromString("localhost:1234")));
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
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.getScanServer(null, null, null));
    assertThrows(NullPointerException.class, () -> ServiceLockPaths.getScanServer(ctx, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.getScanServer(ctx, Optional.of(TEST_RESOURCE_GROUP), null));
    assertTrue(ServiceLockPaths.getScanServer(ctx, Optional.empty(), Optional.empty()).isEmpty());
    assertTrue(ServiceLockPaths
        .getScanServer(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.empty()).isEmpty());
    assertTrue(ServiceLockPaths
        .getScanServer(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp)).isEmpty());

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
        .andReturn(List.of(HOSTNAME)).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZSSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME))
        .andReturn(List.of(HOSTNAME)).anyTimes();
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

    EasyMock.replay(ctx, zc);

    // query for all
    Set<ServiceLockPath> results =
        ServiceLockPaths.getScanServer(ctx, Optional.empty(), Optional.empty());
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
    results =
        ServiceLockPaths.getScanServer(ctx, Optional.of("FAKE_RESOURCE_GROUP"), Optional.empty());
    assertEquals(0, results.size());

    // query for all in test resource group
    results =
        ServiceLockPaths.getScanServer(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.empty());
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZSSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());

    // query for a specific server
    results =
        ServiceLockPaths.getScanServer(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp));
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZSSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZSSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());

    // query for a wrong server
    results = ServiceLockPaths.getScanServer(ctx, Optional.of(TEST_RESOURCE_GROUP),
        Optional.of(HostAndPort.fromString("localhost:1234")));
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
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.getTabletServer(null, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.getTabletServer(ctx, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.getTabletServer(ctx, Optional.of(TEST_RESOURCE_GROUP), null));
    assertTrue(ServiceLockPaths.getTabletServer(ctx, Optional.empty(), Optional.empty()).isEmpty());
    assertTrue(ServiceLockPaths
        .getTabletServer(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.empty()).isEmpty());
    assertTrue(ServiceLockPaths
        .getTabletServer(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp)).isEmpty());

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
        .andReturn(List.of(HOSTNAME)).anyTimes();
    EasyMock.expect(zc.getChildren(ROOT + ZTSERVERS + "/" + DEFAULT_RESOURCE_GROUP_NAME))
        .andReturn(List.of(HOSTNAME)).anyTimes();
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

    EasyMock.replay(ctx, zc);

    // query for all
    Set<ServiceLockPath> results =
        ServiceLockPaths.getTabletServer(ctx, Optional.empty(), Optional.empty());
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
    results =
        ServiceLockPaths.getTabletServer(ctx, Optional.of("FAKE_RESOURCE_GROUP"), Optional.empty());
    assertEquals(0, results.size());

    // query for all in test resource group
    results =
        ServiceLockPaths.getTabletServer(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.empty());
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZTSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());

    // query for a specific server
    results =
        ServiceLockPaths.getTabletServer(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp));
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZTSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp1.toString());

    // query for a wrong server
    results = ServiceLockPaths.getTabletServer(ctx, Optional.of(TEST_RESOURCE_GROUP),
        Optional.of(HostAndPort.fromString("localhost:1234")));
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
    EasyMock.replay(ctx, zc);

    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.getDeadTabletServer(null, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.getDeadTabletServer(ctx, null, null));
    assertThrows(NullPointerException.class,
        () -> ServiceLockPaths.getDeadTabletServer(ctx, Optional.of(TEST_RESOURCE_GROUP), null));
    assertTrue(
        ServiceLockPaths.getDeadTabletServer(ctx, Optional.empty(), Optional.empty()).isEmpty());
    assertTrue(ServiceLockPaths
        .getDeadTabletServer(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.empty()).isEmpty());
    assertTrue(ServiceLockPaths
        .getDeadTabletServer(ctx, Optional.of(TEST_RESOURCE_GROUP), Optional.of(hp)).isEmpty());

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

    EasyMock.replay(ctx, zc);

    // query for all
    Set<ServiceLockPath> results =
        ServiceLockPaths.getDeadTabletServer(ctx, Optional.empty(), Optional.empty());
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
    results = ServiceLockPaths.getDeadTabletServer(ctx, Optional.of("FAKE_RESOURCE_GROUP"),
        Optional.empty());
    assertEquals(0, results.size());

    // query for all in test resource group
    results = ServiceLockPaths.getDeadTabletServer(ctx, Optional.of(TEST_RESOURCE_GROUP),
        Optional.empty());
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZDEADTSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME,
        slp1.toString());

    // query for a specific server
    results = ServiceLockPaths.getDeadTabletServer(ctx, Optional.of(TEST_RESOURCE_GROUP),
        Optional.of(hp));
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZDEADTSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ROOT + ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME,
        slp1.toString());

    // query for a wrong server
    results = ServiceLockPaths.getDeadTabletServer(ctx, Optional.of(TEST_RESOURCE_GROUP),
        Optional.of(HostAndPort.fromString("localhost:1234")));
    assertEquals(0, results.size());

    EasyMock.verify(ctx, zc);

  }

  @Test
  public void testTableLocksPath() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();

    EasyMock.replay(ctx);

    assertThrows(NullPointerException.class, () -> ServiceLockPaths.createTableLocksPath(null));

    // Only table lock creation is supported because the existing code
    // uses a ServiceLockPath with it.
    ServiceLockPath slp = ServiceLockPaths.createTableLocksPath(ctx);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZTABLE_LOCKS, slp.getType());
    assertEquals(ROOT + ZTABLE_LOCKS, slp.toString());

    slp = ServiceLockPaths.createTableLocksPath(ctx, "1");
    assertEquals("1", slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZTABLE_LOCKS, slp.getType());
    assertEquals(ROOT + ZTABLE_LOCKS + "/1", slp.toString());

    // There is no get method

    // Parsing is not supported
    assertThrows(IllegalArgumentException.class, () -> ServiceLockPaths.parse(ROOT + ZTABLE_LOCKS));
    assertThrows(IllegalArgumentException.class,
        () -> ServiceLockPaths.parse(ROOT + ZTABLE_LOCKS + "/1"));

    EasyMock.verify(ctx);

  }

  @Test
  public void testMiniPath() {

    ClientContext ctx = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(ctx.getZooKeeperRoot()).andReturn(ROOT).anyTimes();

    EasyMock.replay(ctx);

    assertThrows(NullPointerException.class, () -> ServiceLockPaths.createMiniPath(null, null));

    // Only mini lock creation is supported because the existing code
    // uses a ServiceLockPath with it.
    String miniUUID = UUID.randomUUID().toString();
    ServiceLockPath slp = ServiceLockPaths.createMiniPath(ctx, miniUUID);
    assertEquals(miniUUID, slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMINI_LOCK, slp.getType());
    assertEquals(ROOT + ZMINI_LOCK + "/" + miniUUID, slp.toString());

    // There is no get method

    // Parsing is not supported
    assertThrows(IllegalArgumentException.class, () -> ServiceLockPaths.parse(ROOT + ZMINI_LOCK));
    slp = ServiceLockPaths.parse(ROOT + ZMINI_LOCK + "/" + miniUUID);
    assertEquals(miniUUID, slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMINI_LOCK, slp.getType());
    assertEquals(ROOT + ZMINI_LOCK + "/" + miniUUID, slp.toString());

    EasyMock.verify(ctx);

  }

}
