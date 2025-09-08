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

import static org.apache.accumulo.core.Constants.ZCOMPACTORS;
import static org.apache.accumulo.core.Constants.ZDEADTSERVERS;
import static org.apache.accumulo.core.Constants.ZGC_LOCK;
import static org.apache.accumulo.core.Constants.ZMANAGER_LOCK;
import static org.apache.accumulo.core.Constants.ZMINI_LOCK;
import static org.apache.accumulo.core.Constants.ZMONITOR_LOCK;
import static org.apache.accumulo.core.Constants.ZSSERVERS;
import static org.apache.accumulo.core.Constants.ZTABLE_LOCKS;
import static org.apache.accumulo.core.Constants.ZTSERVERS;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
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

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.zookeeper.ZcStat;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ServiceLockPathsTest {

  private static final ResourceGroupId TEST_RESOURCE_GROUP = ResourceGroupId.of("TEST_RG");
  private static final String HOSTNAME = "localhost:9876";
  private static final String HOSTNAME_NO_LOCK = "localhost:9877";
  private static final HostAndPort hp = HostAndPort.fromString(HOSTNAME);

  private ZooCache zc;
  private ServiceLockPaths paths;

  @BeforeEach
  public void setupMocks() {
    zc = createMock(ZooCache.class);
    paths = new ServiceLockPaths(zc);
  }

  @AfterEach
  public void verifyMocks() {
    verify(zc);
  }

  @Test
  public void testPathGeneration() {
    replay(zc);

    // Test management process path creation
    var slp = paths.createGarbageCollectorPath();
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZGC_LOCK, slp.getType());
    assertEquals(ZGC_LOCK, slp.toString());

    slp = paths.createManagerPath();
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMANAGER_LOCK, slp.getType());
    assertEquals(ZMANAGER_LOCK, slp.toString());

    assertThrows(NullPointerException.class, () -> paths.createMiniPath(null));
    String miniUUID = UUID.randomUUID().toString();
    slp = paths.createMiniPath(miniUUID);
    assertEquals(miniUUID, slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMINI_LOCK, slp.getType());
    assertEquals(ZMINI_LOCK + "/" + miniUUID, slp.toString());

    slp = paths.createMonitorPath();
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMONITOR_LOCK, slp.getType());
    assertEquals(ZMONITOR_LOCK, slp.toString());

    // Test worker process path creation
    assertThrows(NullPointerException.class, () -> paths.createCompactorPath(null, null));
    assertThrows(NullPointerException.class,
        () -> paths.createCompactorPath(TEST_RESOURCE_GROUP, null));
    slp = paths.createCompactorPath(TEST_RESOURCE_GROUP, hp);
    assertEquals(HOSTNAME, slp.getServer());
    assertEquals(TEST_RESOURCE_GROUP, slp.getResourceGroup());
    assertEquals(ZCOMPACTORS, slp.getType());
    assertEquals(ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
        slp.toString());

    assertThrows(NullPointerException.class, () -> paths.createDeadTabletServerPath(null, null));
    assertThrows(NullPointerException.class,
        () -> paths.createDeadTabletServerPath(TEST_RESOURCE_GROUP, null));
    slp = paths.createDeadTabletServerPath(TEST_RESOURCE_GROUP, hp);
    assertEquals(HOSTNAME, slp.getServer());
    assertEquals(TEST_RESOURCE_GROUP, slp.getResourceGroup());
    assertEquals(ZDEADTSERVERS, slp.getType());
    assertEquals(ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP + "/" + HOSTNAME, slp.toString());

    assertThrows(NullPointerException.class, () -> paths.createScanServerPath(null, null));
    assertThrows(NullPointerException.class,
        () -> paths.createScanServerPath(TEST_RESOURCE_GROUP, null));
    slp = paths.createScanServerPath(TEST_RESOURCE_GROUP, hp);
    assertEquals(HOSTNAME, slp.getServer());
    assertEquals(TEST_RESOURCE_GROUP, slp.getResourceGroup());
    assertEquals(ZSSERVERS, slp.getType());
    assertEquals(ZSSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
        slp.toString());

    assertThrows(NullPointerException.class, () -> paths.createTabletServerPath(null, null));
    assertThrows(NullPointerException.class,
        () -> paths.createTabletServerPath(TEST_RESOURCE_GROUP, null));
    slp = paths.createTabletServerPath(TEST_RESOURCE_GROUP, hp);
    assertEquals(HOSTNAME, slp.getServer());
    assertEquals(TEST_RESOURCE_GROUP, slp.getResourceGroup());
    assertEquals(ZTSERVERS, slp.getType());
    assertEquals(ZTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
        slp.toString());
  }

  @Test
  public void testGetGarbageCollectorNotRunning() {
    expect(zc.getChildren(ZGC_LOCK)).andReturn(List.of()).anyTimes();
    replay(zc);

    var slp = paths.getGarbageCollector(true);
    assertNull(slp);
  }

  @Test
  public void testGetGarbageCollectorNoLock() {
    expect(zc.getChildren(ZGC_LOCK)).andReturn(List.of(HOSTNAME_NO_LOCK)).anyTimes();
    replay(zc);

    var slp = paths.getGarbageCollector(false);
    assertNotNull(slp);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZGC_LOCK, slp.getType());
    assertEquals(ZGC_LOCK, slp.toString());
  }

  @Test
  public void testGetGarbageCollector() {
    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    var sld = new ServiceLockData(uuid, ServerId.gc("localhost", 1234), ThriftService.GC);

    expect(zc.getChildren(ZGC_LOCK)).andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    expect(zc.get(EasyMock.eq(ZGC_LOCK + "/" + svcLock1), EasyMock.isA(ZcStat.class)))
        .andReturn(sld.serialize());
    replay(zc);

    var slp = paths.getGarbageCollector(true);
    assertNotNull(slp);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZGC_LOCK, slp.getType());
    assertEquals(ZGC_LOCK, slp.toString());
  }

  @Test
  public void testGetManagerNotRunning() {
    expect(zc.getChildren(ZMANAGER_LOCK)).andReturn(List.of()).anyTimes();
    replay(zc);

    var slp = paths.getManager(true);
    assertNull(slp);
  }

  @Test
  public void testGetManagerNoLock() {
    expect(zc.getChildren(ZMANAGER_LOCK)).andReturn(List.of(HOSTNAME_NO_LOCK)).anyTimes();
    replay(zc);

    var slp = paths.getManager(false);
    assertNotNull(slp);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMANAGER_LOCK, slp.getType());
    assertEquals(ZMANAGER_LOCK, slp.toString());
  }

  @Test
  public void testGetManager() {
    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    var sld = new ServiceLockData(uuid, ServerId.manager("localhost", 9995), ThriftService.MANAGER);

    expect(zc.getChildren(ZMANAGER_LOCK)).andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    expect(zc.get(EasyMock.eq(ZMANAGER_LOCK + "/" + svcLock1), EasyMock.isA(ZcStat.class)))
        .andReturn(sld.serialize());
    replay(zc);

    var slp = paths.getManager(true);
    assertNotNull(slp);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMANAGER_LOCK, slp.getType());
    assertEquals(ZMANAGER_LOCK, slp.toString());
  }

  @Test
  public void testGetMonitorNotRunning() {
    expect(zc.getChildren(ZMONITOR_LOCK)).andReturn(List.of()).anyTimes();
    replay(zc);

    var slp = paths.getMonitor(true);
    assertNull(slp);
  }

  @Test
  public void testGetMonitorNoLock() {
    expect(zc.getChildren(ZMONITOR_LOCK)).andReturn(List.of(HOSTNAME_NO_LOCK)).anyTimes();
    replay(zc);

    var slp = paths.getMonitor(false);
    assertNotNull(slp);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMONITOR_LOCK, slp.getType());
    assertEquals(ZMONITOR_LOCK, slp.toString());
  }

  @Test
  public void testGetMonitor() {
    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    var sld = new ServiceLockData(uuid, ServerId.monitor("localhost", 9996), ThriftService.NONE);

    expect(zc.getChildren(ZMONITOR_LOCK)).andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    expect(zc.get(EasyMock.eq(ZMONITOR_LOCK + "/" + svcLock1), EasyMock.isA(ZcStat.class)))
        .andReturn(sld.serialize());
    replay(zc);

    var slp = paths.getMonitor(true);
    assertNotNull(slp);
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMONITOR_LOCK, slp.getType());
    assertEquals(ZMONITOR_LOCK, slp.toString());
  }

  @Test
  public void testGetCompactorsNotRunning() {
    expect(zc.getChildren(ZCOMPACTORS)).andReturn(List.of()).anyTimes();
    replay(zc);

    assertThrows(NullPointerException.class, () -> paths.getCompactor(null, null, true));
    assertThrows(NullPointerException.class,
        () -> paths.getCompactor(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP), null, true));
    assertTrue(
        paths.getCompactor(ResourceGroupPredicate.ANY, AddressSelector.all(), true).isEmpty());
    assertTrue(paths.getCompactor(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.all(), true).isEmpty());
    assertTrue(paths.getCompactor(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.exact(hp), true).isEmpty());
  }

  @Test
  public void testGetCompactors() {
    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    var sld1 = new ServiceLockData(uuid, ServerId.compactor(TEST_RESOURCE_GROUP, "localhost", 9876),
        ThriftService.COMPACTOR);
    var sld2 =
        new ServiceLockData(uuid, ServerId.compactor("localhost", 9877), ThriftService.COMPACTOR);

    expect(zc.getChildren(ZCOMPACTORS))
        .andReturn(List.of(TEST_RESOURCE_GROUP.canonical(), ResourceGroupId.DEFAULT.canonical()))
        .anyTimes();
    expect(zc.getChildren(ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical()))
        .andReturn(List.of(HOSTNAME, HOSTNAME_NO_LOCK)).anyTimes();
    expect(zc.getChildren(ZCOMPACTORS + "/" + ResourceGroupId.DEFAULT.canonical()))
        .andReturn(List.of(HOSTNAME, HOSTNAME_NO_LOCK)).anyTimes();
    expect(zc
        .getChildren(ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME_NO_LOCK))
        .andReturn(List.of()).anyTimes();
    expect(zc.getChildren(
        ZCOMPACTORS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME_NO_LOCK))
        .andReturn(List.of()).anyTimes();
    expect(zc.getChildren(ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    expect(zc.getChildren(ZCOMPACTORS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    expect(zc.get(
        EasyMock.eq(
            ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld1.serialize()).anyTimes();
    expect(zc.get(EasyMock.eq(
        ZCOMPACTORS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld2.serialize()).anyTimes();

    expect(zc.getChildren(ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical() + "/localhost:1234"))
        .andReturn(null).anyTimes();
    expect(zc.get(ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical() + "/localhost:1234"))
        .andReturn(null).anyTimes();
    replay(zc);

    // query for all
    Set<ServiceLockPath> results =
        paths.getCompactor(ResourceGroupPredicate.ANY, AddressSelector.all(), false);
    assertEquals(4, results.size());
    for (ServiceLockPath path : results) {
      assertEquals(ZCOMPACTORS, path.getType());
      assertTrue(path.getServer().equals(HOSTNAME) || path.getServer().equals(HOSTNAME_NO_LOCK));
      assertTrue(path.getResourceGroup().equals(ResourceGroupId.DEFAULT)
          || path.getResourceGroup().equals(TEST_RESOURCE_GROUP));
      assertTrue(path.toString()
          .equals(ZCOMPACTORS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME)
          || path.toString().equals(
              ZCOMPACTORS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME_NO_LOCK)
          || path.toString()
              .equals(ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME)
          || path.toString().equals(
              ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME_NO_LOCK));
    }

    // query for all with locks
    results = paths.getCompactor(ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertEquals(2, results.size());
    Iterator<ServiceLockPath> iter = results.iterator();
    var slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZCOMPACTORS, slp1.getType());
    if (slp1.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
          slp1.toString());
    } else if (slp1.getResourceGroup().equals(ResourceGroupId.DEFAULT)) {
      assertEquals(ZCOMPACTORS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME,
          slp1.toString());
    } else {
      fail("wrong resource group");
    }
    var slp2 = iter.next();
    assertEquals(HOSTNAME, slp2.getServer());
    assertEquals(ZCOMPACTORS, slp2.getType());
    if (slp2.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
          slp2.toString());
    } else if (slp2.getResourceGroup().equals(ResourceGroupId.DEFAULT)) {
      assertEquals(ZCOMPACTORS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME,
          slp2.toString());
    } else {
      fail("wrong resource group");
    }

    // query for all in non-existent resource group
    results =
        paths.getCompactor(ResourceGroupPredicate.exact(ResourceGroupId.of("FAKE_RESOURCE_GROUP")),
            AddressSelector.all(), true);
    assertEquals(0, results.size());

    // query for all in test resource group
    results = paths.getCompactor(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.all(), true);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZCOMPACTORS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
        slp1.toString());

    // query for a specific server
    results = paths.getCompactor(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.exact(hp), true);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZCOMPACTORS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ZCOMPACTORS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
        slp1.toString());

    // query for a wrong server
    for (boolean withLock : new boolean[] {true, false}) {
      results = paths.getCompactor(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
          AddressSelector.exact(HostAndPort.fromString("localhost:1234")), withLock);
      assertEquals(0, results.size());
      results = paths.getCompactor(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
          AddressSelector.matching(hp -> hp.equals("localhost:1234")), withLock);
      assertEquals(0, results.size());
    }
  }

  @Test
  public void testGetScanServersNotRunning() {
    expect(zc.getChildren(ZSSERVERS)).andReturn(List.of()).anyTimes();
    replay(zc);

    assertThrows(NullPointerException.class, () -> paths.getScanServer(null, null, true));
    assertThrows(NullPointerException.class,
        () -> paths.getScanServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP), null, true));
    assertTrue(
        paths.getScanServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true).isEmpty());
    assertTrue(paths.getScanServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.all(), true).isEmpty());
    assertTrue(paths.getScanServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.exact(hp), true).isEmpty());
  }

  @Test
  public void testGetScanServers() {
    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    var sld1 = new ServiceLockData(uuid, ServerId.sserver(TEST_RESOURCE_GROUP, "localhost", 9876),
        ThriftService.TABLET_SCAN);
    var sld2 =
        new ServiceLockData(uuid, ServerId.sserver("localhost", 9877), ThriftService.TABLET_SCAN);

    expect(zc.getChildren(ZSSERVERS))
        .andReturn(List.of(TEST_RESOURCE_GROUP.canonical(), ResourceGroupId.DEFAULT.canonical()))
        .anyTimes();
    expect(zc.getChildren(ZSSERVERS + "/" + TEST_RESOURCE_GROUP.canonical()))
        .andReturn(List.of(HOSTNAME, HOSTNAME_NO_LOCK)).anyTimes();
    expect(zc.getChildren(ZSSERVERS + "/" + ResourceGroupId.DEFAULT.canonical()))
        .andReturn(List.of(HOSTNAME, HOSTNAME_NO_LOCK)).anyTimes();
    expect(
        zc.getChildren(ZSSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME_NO_LOCK))
        .andReturn(List.of()).anyTimes();
    expect(zc.getChildren(
        ZSSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME_NO_LOCK))
        .andReturn(List.of()).anyTimes();
    expect(zc.getChildren(ZSSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    expect(zc.getChildren(ZSSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    expect(zc.get(
        EasyMock.eq(
            ZSSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld1.serialize()).anyTimes();
    expect(zc.get(EasyMock.eq(
        ZSSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld2.serialize()).anyTimes();

    expect(zc.getChildren(ZSSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/localhost:1234"))
        .andReturn(null).anyTimes();
    replay(zc);

    // query for all
    Set<ServiceLockPath> results =
        paths.getScanServer(ResourceGroupPredicate.ANY, AddressSelector.all(), false);
    assertEquals(4, results.size());
    for (ServiceLockPath path : results) {
      assertEquals(ZSSERVERS, path.getType());
      assertTrue(path.getServer().equals(HOSTNAME) || path.getServer().equals(HOSTNAME_NO_LOCK));
      assertTrue(path.getResourceGroup().equals(ResourceGroupId.DEFAULT)
          || path.getResourceGroup().equals(TEST_RESOURCE_GROUP));
      assertTrue(path.toString()
          .equals(ZSSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME)
          || path.toString().equals(
              ZSSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME_NO_LOCK)
          || path.toString()
              .equals(ZSSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME)
          || path.toString()
              .equals(ZSSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME_NO_LOCK));
    }

    // query for all with lock
    results = paths.getScanServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertEquals(2, results.size());
    Iterator<ServiceLockPath> iter = results.iterator();
    var slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZSSERVERS, slp1.getType());
    if (slp1.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ZSSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
          slp1.toString());
    } else if (slp1.getResourceGroup().equals(ResourceGroupId.DEFAULT)) {
      assertEquals(ZSSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME,
          slp1.toString());
    } else {
      fail("wrong resource group");
    }
    var slp2 = iter.next();
    assertEquals(HOSTNAME, slp2.getServer());
    assertEquals(ZSSERVERS, slp2.getType());
    if (slp2.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ZSSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
          slp2.toString());
    } else if (slp2.getResourceGroup().equals(ResourceGroupId.DEFAULT)) {
      assertEquals(ZSSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME,
          slp2.toString());
    } else {
      fail("wrong resource group");
    }

    // query for all in non-existent resource group
    results =
        paths.getScanServer(ResourceGroupPredicate.exact(ResourceGroupId.of("FAKE_RESOURCE_GROUP")),
            AddressSelector.all(), true);
    assertEquals(0, results.size());

    // query for all in test resource group
    results = paths.getScanServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.all(), true);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZSSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ZSSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
        slp1.toString());

    // query for a specific server
    results = paths.getScanServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.exact(hp), true);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZSSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ZSSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
        slp1.toString());

    // query for a wrong server
    results = paths.getScanServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.exact(HostAndPort.fromString("localhost:1234")), true);
    assertEquals(0, results.size());
  }

  @Test
  public void testGetTabletServersNotRunning() {
    expect(zc.getChildren(ZTSERVERS)).andReturn(List.of()).anyTimes();
    replay(zc);

    assertThrows(NullPointerException.class, () -> paths.getTabletServer(null, null, true));
    assertThrows(NullPointerException.class,
        () -> paths.getTabletServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP), null, true));
    assertTrue(
        paths.getTabletServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true).isEmpty());
    assertTrue(paths.getTabletServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.all(), true).isEmpty());
    assertTrue(paths.getTabletServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.exact(hp), true).isEmpty());
  }

  @Test
  public void testGetTabletServers() {
    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    var sld1 = new ServiceLockData(uuid, ServerId.tserver(TEST_RESOURCE_GROUP, "localhost", 9876),
        ThriftService.TABLET_SCAN);
    var sld2 =
        new ServiceLockData(uuid, ServerId.tserver("localhost", 9877), ThriftService.TABLET_SCAN);

    expect(zc.getChildren(ZTSERVERS))
        .andReturn(List.of(TEST_RESOURCE_GROUP.canonical(), ResourceGroupId.DEFAULT.canonical()))
        .anyTimes();
    expect(zc.getChildren(ZTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical()))
        .andReturn(List.of(HOSTNAME, HOSTNAME_NO_LOCK)).anyTimes();
    expect(zc.getChildren(ZTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical()))
        .andReturn(List.of(HOSTNAME, HOSTNAME_NO_LOCK)).anyTimes();
    expect(
        zc.getChildren(ZTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME_NO_LOCK))
        .andReturn(List.of()).anyTimes();
    expect(zc.getChildren(
        ZTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME_NO_LOCK))
        .andReturn(List.of()).anyTimes();
    expect(zc.getChildren(ZTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    expect(zc.getChildren(ZTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    expect(zc.get(
        EasyMock.eq(
            ZTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld1.serialize()).anyTimes();
    expect(zc.get(EasyMock.eq(
        ZTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld2.serialize()).anyTimes();

    expect(zc.getChildren(ZTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/localhost:1234"))
        .andReturn(null).anyTimes();
    replay(zc);

    // query for all
    Set<ServiceLockPath> results =
        paths.getTabletServer(ResourceGroupPredicate.ANY, AddressSelector.all(), false);
    assertEquals(4, results.size());
    for (ServiceLockPath path : results) {
      assertEquals(ZTSERVERS, path.getType());
      assertTrue(path.getServer().equals(HOSTNAME) || path.getServer().equals(HOSTNAME_NO_LOCK));
      assertTrue(path.getResourceGroup().equals(ResourceGroupId.DEFAULT)
          || path.getResourceGroup().equals(TEST_RESOURCE_GROUP));
      assertTrue(path.toString()
          .equals(ZTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME)
          || path.toString().equals(
              ZTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME_NO_LOCK)
          || path.toString()
              .equals(ZTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME)
          || path.toString()
              .equals(ZTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME_NO_LOCK));
    }

    // query for all with lock
    results = paths.getTabletServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertEquals(2, results.size());
    Iterator<ServiceLockPath> iter = results.iterator();
    var slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZTSERVERS, slp1.getType());
    if (slp1.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ZTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
          slp1.toString());
    } else if (slp1.getResourceGroup().equals(ResourceGroupId.DEFAULT)) {
      assertEquals(ZTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME,
          slp1.toString());
    } else {
      fail("wrong resource group");
    }
    var slp2 = iter.next();
    assertEquals(HOSTNAME, slp2.getServer());
    assertEquals(ZTSERVERS, slp2.getType());
    if (slp2.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ZTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
          slp2.toString());
    } else if (slp2.getResourceGroup().equals(ResourceGroupId.DEFAULT)) {
      assertEquals(ZTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME,
          slp2.toString());
    } else {
      fail("wrong resource group");
    }

    // query for all in non-existent resource group
    results = paths.getTabletServer(
        ResourceGroupPredicate.exact(ResourceGroupId.of("FAKE_RESOURCE_GROUP")),
        AddressSelector.all(), true);
    assertEquals(0, results.size());

    // query for all in test resource group
    results = paths.getTabletServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.all(), true);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZTSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ZTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
        slp1.toString());

    // query for a specific server
    results = paths.getTabletServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.exact(hp), true);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZTSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ZTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
        slp1.toString());

    // query for a wrong server
    results = paths.getTabletServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.exact(HostAndPort.fromString("localhost:1234")), true);
    assertEquals(0, results.size());
  }

  @Test
  public void testGetDeadTabletServersNone() {
    expect(zc.getChildren(ZDEADTSERVERS)).andReturn(List.of()).anyTimes();
    replay(zc);

    assertThrows(NullPointerException.class, () -> paths.getDeadTabletServer(null, null, false));
    assertThrows(NullPointerException.class, () -> paths
        .getDeadTabletServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP), null, false));
    assertTrue(paths.getDeadTabletServer(ResourceGroupPredicate.ANY, AddressSelector.all(), false)
        .isEmpty());
    assertTrue(paths.getDeadTabletServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.all(), false).isEmpty());
    assertTrue(paths.getDeadTabletServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.exact(hp), false).isEmpty());
  }

  @Test
  public void testGetDeadTabletServers() {
    UUID uuid = UUID.randomUUID();
    String svcLock1 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000001";
    String svcLock2 = ServiceLock.ZLOCK_PREFIX + uuid.toString() + "#0000000002";
    var sld1 = new ServiceLockData(uuid, ServerId.tserver(TEST_RESOURCE_GROUP, "localhost", 9876),
        ThriftService.TABLET_SCAN);
    var sld2 =
        new ServiceLockData(uuid, ServerId.tserver("localhost", 9877), ThriftService.TABLET_SCAN);

    expect(zc.getChildren(ZDEADTSERVERS))
        .andReturn(List.of(TEST_RESOURCE_GROUP.canonical(), ResourceGroupId.DEFAULT.canonical()))
        .anyTimes();
    expect(zc.getChildren(ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical()))
        .andReturn(List.of(HOSTNAME)).anyTimes();
    expect(zc.getChildren(ZDEADTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical()))
        .andReturn(List.of(HOSTNAME)).anyTimes();
    expect(zc.getChildren(ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    expect(zc.get(ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME))
        .andReturn(new byte[0]).anyTimes();
    expect(
        zc.getChildren(ZDEADTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME))
        .andReturn(List.of(svcLock1, svcLock2)).anyTimes();
    expect(zc.get(EasyMock.eq(
        ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME + "/" + svcLock1),
        EasyMock.isA(ZcStat.class))).andReturn(sld1.serialize()).anyTimes();
    expect(zc.get(EasyMock.eq(ZDEADTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/"
        + HOSTNAME + "/" + svcLock1), EasyMock.isA(ZcStat.class))).andReturn(sld2.serialize())
        .anyTimes();

    expect(zc.get(ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/localhost:1234"))
        .andReturn(null).anyTimes();
    replay(zc);

    // query for all
    Set<ServiceLockPath> results =
        paths.getDeadTabletServer(ResourceGroupPredicate.ANY, AddressSelector.all(), false);
    assertEquals(2, results.size());
    Iterator<ServiceLockPath> iter = results.iterator();
    var slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZDEADTSERVERS, slp1.getType());
    if (slp1.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
          slp1.toString());
    } else if (slp1.getResourceGroup().equals(ResourceGroupId.DEFAULT)) {
      assertEquals(ZDEADTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME,
          slp1.toString());
    } else {
      fail("wrong resource group");
    }
    var slp2 = iter.next();
    assertEquals(HOSTNAME, slp2.getServer());
    assertEquals(ZDEADTSERVERS, slp2.getType());
    if (slp2.getResourceGroup().equals(TEST_RESOURCE_GROUP)) {
      assertEquals(ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
          slp2.toString());
    } else if (slp2.getResourceGroup().equals(ResourceGroupId.DEFAULT)) {
      assertEquals(ZDEADTSERVERS + "/" + ResourceGroupId.DEFAULT.canonical() + "/" + HOSTNAME,
          slp2.toString());
    } else {
      fail("wrong resource group");
    }

    // query for all in non-existent resource group
    results = paths.getDeadTabletServer(
        ResourceGroupPredicate.exact(ResourceGroupId.of("FAKE_RESOURCE_GROUP")),
        AddressSelector.all(), false);
    assertEquals(0, results.size());

    // query for all in test resource group
    results = paths.getDeadTabletServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.all(), false);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZDEADTSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
        slp1.toString());

    // query for a specific server
    results = paths.getDeadTabletServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.exact(hp), false);
    assertEquals(1, results.size());
    iter = results.iterator();
    slp1 = iter.next();
    assertEquals(HOSTNAME, slp1.getServer());
    assertEquals(ZDEADTSERVERS, slp1.getType());
    assertEquals(TEST_RESOURCE_GROUP, slp1.getResourceGroup());
    assertEquals(ZDEADTSERVERS + "/" + TEST_RESOURCE_GROUP.canonical() + "/" + HOSTNAME,
        slp1.toString());

    // query for a wrong server
    results = paths.getDeadTabletServer(ResourceGroupPredicate.exact(TEST_RESOURCE_GROUP),
        AddressSelector.exact(HostAndPort.fromString("localhost:1234")), false);
    assertEquals(0, results.size());
  }

  @Test
  public void testTableLocksPath() {
    replay(zc);

    // Only table lock creation is supported because the existing code
    // uses a ServiceLockPath with it.
    var slp = paths.createTableLocksPath();
    assertNull(slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZTABLE_LOCKS, slp.getType());
    assertEquals(ZTABLE_LOCKS, slp.toString());

    slp = paths.createTableLocksPath(TableId.of("1"));
    assertEquals("1", slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZTABLE_LOCKS, slp.getType());
    assertEquals(ZTABLE_LOCKS + "/1", slp.toString());

    // There is no get method

    // Parsing is not supported
    assertThrows(IllegalArgumentException.class,
        () -> ServiceLockPaths.parse(Optional.of(ZTABLE_LOCKS), ZTABLE_LOCKS));
    assertThrows(IllegalArgumentException.class,
        () -> ServiceLockPaths.parse(Optional.of(ZTABLE_LOCKS), ZTABLE_LOCKS + "/1"));
  }

  @Test
  public void testMiniPath() {
    replay(zc);

    assertThrows(NullPointerException.class, () -> paths.createMiniPath(null));

    // Only mini lock creation is supported because the existing code
    // uses a ServiceLockPath with it.
    String miniUUID = UUID.randomUUID().toString();
    var slp = paths.createMiniPath(miniUUID);
    assertEquals(miniUUID, slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMINI_LOCK, slp.getType());
    assertEquals(ZMINI_LOCK + "/" + miniUUID, slp.toString());

    // There is no get method

    // Parsing is not supported
    assertThrows(IllegalArgumentException.class,
        () -> ServiceLockPaths.parse(Optional.of(ZMINI_LOCK), ZMINI_LOCK));
    slp = ServiceLockPaths.parse(Optional.of(ZMINI_LOCK), ZMINI_LOCK + "/" + miniUUID);
    assertEquals(miniUUID, slp.getServer());
    assertNull(slp.getResourceGroup());
    assertEquals(ZMINI_LOCK, slp.getType());
    assertEquals(ZMINI_LOCK + "/" + miniUUID, slp.toString());
  }
}
