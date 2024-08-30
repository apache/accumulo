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
package org.apache.accumulo.core.clientImpl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.servers.CompactorServerId;
import org.apache.accumulo.core.client.admin.servers.ManagerServerId;
import org.apache.accumulo.core.client.admin.servers.ScanServerId;
import org.apache.accumulo.core.client.admin.servers.TabletServerId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLock.ServiceLockPath;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class ServerIds {

  private static final Logger LOG = LoggerFactory.getLogger(ServerIds.class);

  private final ClientContext ctx;

  public ServerIds(ClientContext ctx) {
    this.ctx = ctx;
  }

  public CompactorServerId resolveCompactor(String address) {
    final String compactorGroupsPath = ctx.getZooKeeperRoot() + Constants.ZCOMPACTORS;
    final ZooCache cache = ctx.getZooCache();
    final List<String> groups = cache.getChildren(compactorGroupsPath);
    for (String group : groups) {
      final ZcStat stat = new ZcStat();
      final ServiceLockPath lockPath =
          ServiceLock.path(compactorGroupsPath + "/" + group + "/" + address);
      Optional<ServiceLockData> sld = ServiceLock.getLockData(ctx.getZooCache(), lockPath, stat);
      if (sld.isPresent()) {
        LOG.trace("Found live compactor {} ", address);
        final HostAndPort hp = HostAndPort.fromString(address);
        return new CompactorServerId(group, hp.getHost(), hp.getPort());
      }
    }
    return null;
  }

  public Set<CompactorServerId> getCompactors() {
    final Set<CompactorServerId> results = new HashSet<>();
    final ZooCache cache = ctx.getZooCache();
    final String root = ctx.getZooKeeperRoot() + Constants.ZCOMPACTORS;
    final List<String> groups = cache.getChildren(root);
    for (String group : groups) {
      final List<String> compactors = cache.getChildren(root + "/" + group);
      for (String addr : compactors) {
        try {
          final var zLockPath = ServiceLock.path(root + "/" + group + "/" + addr);
          final ZcStat stat = new ZcStat();
          final Optional<ServiceLockData> sld = ServiceLock.getLockData(cache, zLockPath, stat);
          if (sld.isPresent()) {
            LOG.trace("Found live compactor {} ", addr);
            final String resourceGroup = sld.orElseThrow().getGroup(ThriftService.COMPACTOR);
            final HostAndPort hp = HostAndPort.fromString(addr);
            results.add(new CompactorServerId(resourceGroup, hp.getHost(), hp.getPort()));
          }
        } catch (IllegalArgumentException e) {
          LOG.error("Error validating zookeeper compactor server node: " + addr, e);
        }
      }
    }
    return results;
  }

  public ManagerServerId getManager() {

    final ServiceLockPath zLockManagerPath =
        ServiceLock.path(Constants.ZROOT + "/" + ctx.getInstanceID() + Constants.ZMANAGER_LOCK);

    Timer timer = null;

    if (LOG.isTraceEnabled()) {
      LOG.trace("tid={} Looking up manager location in zookeeper at {}.",
          Thread.currentThread().getId(), zLockManagerPath);
      timer = Timer.startNew();
    }

    final Optional<ServiceLockData> sld = ctx.getZooCache().getLockData(zLockManagerPath);
    if (sld.isPresent()) {
      String location = sld.orElseThrow().getAddressString(ThriftService.MANAGER);
      final String group = sld.orElseThrow().getGroup(ThriftService.MANAGER);
      final HostAndPort hp = HostAndPort.fromString(location);
      if (timer != null) {
        LOG.trace("tid={} Found manager at {} in {}", Thread.currentThread().getId(), hp,
            String.format("%.3f secs", timer.elapsed(MILLISECONDS) / 1000.0));
      }
      return new ManagerServerId(group, hp.getHost(), hp.getPort());
    }
    return null;
  }

  public ScanServerId resolveScanServer(String address) {
    final String root = ctx.getZooKeeperRoot() + Constants.ZSSERVERS;
    final var zLockPath = ServiceLock.path(root + "/" + address);
    final ZcStat stat = new ZcStat();
    final Optional<ServiceLockData> sld =
        ServiceLock.getLockData(ctx.getZooCache(), zLockPath, stat);
    if (sld.isPresent()) {
      final String group = sld.orElseThrow().getGroup(ThriftService.TABLET_SCAN);
      final HostAndPort hp = HostAndPort.fromString(address);
      return new ScanServerId(group, hp.getHost(), hp.getPort());
    }
    return null;
  }

  /**
   * @return map of live scan server addresses to lock uuids.
   */
  public Map<ScanServerId,UUID> getScanServers() {
    final Map<ScanServerId,UUID> scanServers = new HashMap<>();
    final String root = ctx.getZooKeeperRoot() + Constants.ZSSERVERS;
    for (String addr : ctx.getZooCache().getChildren(root)) {
      try {
        final var zLockPath = ServiceLock.path(root + "/" + addr);
        final ZcStat stat = new ZcStat();
        final Optional<ServiceLockData> sld =
            ServiceLock.getLockData(ctx.getZooCache(), zLockPath, stat);
        if (sld.isPresent()) {
          final UUID uuid = sld.orElseThrow().getServerUUID(ThriftService.TABLET_SCAN);
          final String group = sld.orElseThrow().getGroup(ThriftService.TABLET_SCAN);
          final HostAndPort hp = HostAndPort.fromString(addr);
          scanServers.put(new ScanServerId(group, hp.getHost(), hp.getPort()), uuid);
        }
      } catch (IllegalArgumentException e) {
        LOG.error("Error validating zookeeper scan server node: " + addr, e);
      }
    }
    return scanServers;
  }

  public TabletServerId resolveTabletServer(String address) {
    final String root = ctx.getZooKeeperRoot() + Constants.ZTSERVERS;
    final var zLockPath = ServiceLock.path(root + "/" + address);
    final ZcStat stat = new ZcStat();
    final Optional<ServiceLockData> sld =
        ServiceLock.getLockData(ctx.getZooCache(), zLockPath, stat);
    if (sld.isPresent()) {
      final String group = sld.orElseThrow().getGroup(ThriftService.TABLET_SCAN);
      final HostAndPort hp = HostAndPort.fromString(address);
      return new TabletServerId(group, hp.getHost(), hp.getPort());
    }
    return null;

  }

  public Set<TabletServerId> getTabletServers() {
    final Set<TabletServerId> tservers = new HashSet<>();
    final ZooCache cache = ctx.getZooCache();
    final String root = ctx.getZooKeeperRoot() + Constants.ZTSERVERS;
    for (String addr : cache.getChildren(root)) {
      try {
        final var zLockPath = ServiceLock.path(root + "/" + addr);
        final ZcStat stat = new ZcStat();
        final Optional<ServiceLockData> sld =
            ServiceLock.getLockData(ctx.getZooCache(), zLockPath, stat);
        if (sld.isPresent()) {
          final String group = sld.orElseThrow().getGroup(ThriftService.TABLET_SCAN);
          final HostAndPort hp = HostAndPort.fromString(addr);
          tservers.add(new TabletServerId(group, hp.getHost(), hp.getPort()));
        }
      } catch (IllegalArgumentException e) {
        LOG.error("Error validating zookeeper tablet server node: " + addr, e);
      }
    }
    return tservers;
  }
}
