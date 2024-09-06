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
package org.apache.accumulo.test.lock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Optional;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class ServiceLockPathsIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "10s");
    cfg.getClusterServerConfiguration().setNumDefaultCompactors(1);
    cfg.getClusterServerConfiguration().setNumDefaultScanServers(1);
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
    cfg.getClusterServerConfiguration().addCompactorResourceGroup("TEST", 1);
    cfg.getClusterServerConfiguration().addScanServerResourceGroup("TEST", 1);
    cfg.getClusterServerConfiguration().addTabletServerResourceGroup("TEST", 1);
  }

  @Test
  public void testPaths() throws Exception {
    assertNotNull(ServiceLockPaths.getGarbageCollector(getServerContext()));
    assertNotNull(ServiceLockPaths.getManager(getServerContext()));
    assertNull(ServiceLockPaths.getMonitor(getServerContext())); // monitor not started
    assertEquals(2, ServiceLockPaths
        .getTabletServer(getServerContext(), Optional.empty(), Optional.empty()).size());
    assertEquals(1, ServiceLockPaths.getTabletServer(getServerContext(),
        Optional.of(Constants.DEFAULT_RESOURCE_GROUP_NAME), Optional.empty()).size());
    assertEquals(1, ServiceLockPaths
        .getTabletServer(getServerContext(), Optional.of("TEST"), Optional.empty()).size());
    assertEquals(0, ServiceLockPaths
        .getTabletServer(getServerContext(), Optional.of("FAKE"), Optional.empty()).size());
    assertEquals(2, ServiceLockPaths
        .getCompactor(getServerContext(), Optional.empty(), Optional.empty()).size());
    assertEquals(1, ServiceLockPaths.getCompactor(getServerContext(),
        Optional.of(Constants.DEFAULT_RESOURCE_GROUP_NAME), Optional.empty()).size());
    assertEquals(1, ServiceLockPaths
        .getCompactor(getServerContext(), Optional.of("TEST"), Optional.empty()).size());
    assertEquals(0, ServiceLockPaths
        .getCompactor(getServerContext(), Optional.of("FAKE"), Optional.empty()).size());
    assertEquals(2, ServiceLockPaths
        .getScanServer(getServerContext(), Optional.empty(), Optional.empty()).size());
    assertEquals(1, ServiceLockPaths.getScanServer(getServerContext(),
        Optional.of(Constants.DEFAULT_RESOURCE_GROUP_NAME), Optional.empty()).size());
    assertEquals(1, ServiceLockPaths
        .getScanServer(getServerContext(), Optional.of("TEST"), Optional.empty()).size());
    assertEquals(0, ServiceLockPaths
        .getScanServer(getServerContext(), Optional.of("FAKE"), Optional.empty()).size());

    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);

    Wait.waitFor(() -> ServiceLockPaths
        .getCompactor(getServerContext(), Optional.empty(), Optional.empty()).size() == 0);

    getCluster().getClusterControl().stopAllServers(ServerType.SCAN_SERVER);

    Wait.waitFor(() -> ServiceLockPaths
        .getScanServer(getServerContext(), Optional.empty(), Optional.empty()).size() == 0);

    getCluster().getClusterControl().stopAllServers(ServerType.GARBAGE_COLLECTOR);

    Wait.waitFor(() -> ServiceLockPaths.getGarbageCollector(getServerContext()) == null);

    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);

    Wait.waitFor(() -> ServiceLockPaths.getManager(getServerContext()) == null);

    getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);

    Wait.waitFor(() -> ServiceLockPaths
        .getTabletServer(getServerContext(), Optional.empty(), Optional.empty()).size() == 0);

  }

}
