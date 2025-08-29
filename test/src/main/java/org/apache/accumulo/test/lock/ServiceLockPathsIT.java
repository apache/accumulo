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

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
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
    cfg.getClusterServerConfiguration().addCompactorResourceGroup("CTEST", 3);
    cfg.getClusterServerConfiguration().addScanServerResourceGroup("STEST", 2);
    cfg.getClusterServerConfiguration().addTabletServerResourceGroup("TTEST", 1);
  }

  @Test
  public void testPaths() throws Exception {
    ServiceLockPaths paths = getServerContext().getServerPaths();
    assertNotNull(paths.getGarbageCollector(true));
    assertNotNull(paths.getManager(true));
    assertNull(paths.getMonitor(true)); // monitor not started
    assertEquals(2,
        paths.getTabletServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true).size());
    assertEquals(1,
        paths.getTabletServer(ResourceGroupPredicate.DEFAULT, AddressSelector.all(), true).size());
    assertEquals(1, paths.getTabletServer(ResourceGroupPredicate.exact(ResourceGroupId.of("TTEST")),
        AddressSelector.all(), true).size());
    assertEquals(0, paths.getTabletServer(ResourceGroupPredicate.exact(ResourceGroupId.of("FAKE")),
        AddressSelector.all(), true).size());
    assertEquals(0, paths.getTabletServer(ResourceGroupPredicate.exact(ResourceGroupId.of("CTEST")),
        AddressSelector.all(), true).size());
    assertEquals(0, paths.getTabletServer(ResourceGroupPredicate.exact(ResourceGroupId.of("STEST")),
        AddressSelector.all(), true).size());

    assertEquals(4,
        paths.getCompactor(ResourceGroupPredicate.ANY, AddressSelector.all(), true).size());
    assertEquals(1, paths.getCompactor(ResourceGroupPredicate.exact(ResourceGroupId.DEFAULT),
        AddressSelector.all(), true).size());
    assertEquals(3, paths.getCompactor(ResourceGroupPredicate.exact(ResourceGroupId.of("CTEST")),
        AddressSelector.all(), true).size());
    assertEquals(0, paths.getCompactor(ResourceGroupPredicate.exact(ResourceGroupId.of("FAKE")),
        AddressSelector.all(), true).size());
    assertEquals(0, paths.getCompactor(ResourceGroupPredicate.exact(ResourceGroupId.of("TTEST")),
        AddressSelector.all(), true).size());
    assertEquals(0, paths.getCompactor(ResourceGroupPredicate.exact(ResourceGroupId.of("STEST")),
        AddressSelector.all(), true).size());

    assertEquals(3,
        paths.getScanServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true).size());
    assertEquals(1,
        paths.getScanServer(ResourceGroupPredicate.DEFAULT, AddressSelector.all(), true).size());
    assertEquals(2, paths.getScanServer(ResourceGroupPredicate.exact(ResourceGroupId.of("STEST")),
        AddressSelector.all(), true).size());
    assertEquals(0, paths.getScanServer(ResourceGroupPredicate.exact(ResourceGroupId.of("FAKE")),
        AddressSelector.all(), true).size());
    assertEquals(0, paths.getScanServer(ResourceGroupPredicate.exact(ResourceGroupId.of("CTEST")),
        AddressSelector.all(), true).size());
    assertEquals(0, paths.getScanServer(ResourceGroupPredicate.exact(ResourceGroupId.of("TTEST")),
        AddressSelector.all(), true).size());

    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);

    Wait.waitFor(
        () -> paths.getCompactor(ResourceGroupPredicate.ANY, AddressSelector.all(), true).size()
            == 0);

    getCluster().getClusterControl().stopAllServers(ServerType.SCAN_SERVER);

    Wait.waitFor(
        () -> paths.getScanServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true).size()
            == 0);

    getCluster().getClusterControl().stopAllServers(ServerType.GARBAGE_COLLECTOR);

    Wait.waitFor(() -> paths.getGarbageCollector(true) == null);

    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);

    Wait.waitFor(() -> paths.getManager(true) == null);

    getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);

    Wait.waitFor(
        () -> paths.getTabletServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true).size()
            == 0);
    Wait.waitFor(
        () -> paths.getTabletServer(ResourceGroupPredicate.ANY, AddressSelector.all(), false).size()
            == 2);

  }

}
