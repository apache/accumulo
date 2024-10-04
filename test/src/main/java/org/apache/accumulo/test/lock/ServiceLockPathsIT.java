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
    assertEquals(2, paths.getTabletServer(rg -> true, addr -> true, true).size());
    assertEquals(1, paths
        .getTabletServer(rg -> rg.equals(Constants.DEFAULT_RESOURCE_GROUP_NAME), addr -> true, true)
        .size());
    assertEquals(1, paths.getTabletServer(rg -> rg.equals("TTEST"), addr -> true, true).size());
    assertEquals(0, paths.getTabletServer(rg -> rg.equals("FAKE"), addr -> true, true).size());
    assertEquals(0, paths.getTabletServer(rg -> rg.equals("CTEST"), addr -> true, true).size());
    assertEquals(0, paths.getTabletServer(rg -> rg.equals("STEST"), addr -> true, true).size());

    assertEquals(4, paths.getCompactor(rg -> true, addr -> true, true).size());
    assertEquals(1, paths
        .getCompactor(rg -> rg.equals(Constants.DEFAULT_RESOURCE_GROUP_NAME), addr -> true, true)
        .size());
    assertEquals(3, paths.getCompactor(rg -> rg.equals("CTEST"), addr -> true, true).size());
    assertEquals(0, paths.getCompactor(rg -> rg.equals("FAKE"), addr -> true, true).size());
    assertEquals(0, paths.getCompactor(rg -> rg.equals("TTEST"), addr -> true, true).size());
    assertEquals(0, paths.getCompactor(rg -> rg.equals("STEST"), addr -> true, true).size());

    assertEquals(3, paths.getScanServer(rg -> true, addr -> true, true).size());
    assertEquals(1, paths
        .getScanServer(rg -> rg.equals(Constants.DEFAULT_RESOURCE_GROUP_NAME), addr -> true, true)
        .size());
    assertEquals(2, paths.getScanServer(rg -> rg.equals("STEST"), addr -> true, true).size());
    assertEquals(0, paths.getScanServer(rg -> rg.equals("FAKE"), addr -> true, true).size());
    assertEquals(0, paths.getScanServer(rg -> rg.equals("CTEST"), addr -> true, true).size());
    assertEquals(0, paths.getScanServer(rg -> rg.equals("TTEST"), addr -> true, true).size());

    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);

    Wait.waitFor(() -> paths.getCompactor(rg -> true, addr -> true, true).size() == 0);

    getCluster().getClusterControl().stopAllServers(ServerType.SCAN_SERVER);

    Wait.waitFor(() -> paths.getScanServer(rg -> true, addr -> true, true).size() == 0);

    getCluster().getClusterControl().stopAllServers(ServerType.GARBAGE_COLLECTOR);

    Wait.waitFor(() -> paths.getGarbageCollector(true) == null);

    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);

    Wait.waitFor(() -> paths.getManager(true) == null);

    getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);

    Wait.waitFor(() -> paths.getTabletServer(rg -> true, addr -> true, true).size() == 0);
    Wait.waitFor(() -> paths.getTabletServer(rg -> true, addr -> true, false).size() == 2);

  }

}
