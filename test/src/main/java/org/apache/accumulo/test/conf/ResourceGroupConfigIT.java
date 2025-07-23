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
package org.apache.accumulo.test.conf;

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.ResourceGroupOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.rpc.clients.TServerClient;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.conf.store.ResourceGroupPropKey;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MINI_CLUSTER_ONLY)
@Tag(SUNNY_DAY)
public class ResourceGroupConfigIT extends SharedMiniClusterBase {

  private static final String RG = "ResourceGroupTest";

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new MiniClusterConfigurationCallback() {
      @Override
      public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
        cfg.getClusterServerConfiguration().setNumDefaultCompactors(1);
        cfg.getClusterServerConfiguration().setNumDefaultScanServers(1);
        cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
      }
    });
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testConfiguration() throws Exception {

    final ResourceGroupId rgid = ResourceGroupId.of(RG);
    final ResourceGroupPropKey rgpk = ResourceGroupPropKey.of(rgid);

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      @SuppressWarnings("resource")
      final ClientContext cc = (ClientContext) client;
      final InstanceOperations iops = client.instanceOperations();
      final ResourceGroupOperations rgOps = client.resourceGroupOperations();
      final ZooReaderWriter zrw = getCluster().getServerContext().getZooSession().asReaderWriter();

      // Default node created in instance init
      assertTrue(zrw.exists(ResourceGroupPropKey.DEFAULT.getPath()));

      assertFalse(zrw.exists(Constants.ZRESOURCEGROUPS + "/" + rgid.canonical()));
      // This will get created by mini, but doing it here manually for testing.
      rgOps.create(rgid);
      assertTrue(zrw.exists(rgpk.getPath()));
      assertTrue(rgOps.getProperties(rgid).isEmpty());

      rgOps.setProperty(rgid, Property.COMPACTION_WARN_TIME.getKey(), "1m");

      // Start the processes in the resource group
      getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(RG, 1);
      getCluster().getConfig().getClusterServerConfiguration().addScanServerResourceGroup(RG, 1);
      getCluster().getConfig().getClusterServerConfiguration().addTabletServerResourceGroup(RG, 1);
      getCluster().start();

      Wait.waitFor(() -> cc.getServerPaths()
          .getCompactor(rg -> rg.equals(rgid), AddressSelector.all(), true).size() == 1);
      Wait.waitFor(() -> cc.getServerPaths()
          .getScanServer(rg -> rg.equals(rgid), AddressSelector.all(), true).size() == 1);
      Wait.waitFor(() -> cc.getServerPaths()
          .getTabletServer(rg -> rg.equals(rgid), AddressSelector.all(), true).size() == 1);

      checkProperty(iops, rgOps,
          cc.getServerPaths().getCompactor(rg -> rg.equals(ResourceGroupId.DEFAULT),
              AddressSelector.all(), true),
          ResourceGroupId.DEFAULT, Property.COMPACTION_WARN_TIME,
          Property.COMPACTION_WARN_TIME.getDefaultValue());

      checkProperty(iops, rgOps,
          cc.getServerPaths().getScanServer(rg -> rg.equals(ResourceGroupId.DEFAULT),
              AddressSelector.all(), true),
          ResourceGroupId.DEFAULT, Property.COMPACTION_WARN_TIME,
          Property.COMPACTION_WARN_TIME.getDefaultValue());

      checkProperty(iops, rgOps,
          cc.getServerPaths().getTabletServer(rg -> rg.equals(ResourceGroupId.DEFAULT),
              AddressSelector.all(), true),
          ResourceGroupId.DEFAULT, Property.COMPACTION_WARN_TIME,
          Property.COMPACTION_WARN_TIME.getDefaultValue());

      checkProperty(iops, rgOps,
          cc.getServerPaths().getCompactor(rg -> rg.equals(rgid), AddressSelector.all(), true),
          rgid, Property.COMPACTION_WARN_TIME, "1m");

      checkProperty(iops, rgOps,
          cc.getServerPaths().getScanServer(rg -> rg.equals(rgid), AddressSelector.all(), true),
          rgid, Property.COMPACTION_WARN_TIME, "1m");

      checkProperty(iops, rgOps,
          cc.getServerPaths().getTabletServer(rg -> rg.equals(rgid), AddressSelector.all(), true),
          rgid, Property.COMPACTION_WARN_TIME, "1m");

      rgOps.remove(rgid);
      assertFalse(zrw.exists(rgpk.getPath()));
      assertFalse(zrw.exists(rgpk.getPath()));
      assertFalse(zrw.exists(Constants.ZRESOURCEGROUPS + "/" + rgid.canonical()));
    }

  }

  private void checkProperty(InstanceOperations iops, ResourceGroupOperations ops,
      Set<ServiceLockPath> locks, ResourceGroupId group, Property property, String value)
      throws AccumuloException, AccumuloSecurityException {
    assertEquals(1, locks.size());
    ServiceLockPath slp = locks.iterator().next();
    assertEquals(group, slp.getResourceGroup());
    String serverAddr = slp.getServer();
    System.setProperty(TServerClient.DEBUG_HOST, serverAddr);
    if (!group.equals(ResourceGroupId.DEFAULT)) {
      // validate property value for resource group
      Map<String,String> rgProps = ops.getProperties(group);
      assertEquals(1, rgProps.size());
      assertEquals(value, rgProps.get(property.getKey()));
    }
    // validate proper merge
    Map<String,String> sysConfig = iops.getSystemConfiguration();
    assertEquals(value, sysConfig.get(property.getKey()));
    System.clearProperty(TServerClient.DEBUG_HOST);
  }

}
