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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ResourceGroupNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.ResourceGroupOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.rpc.clients.TServerClient;
import org.apache.accumulo.core.security.SystemPermission;
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

      // Start the processes in the resource group
      getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(RG, 1);
      getCluster().getConfig().getClusterServerConfiguration().addScanServerResourceGroup(RG, 1);
      getCluster().getConfig().getClusterServerConfiguration().addTabletServerResourceGroup(RG, 1);
      getCluster().start();

      Wait.waitFor(() -> cc.getServerPaths()
          .getCompactor(ResourceGroupPredicate.exact(rgid), AddressSelector.all(), true).size()
          == 1);
      Wait.waitFor(() -> cc.getServerPaths()
          .getScanServer(ResourceGroupPredicate.exact(rgid), AddressSelector.all(), true).size()
          == 1);
      Wait.waitFor(() -> cc.getServerPaths()
          .getTabletServer(ResourceGroupPredicate.exact(rgid), AddressSelector.all(), true).size()
          == 1);

      assertTrue(rgOps.getProperties(rgid).isEmpty());

      rgOps.setProperty(rgid, Property.COMPACTION_WARN_TIME.getKey(), "1m");

      checkProperty(iops, rgOps,
          cc.getServerPaths().getCompactor(ResourceGroupPredicate.DEFAULT_RG_ONLY,
              AddressSelector.all(), true),
          ResourceGroupId.DEFAULT, Property.COMPACTION_WARN_TIME,
          Property.COMPACTION_WARN_TIME.getDefaultValue(),
          Property.COMPACTION_WARN_TIME.getDefaultValue());

      checkProperty(iops, rgOps,
          cc.getServerPaths().getScanServer(ResourceGroupPredicate.DEFAULT_RG_ONLY,
              AddressSelector.all(), true),
          ResourceGroupId.DEFAULT, Property.COMPACTION_WARN_TIME,
          Property.COMPACTION_WARN_TIME.getDefaultValue(),
          Property.COMPACTION_WARN_TIME.getDefaultValue());

      checkProperty(iops, rgOps,
          cc.getServerPaths().getTabletServer(ResourceGroupPredicate.DEFAULT_RG_ONLY,
              AddressSelector.all(), true),
          ResourceGroupId.DEFAULT, Property.COMPACTION_WARN_TIME,
          Property.COMPACTION_WARN_TIME.getDefaultValue(),
          Property.COMPACTION_WARN_TIME.getDefaultValue());

      checkProperty(iops, rgOps,
          cc.getServerPaths().getCompactor(ResourceGroupPredicate.exact(rgid),
              AddressSelector.all(), true),
          rgid, Property.COMPACTION_WARN_TIME, "1m",
          Property.COMPACTION_WARN_TIME.getDefaultValue());

      checkProperty(iops, rgOps,
          cc.getServerPaths().getScanServer(ResourceGroupPredicate.exact(rgid),
              AddressSelector.all(), true),
          rgid, Property.COMPACTION_WARN_TIME, "1m",
          Property.COMPACTION_WARN_TIME.getDefaultValue());

      checkProperty(iops, rgOps,
          cc.getServerPaths().getTabletServer(ResourceGroupPredicate.exact(rgid),
              AddressSelector.all(), true),
          rgid, Property.COMPACTION_WARN_TIME, "1m",
          Property.COMPACTION_WARN_TIME.getDefaultValue());

      // test error cases
      ResourceGroupId invalid = ResourceGroupId.of("INVALID");
      Consumer<Map<String,String>> consumer = (m) -> {};
      assertThrows(ResourceGroupNotFoundException.class, () -> rgOps.getProperties(invalid));
      assertThrows(ResourceGroupNotFoundException.class,
          () -> rgOps.setProperty(invalid, Property.COMPACTION_WARN_TIME.getKey(), "1m"));
      assertThrows(ResourceGroupNotFoundException.class,
          () -> rgOps.modifyProperties(invalid, consumer));
      assertThrows(ResourceGroupNotFoundException.class,
          () -> rgOps.removeProperty(invalid, Property.COMPACTION_WARN_TIME.getKey()));
      assertThrows(ResourceGroupNotFoundException.class, () -> rgOps.remove(invalid));

      getCluster().getClusterControl().stopCompactorGroup(RG);
      getCluster().getClusterControl().stopScanServerGroup(RG);
      getCluster().getClusterControl().stopTabletServerGroup(RG);

      getCluster().getConfig().getClusterServerConfiguration().clearCompactorResourceGroups();
      getCluster().getConfig().getClusterServerConfiguration().clearSServerResourceGroups();
      getCluster().getConfig().getClusterServerConfiguration().clearTServerResourceGroups();

      Wait.waitFor(() -> cc.getServerPaths()
          .getCompactor(ResourceGroupPredicate.exact(rgid), AddressSelector.all(), true).size()
          == 0);
      Wait.waitFor(() -> cc.getServerPaths()
          .getScanServer(ResourceGroupPredicate.exact(rgid), AddressSelector.all(), true).size()
          == 0);
      Wait.waitFor(() -> cc.getServerPaths()
          .getTabletServer(ResourceGroupPredicate.exact(rgid), AddressSelector.all(), true).size()
          == 0);

      rgOps.remove(rgid);
      assertFalse(zrw.exists(rgpk.getPath()));
      assertFalse(zrw.exists(rgpk.getPath()));
      assertFalse(zrw.exists(Constants.ZRESOURCEGROUPS + "/" + rgid.canonical()));

    }

  }

  private void checkProperty(InstanceOperations iops, ResourceGroupOperations ops,
      Set<ServiceLockPath> locks, ResourceGroupId group, Property property, String rgValue,
      String defaultValue)
      throws AccumuloException, AccumuloSecurityException, ResourceGroupNotFoundException {
    assertEquals(1, locks.size());
    ServiceLockPath slp = locks.iterator().next();
    assertEquals(group, slp.getResourceGroup());
    String serverAddr = slp.getServer();
    System.setProperty(TServerClient.DEBUG_HOST, serverAddr);
    if (!group.equals(ResourceGroupId.DEFAULT)) {
      // validate property value for resource group
      Map<String,String> rgProps = ops.getProperties(group);
      assertEquals(1, rgProps.size());
      assertEquals(rgValue, rgProps.get(property.getKey()));
    }
    // validate proper merge
    Map<String,String> sysConfig = iops.getSystemConfiguration();
    assertEquals(defaultValue, sysConfig.get(property.getKey()));
    System.clearProperty(TServerClient.DEBUG_HOST);
  }

  @Test
  public void testDefaultResourceGroup() throws Exception {
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      Set<ResourceGroupId> rgs = client.resourceGroupOperations().list();
      assertEquals(1, rgs.size());
      assertEquals(ResourceGroupId.DEFAULT, rgs.iterator().next());
      client.resourceGroupOperations().create(ResourceGroupId.DEFAULT);
      assertThrows(AccumuloException.class,
          () -> client.resourceGroupOperations().remove(ResourceGroupId.DEFAULT));
    }
  }

  @Test
  public void testDuplicateCreatesRemovals()
      throws AccumuloException, AccumuloSecurityException, ResourceGroupNotFoundException {
    final var rgid = ResourceGroupId.of("DUPES");
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      Set<ResourceGroupId> rgs = client.resourceGroupOperations().list();
      assertEquals(1, rgs.size());
      assertEquals(ResourceGroupId.DEFAULT, rgs.iterator().next());
      client.resourceGroupOperations().create(rgid);
      Set<ResourceGroupId> rgs2 = new HashSet<>(client.resourceGroupOperations().list());
      assertEquals(2, rgs2.size());
      assertTrue(rgs2.remove(ResourceGroupId.DEFAULT));
      assertEquals(rgid, rgs2.iterator().next());
      client.resourceGroupOperations().create(rgid); // creating again succeeds doing nothing
      client.resourceGroupOperations().remove(rgid);
      rgs = client.resourceGroupOperations().list();
      assertEquals(1, rgs.size());
      assertEquals(ResourceGroupId.DEFAULT, rgs.iterator().next());
      assertThrows(ResourceGroupNotFoundException.class,
          () -> client.resourceGroupOperations().remove(rgid));
    }
  }

  @Test
  public void testPermissions() throws Exception {

    ClusterUser testUser = getUser(0);

    String principal = testUser.getPrincipal();
    AuthenticationToken token = testUser.getToken();
    PasswordToken passwordToken = null;
    if (token instanceof PasswordToken) {
      passwordToken = (PasswordToken) token;
    }

    ResourceGroupId rgid = ResourceGroupId.of("TEST_GROUP");

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      client.securityOperations().createLocalUser(principal, passwordToken);
    }

    try (AccumuloClient test_user_client =
        Accumulo.newClient().from(getClientProps()).as(principal, token).build()) {
      assertThrows(AccumuloSecurityException.class,
          () -> test_user_client.resourceGroupOperations().create(rgid));
    }

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      client.resourceGroupOperations().create(rgid);
    }

    try (AccumuloClient test_user_client =
        Accumulo.newClient().from(getClientProps()).as(principal, token).build()) {
      assertThrows(AccumuloSecurityException.class,
          () -> test_user_client.resourceGroupOperations().remove(rgid));
    }

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      client.securityOperations().grantSystemPermission(principal, SystemPermission.SYSTEM);
    }

    // Regular user with SYSTEM permission should now be able to create/remove
    try (AccumuloClient test_user_client =
        Accumulo.newClient().from(getClientProps()).as(principal, token).build()) {
      test_user_client.resourceGroupOperations().remove(rgid);
      test_user_client.resourceGroupOperations().create(rgid);
    }
  }
}
