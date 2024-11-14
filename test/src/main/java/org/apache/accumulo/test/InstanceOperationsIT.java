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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class InstanceOperationsIT extends AccumuloClusterHarness {

  final int NUM_COMPACTORS = 3;
  final int NUM_SCANSERVERS = 2;
  final int NUM_TABLETSERVERS = 1;

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "10s");
    cfg.getClusterServerConfiguration().setNumDefaultCompactors(NUM_COMPACTORS);
    cfg.getClusterServerConfiguration().setNumDefaultScanServers(NUM_SCANSERVERS);
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(NUM_TABLETSERVERS);
  }

  /**
   * Verify that we get the same servers from getServers() and getServer()
   */
  @Test
  public void testGetServer() {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final InstanceOperations iops = client.instanceOperations();

      // Verify scan servers
      final Set<ServerId> sservers = iops.getServers(ServerId.Type.SCAN_SERVER);
      assertEquals(NUM_SCANSERVERS, sservers.size());
      sservers.forEach(expectedServerId -> {
        ServerId actualServerId =
            iops.getServer(expectedServerId.getType(), expectedServerId.getResourceGroup(),
                expectedServerId.getHost(), expectedServerId.getPort());
        assertNotNull(actualServerId, "Expected to find scan server " + expectedServerId);
        assertEquals(expectedServerId, actualServerId);
      });

      // Verify tablet servers
      final Set<ServerId> tservers = iops.getServers(ServerId.Type.TABLET_SERVER);
      assertEquals(NUM_TABLETSERVERS, tservers.size());
      tservers.forEach(expectedServerId -> {
        ServerId actualServerId =
            iops.getServer(expectedServerId.getType(), expectedServerId.getResourceGroup(),
                expectedServerId.getHost(), expectedServerId.getPort());
        assertNotNull(actualServerId, "Expected to find tablet server " + expectedServerId);
        assertEquals(expectedServerId, actualServerId);
      });

      // Verify compactors
      final Set<ServerId> compactors = iops.getServers(ServerId.Type.COMPACTOR);
      assertEquals(NUM_COMPACTORS, compactors.size());
      compactors.forEach(expectedServerId -> {
        ServerId actualServerId =
            iops.getServer(expectedServerId.getType(), expectedServerId.getResourceGroup(),
                expectedServerId.getHost(), expectedServerId.getPort());
        assertNotNull(actualServerId, "Expected to find compactor " + expectedServerId);
        assertEquals(expectedServerId, actualServerId);
      });

      // Verify managers
      final Set<ServerId> managers = iops.getServers(ServerId.Type.MANAGER);
      assertEquals(1, managers.size()); // Assuming there is only one manager
      managers.forEach(expectedServerId -> {
        ServerId actualServerId =
            iops.getServer(expectedServerId.getType(), expectedServerId.getResourceGroup(),
                expectedServerId.getHost(), expectedServerId.getPort());
        assertNotNull(actualServerId, "Expected to find manager " + expectedServerId);
        assertEquals(expectedServerId, actualServerId);
      });

      // verify GC
      final Set<ServerId> gcs = iops.getServers(ServerId.Type.GARBAGE_COLLECTOR);
      assertEquals(1, gcs.size()); // Assuming there is only one garbage collector
      gcs.forEach(expectedServerId -> {
        ServerId actualServerId =
            iops.getServer(expectedServerId.getType(), expectedServerId.getResourceGroup(),
                expectedServerId.getHost(), expectedServerId.getPort());
        assertNotNull(actualServerId, "Expected to find manager " + expectedServerId);
        assertEquals(expectedServerId, actualServerId);
      });

      // monitor not started in MAC
      final Set<ServerId> mon = iops.getServers(ServerId.Type.MONITOR);
      assertEquals(0, mon.size());

    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testGetServers() throws AccumuloException, AccumuloSecurityException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      InstanceOperations iops = client.instanceOperations();

      assertEquals(NUM_COMPACTORS, iops.getServers(ServerId.Type.COMPACTOR).size());
      assertEquals(NUM_COMPACTORS, iops.getCompactors().size());
      validateAddresses(iops.getCompactors(), iops.getServers(ServerId.Type.COMPACTOR));

      assertEquals(NUM_SCANSERVERS, iops.getServers(ServerId.Type.SCAN_SERVER).size());
      assertEquals(NUM_SCANSERVERS, iops.getScanServers().size());
      validateAddresses(iops.getScanServers(), iops.getServers(ServerId.Type.SCAN_SERVER));

      assertEquals(NUM_TABLETSERVERS, iops.getServers(ServerId.Type.TABLET_SERVER).size());
      assertEquals(NUM_TABLETSERVERS, iops.getTabletServers().size());
      validateAddresses(iops.getTabletServers(), iops.getServers(ServerId.Type.TABLET_SERVER));

      assertEquals(1, iops.getServers(ServerId.Type.MANAGER).size());
      assertEquals(1, iops.getManagerLocations().size());
      validateAddresses(iops.getManagerLocations(), iops.getServers(ServerId.Type.MANAGER));

      assertEquals(1, iops.getServers(ServerId.Type.GARBAGE_COLLECTOR).size());

      // Monitor not started in MAC
      assertEquals(0, iops.getServers(ServerId.Type.MONITOR).size());

      for (ServerId compactor : iops.getServers(ServerId.Type.COMPACTOR)) {
        assertNotNull(iops.getActiveCompactions(compactor));
        assertThrows(IllegalArgumentException.class, () -> iops.getActiveScans(compactor));
      }

      for (ServerId tserver : iops.getServers(ServerId.Type.TABLET_SERVER)) {
        assertNotNull(iops.getActiveCompactions(tserver));
        assertNotNull(iops.getActiveScans(tserver));
      }

      for (ServerId sserver : iops.getServers(ServerId.Type.SCAN_SERVER)) {
        assertThrows(IllegalArgumentException.class, () -> iops.getActiveCompactions(sserver));
        assertNotNull(iops.getActiveScans(sserver));
      }

    }
  }

  @Test
  public void testPing() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Wait.waitFor(() -> client.instanceOperations().getServers(ServerId.Type.COMPACTOR).size()
          == NUM_COMPACTORS);
      Wait.waitFor(() -> client.instanceOperations().getServers(ServerId.Type.SCAN_SERVER).size()
          == NUM_SCANSERVERS);
      Wait.waitFor(() -> client.instanceOperations().getServers(ServerId.Type.TABLET_SERVER).size()
          == NUM_TABLETSERVERS);

      final InstanceOperations io = client.instanceOperations();
      Set<ServerId> servers = io.getServers(ServerId.Type.COMPACTOR);
      for (ServerId sid : servers) {
        io.ping(sid);
      }

      servers = io.getServers(ServerId.Type.SCAN_SERVER);
      for (ServerId sid : servers) {
        io.ping(sid);
      }

      servers = io.getServers(ServerId.Type.TABLET_SERVER);
      for (ServerId sid : servers) {
        io.ping(sid);
      }

      ServerId fake = new ServerId(ServerId.Type.COMPACTOR, Constants.DEFAULT_RESOURCE_GROUP_NAME,
          "localhost", 1024);
      assertThrows(AccumuloException.class, () -> io.ping(fake));
    }

  }

  private void validateAddresses(Collection<String> e, Set<ServerId> addresses) {
    List<String> actual = new ArrayList<>(addresses.size());
    addresses.forEach(a -> actual.add(a.toHostPortString()));
    List<String> expected = new ArrayList<>(e);
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(actual, expected);
  }

}
