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
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerTypeName;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ServerIdsIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.getClusterServerConfiguration().setNumDefaultCompactors(2);
    cfg.getClusterServerConfiguration().setNumDefaultScanServers(2);
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(2);
    cfg.getClusterServerConfiguration().addCompactorResourceGroup("C1", 1);
    cfg.getClusterServerConfiguration().addScanServerResourceGroup("SS1", 1);
    cfg.getClusterServerConfiguration().addTabletServerResourceGroup("TS1", 1);
  }

  @Test
  public void testCompactors() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      assertEquals(3, c.instanceOperations().getServers(ServerTypeName.COMPACTOR).size());
      assertEquals(2,
          c.instanceOperations()
              .getServers(ServerTypeName.COMPACTOR,
                  (s) -> s.getResourceGroup().equals(Constants.DEFAULT_RESOURCE_GROUP_NAME))
              .size());
      assertEquals(1, c.instanceOperations()
          .getServers(ServerTypeName.COMPACTOR, (s) -> s.getResourceGroup().equals("C1")).size());
      assertEquals(0, c.instanceOperations()
          .getServers(ServerTypeName.COMPACTOR, (s) -> s.getResourceGroup().equals("DOESNT_EXIST"))
          .size());
      Set<ServerId> servers = c.instanceOperations().getServers(ServerTypeName.COMPACTOR);
      for (ServerId server : servers) {
        ServerId identity =
            c.instanceOperations().getServer(server.getType(), server.getHost(), server.getPort());
        assertNotNull(identity);
        assertEquals(server, identity);
      }
      assertNull(
          c.instanceOperations().getServer(ServerTypeName.COMPACTOR, "test.localhost", 4242));
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testManager() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      assertEquals(1, c.instanceOperations().getServers(ServerTypeName.MANAGER).size());
      assertEquals(1, c.instanceOperations().getManagerLocations().size());

      Set<ServerId> managers = c.instanceOperations().getServers(ServerTypeName.MANAGER);
      ServerId manager = managers.iterator().next();

      assertEquals(HostAndPort.fromParts(manager.getHost(), manager.getPort()).toString(),
          c.instanceOperations().getManagerLocations().get(0));
    }
  }

  @Test
  public void testScanServers() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      assertEquals(3, c.instanceOperations().getServers(ServerTypeName.SCAN_SERVER).size());
      assertEquals(2,
          c.instanceOperations()
              .getServers(ServerTypeName.SCAN_SERVER,
                  (s) -> s.getResourceGroup().equals(Constants.DEFAULT_RESOURCE_GROUP_NAME))
              .size());
      assertEquals(1,
          c.instanceOperations()
              .getServers(ServerTypeName.SCAN_SERVER, (s) -> s.getResourceGroup().equals("SS1"))
              .size());
      assertEquals(0, c.instanceOperations().getServers(ServerTypeName.SCAN_SERVER,
          (s) -> s.getResourceGroup().equals("DOESNT_EXIST")).size());
      Set<ServerId> servers = c.instanceOperations().getServers(ServerTypeName.SCAN_SERVER);
      for (ServerId server : servers) {
        ServerId identity =
            c.instanceOperations().getServer(server.getType(), server.getHost(), server.getPort());
        assertNotNull(identity);
        assertEquals(server, identity);
      }
      assertNull(
          c.instanceOperations().getServer(ServerTypeName.SCAN_SERVER, "test.localhost", 4242));
    }
  }

  @Test
  public void testTabletServers() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      assertEquals(3, c.instanceOperations().getServers(ServerTypeName.TABLET_SERVER).size());
      assertEquals(2,
          c.instanceOperations()
              .getServers(ServerTypeName.TABLET_SERVER,
                  (s) -> s.getResourceGroup().equals(Constants.DEFAULT_RESOURCE_GROUP_NAME))
              .size());
      assertEquals(1,
          c.instanceOperations()
              .getServers(ServerTypeName.TABLET_SERVER, (s) -> s.getResourceGroup().equals("TS1"))
              .size());
      assertEquals(0, c.instanceOperations().getServers(ServerTypeName.TABLET_SERVER,
          (s) -> s.getResourceGroup().equals("DOESNT_EXIST")).size());
      Set<ServerId> servers = c.instanceOperations().getServers(ServerTypeName.TABLET_SERVER);
      for (ServerId server : servers) {
        ServerId identity =
            c.instanceOperations().getServer(server.getType(), server.getHost(), server.getPort());
        assertNotNull(identity);
        assertEquals(server, identity);
      }
      assertNull(
          c.instanceOperations().getServer(ServerTypeName.TABLET_SERVER, "test.localhost", 4242));
    }
  }

}
