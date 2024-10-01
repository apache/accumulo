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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerTypeName;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class InstanceOperationsIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "10s");
    cfg.getClusterServerConfiguration().setNumDefaultCompactors(3);
    cfg.getClusterServerConfiguration().setNumDefaultScanServers(2);
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testGetServers() throws AccumuloException, AccumuloSecurityException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      InstanceOperations iops = client.instanceOperations();

      assertEquals(3, iops.getServers(ServerTypeName.COMPACTOR).size());
      assertEquals(3, iops.getCompactors().size());
      assertTrue(
          validateAddresses(iops.getCompactors(), iops.getServers(ServerTypeName.COMPACTOR)));

      assertEquals(2, iops.getServers(ServerTypeName.SCAN_SERVER).size());
      assertEquals(2, iops.getScanServers().size());
      assertTrue(
          validateAddresses(iops.getScanServers(), iops.getServers(ServerTypeName.SCAN_SERVER)));

      assertEquals(1, iops.getServers(ServerTypeName.TABLET_SERVER).size());
      assertEquals(1, iops.getTabletServers().size());
      assertTrue(validateAddresses(iops.getTabletServers(),
          iops.getServers(ServerTypeName.TABLET_SERVER)));

      assertEquals(1, iops.getServers(ServerTypeName.MANAGER).size());
      assertEquals(1, iops.getManagerLocations().size());
      assertTrue(
          validateAddresses(iops.getManagerLocations(), iops.getServers(ServerTypeName.MANAGER)));

      for (ServerId compactor : iops.getServers(ServerTypeName.COMPACTOR)) {
        assertNotNull(iops.getActiveCompactions(compactor));
        assertThrows(IllegalArgumentException.class, () -> iops.getActiveScans(compactor));
      }

      for (ServerId tserver : iops.getServers(ServerTypeName.TABLET_SERVER)) {
        assertNotNull(iops.getActiveCompactions(tserver));
        assertNotNull(iops.getActiveScans(tserver));
      }

      for (ServerId sserver : iops.getServers(ServerTypeName.SCAN_SERVER)) {
        assertThrows(IllegalArgumentException.class, () -> iops.getActiveCompactions(sserver));
        assertNotNull(iops.getActiveScans(sserver));
      }

    }
  }

  private boolean validateAddresses(Collection<String> e, Set<ServerId> addresses) {
    List<String> actual = new ArrayList<>(addresses.size());
    addresses.forEach(a -> actual.add(a.toHostPortString()));
    List<String> expected = new ArrayList<>(e);
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(actual, expected);
    return true;
  }

}
