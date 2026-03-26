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

import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.manager.fate.FateManager;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class ComprehensiveMultiManagerIT extends ComprehensiveITBase {

  private static final Logger log = LoggerFactory.getLogger(ComprehensiveMultiManagerIT.class);

  private static class ComprehensiveITConfiguration implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.setProperty(Property.SSERV_CACHED_TABLET_METADATA_EXPIRATION, "5s");
      cfg.getClusterServerConfiguration().setNumManagers(3);
    }
  }

  @BeforeAll
  public static void setup() throws Exception {
    ComprehensiveITConfiguration c = new ComprehensiveITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.securityOperations().changeUserAuthorizations("root", AUTHORIZATIONS);
    }

    // Wait for 3 managers to have a fate partition assigned to them
    var srvCtx = getCluster().getServerContext();
    Wait.waitFor(() -> {
      Map<HostAndPort,FateManager.CurrentPartitions> fateAssignments =
          FateManager.getCurrentAssignments(srvCtx);
      boolean allAssigned = fateAssignments.size() == 3 && fateAssignments.values().stream()
          .noneMatch(currentPartitions -> currentPartitions.partitions().isEmpty());
      if (allAssigned) {
        fateAssignments.forEach((hostPort, partitions) -> {
          log.debug("Fate assignment {} {}", hostPort, partitions.partitions());
        });
      }
      return allAssigned;
    });

  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }
}
