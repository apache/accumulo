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

import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.scan.ScanServerSelector;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;

public class ScanServerGroupConfigurationIT extends SharedMiniClusterBase {

  // @formatter:off
  private static final String clientConfiguration =
     "["+
     " {"+
     "   \"isDefault\": true,"+
     "   \"maxBusyTimeout\": \"5m\","+
     "   \"busyTimeoutMultiplier\": 8,"+
     "   \"scanTypeActivations\": [],"+
     "   \"timeToWaitForScanServers\":\"0s\","+
     "   \"attemptPlans\": ["+
     "     {"+
     "       \"servers\": \"3\","+
     "       \"busyTimeout\": \"33ms\","+
     "       \"salt\": \"one\""+
     "     },"+
     "     {"+
     "       \"servers\": \"13\","+
     "       \"busyTimeout\": \"33ms\","+
     "       \"salt\": \"two\""+
     "     },"+
     "     {"+
     "       \"servers\": \"100%\","+
     "       \"busyTimeout\": \"33ms\""+
     "     }"+
     "   ]"+
     "  },"+
     " {"+
     "   \"isDefault\": false,"+
     "   \"maxBusyTimeout\": \"5m\","+
     "   \"busyTimeoutMultiplier\": 8,"+
     "   \"group\": \"GROUP1\","+
     "   \"scanTypeActivations\": [\"use_group1\"],"+
     "   \"timeToWaitForScanServers\":\"0s\","+
     "   \"attemptPlans\": ["+
     "     {"+
     "       \"servers\": \"3\","+
     "       \"busyTimeout\": \"33ms\","+
     "       \"salt\": \"one\""+
     "     },"+
     "     {"+
     "       \"servers\": \"13\","+
     "       \"busyTimeout\": \"33ms\","+
     "       \"salt\": \"two\""+
     "     },"+
     "     {"+
     "       \"servers\": \"100%\","+
     "       \"busyTimeout\": \"33ms\""+
     "     }"+
     "   ]"+
     "  }"+
     "]";
  // @formatter:on

  private static class Config implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.getClusterServerConfiguration().setNumDefaultScanServers(0); // start with no scan servers
      cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT.getKey(), "10s");

      cfg.setClientProperty(ClientProperty.SCAN_SERVER_SELECTOR_OPTS_PREFIX.getKey() + "profiles",
          clientConfiguration);
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new Config());
  }

  @AfterAll
  public static void after() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testClientConfiguration() throws Exception {

    // Ensure no scan servers running
    Wait.waitFor(() -> getCluster().getServerContext().getServerPaths()
        .getScanServer(Optional.empty(), Optional.empty(), true).isEmpty());

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];

      final int ingestedEntryCount =
          ScanServerIT.createTableAndIngest(client, tableName, null, 10, 10, "colf");
      assertEquals(100, ingestedEntryCount);

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);

        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scanner should fall back to the tserver and should have seen all ingested and flushed entries");

        // Allow one scan server to be started at this time
        getCluster().getConfig().getClusterServerConfiguration().setNumDefaultScanServers(1);

        // Start a ScanServer. No group specified, should be in the default group.
        getCluster().getClusterControl().start(ServerType.SCAN_SERVER, "localhost");
        Wait.waitFor(() -> getCluster().getServerContext().getServerPaths()
            .getScanServer(Optional.empty(), Optional.empty(), true).size() == 1, 30_000);
        Wait.waitFor(() -> ((ClientContext) client).getScanServers().values().stream().anyMatch(
            (p) -> p.getSecond().equals(ScanServerSelector.DEFAULT_SCAN_SERVER_GROUP_NAME))
            == true);

        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");

        // if scanning against tserver would see the following, but should not on scan server
        final int additionalIngest1 =
            ScanServerIT.ingest(client, tableName, 10, 10, 10, "colf", true);
        assertEquals(100, additionalIngest1);

        // A a scan server for resource group GROUP1
        getCluster().getConfig().getClusterServerConfiguration()
            .addScanServerResourceGroup("GROUP1", 1);
        getCluster().getClusterControl().start(ServerType.SCAN_SERVER);
        Wait.waitFor(() -> getCluster().getServerContext().getServerPaths()
            .getScanServer(Optional.empty(), Optional.empty(), true).size() == 2, 30_000);
        Wait.waitFor(() -> ((ClientContext) client).getScanServers().values().stream().anyMatch(
            (p) -> p.getSecond().equals(ScanServerSelector.DEFAULT_SCAN_SERVER_GROUP_NAME))
            == true);
        Wait.waitFor(() -> ((ClientContext) client).getScanServers().values().stream()
            .anyMatch((p) -> p.getSecond().equals("GROUP1")) == true);

        scanner.setExecutionHints(Map.of("scan_type", "use_group1"));
        assertEquals(ingestedEntryCount + additionalIngest1, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");

        // if scanning against tserver would see the following, but should not on scan server
        final int additionalIngest2 =
            ScanServerIT.ingest(client, tableName, 10, 10, 20, "colf", false);
        assertEquals(100, additionalIngest2);

        assertEquals(ingestedEntryCount + additionalIngest1, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");

        scanner.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        assertEquals(ingestedEntryCount + additionalIngest1 + additionalIngest2,
            Iterables.size(scanner),
            "Scanning against tserver should have resulted in seeing all ingested entries");
      }
    }

  }
}
