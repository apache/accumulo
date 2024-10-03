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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ScanServerShutdownIT extends SharedMiniClusterBase {

  private static class ScanServerShutdownITConfiguration
      implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {

      cfg.getClusterServerConfiguration().setNumDefaultScanServers(1);

      // Timeout scan sessions after being idle for 3 seconds
      cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "3s");

      // Configure the scan server to only have 1 scan executor thread. This means
      // that the scan server will run scans serially, not concurrently.
      cfg.setProperty(Property.SSERV_SCAN_EXECUTORS_DEFAULT_THREADS, "1");

      // Set our custom implementation that shuts down after 3 batch scans
      cfg.setServerClass(ServerType.SCAN_SERVER, SelfStoppingScanServer.class);
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    ScanServerShutdownITConfiguration c = new ScanServerShutdownITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);
  }

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testRefRemovalOnShutdown() throws Exception {

    ServerContext ctx = getCluster().getServerContext();

    Wait.waitFor(() -> !ctx.getServerPaths().getScanServer(Optional.empty(), Optional.empty(), true)
        .isEmpty());

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      // Make multiple files
      final int fileCount = 3;
      for (int i = 0; i < fileCount; i++) {
        ScanServerIT.ingest(client, tableName, 10, 10, 0, "colf", true);
      }
      assertEquals(0, ctx.getAmple().scanServerRefs().list().count());

      for (int i = 0; i < 3; i++) {
        try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
          scanner.setRanges(Collections.singletonList(new Range()));
          scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);

          Iterator<Entry<Key,Value>> iter = scanner.iterator();
          assertTrue(iter.hasNext());
          assertNotNull(iter.next());

          assertEquals(fileCount, ctx.getAmple().scanServerRefs().list().count());

        }
      }

      // ScanServer should stop after the 3rd batch scan closes
      Wait.waitFor(() -> ((ClientContext) client).getScanServers().size() == 0);

      // The ScanServer should clean up the references on normal shutdown
      Wait.waitFor(() -> ctx.getAmple().scanServerRefs().list().count() == 0);

    } finally {
      getCluster().getClusterControl().stopAllServers(ServerType.SCAN_SERVER);
    }

  }

}
