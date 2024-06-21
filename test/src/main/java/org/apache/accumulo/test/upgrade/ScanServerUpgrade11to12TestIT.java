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
package org.apache.accumulo.test.upgrade;

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.manager.upgrade.Upgrader11to12;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

@Tag(MINI_CLUSTER_ONLY)
public class ScanServerUpgrade11to12TestIT extends SharedMiniClusterBase {

  public static final Logger log = LoggerFactory.getLogger(ScanServerUpgrade11to12TestIT.class);

  private static class ScanServerUpgradeITConfiguration
      implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.setNumScanServers(0);
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    ScanServerUpgradeITConfiguration c = new ScanServerUpgradeITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);
  }

  @AfterAll
  public static void stop() throws Exception {
    stopMiniCluster();
  }

  private Stream<Map.Entry<Key,Value>> getOldScanServerRefs(String tableName) {
    try {
      Scanner scanner =
          getCluster().getServerContext().createScanner(tableName, Authorizations.EMPTY);
      scanner.setRange(Upgrader11to12.OLD_SCAN_SERVERS_RANGE);
      return scanner.stream().onClose(scanner::close);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException("Unable to find table " + tableName);
    }
  }

  private void testMetadataScanServerRefRemoval(String tableName) {

    HostAndPort server = HostAndPort.fromParts("127.0.0.1", 1234);
    UUID serverLockUUID = UUID.randomUUID();

    Set<ScanServerRefTabletFile> scanRefs = Stream.of("F0000070.rf", "F0000071.rf", "F0000072.rf")
        .map(f -> "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/" + f)
        .map(f -> new ScanServerRefTabletFile(serverLockUUID, server.toString(), f))
        .collect(Collectors.toSet());

    ServerContext ctx = getCluster().getServerContext();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().removeProperty(tableName,
          Property.TABLE_CONSTRAINT_PREFIX.getKey() + "1");
      log.info("Removed constraints from table {}", tableName);
      Thread.sleep(10_000);
    } catch (AccumuloException | AccumuloSecurityException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    try (BatchWriter writer = ctx.createBatchWriter(tableName)) {
      for (ScanServerRefTabletFile ref : scanRefs) {
        Mutation m = new Mutation("~sserv" + ref.getServerLockUUID().toString());
        m.put(ref.getServerAddress(), ref.getFilePath(), new Value(""));
        writer.addMutation(m);
      }
      writer.flush();
    } catch (TableNotFoundException | MutationsRejectedException e) {
      log.warn("Failed to write mutations to metadata table");
      throw new RuntimeException(e);
    }

    // Check that ample cannot find these scan server refs
    assertEquals(0, ctx.getAmple().scanServerRefs().list().count());

    // Ensure they exist on the metadata table
    assertEquals(scanRefs.size(), getOldScanServerRefs(tableName).count());

    var upgrader = new Upgrader11to12();
    upgrader.removeScanServerRange(ctx, tableName);

    // Ensure entries are now removed from the metadata table
    assertEquals(0, getOldScanServerRefs(tableName).count());
  }

  @Test
  public void testMetadataScanServerRefs() {
    testMetadataScanServerRefRemoval(Ample.DataLevel.USER.metaTable());
  }

  @Test
  public void testRootScanServerRefs() {
    testMetadataScanServerRefRemoval(Ample.DataLevel.METADATA.metaTable());
  }
}
