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

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

public class RootRecoveryIT extends SharedMiniClusterBase {

  private static final int ZK_TIMEOUT_MS = 5000;
  private static final Logger LOG = LoggerFactory.getLogger(RootRecoveryIT.class);

  private static class RootRecoveryITConfigurationCallback
      implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, ZK_TIMEOUT_MS + "ms");
    }
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @BeforeAll
  public static void start() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new RootRecoveryITConfigurationCallback());
  }

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testRootRecovery() throws Exception {

    // Insert some delete mutations into the metadata table
    try (BatchWriter writer =
        getCluster().getServerContext().createBatchWriter(SystemTables.METADATA.tableName())) {
      var mutation = new Mutation("3;<");
      mutation.putDelete(TabletColumnFamily.AVAILABILITY_COLUMN.getColumnFamily(),
          TabletColumnFamily.AVAILABILITY_COLUMN.getColumnQualifier());
      writer.addMutation(mutation);
      writer.flush();
    } catch (TableNotFoundException | MutationsRejectedException e) {
      throw new RuntimeException(e);
    }

    // Compact the metadata table, which will end up writing mutations
    // to the root tablet for new metadata files
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().compact(SystemTables.METADATA.tableName(), null, null, true, true);
    }

    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);

    // Let the ZooKeeper locks expire
    Thread.sleep(ZK_TIMEOUT_MS * 2);

    getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);
    getCluster().getClusterControl().startAllServers(ServerType.MANAGER);

    // This scan should not time out
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      Wait.waitFor(() -> c.instanceOperations().getServers(Type.MANAGER).size() == 1);
      Wait.waitFor(() -> c.instanceOperations().getServers(Type.TABLET_SERVER).size() == 2);

      LOG.info("Scanning root table");
      Scanner s = c.createScanner(SystemTables.ROOT.tableName());
      @SuppressWarnings("unused")
      int ignoredSize = Iterables.size(s);
    }

  }

}
