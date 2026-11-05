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

import static org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector.all;
import static org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate.DEFAULT_RG_ONLY;
import static org.apache.accumulo.minicluster.ServerType.GARBAGE_COLLECTOR;
import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.spi.wal.WalProvider;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.server.log.DfsWalProvider;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalState;
import org.apache.accumulo.server.log.WalStateManager.WalStatePath;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.accumulo.test.util.Wait;
import org.apache.accumulo.test.wal.InMemoryWalProvider;
import org.apache.accumulo.test.wal.InMemoryWalStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class WalProviderSwitchIT extends ConfigurableMacBase {

  private InMemoryWalStore walStore;

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    try {
      if (walStore == null) {
        walStore = InMemoryWalStore.start();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to start in-memory WAL store", e);
    }

    var options = walStore.getProviderOptions();
    String memoryProviderClass = InMemoryWalProvider.class.getName();
    String providers = "{\"" + DfsWalProvider.class.getName() + "\":{},\"" + memoryProviderClass
        + "\":{\"host\":\"" + options.get(InMemoryWalProvider.HOST_OPTION) + "\",\"port\":\""
        + options.get(InMemoryWalProvider.PORT_OPTION) + "\"}}";
    cfg.setProperty(Property.INSTANCE_WAL_PROVIDER, DfsWalProvider.class.getName());
    cfg.setProperty(Property.INSTANCE_WAL_PROVIDERS, providers);
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.MANAGER_RECOVERY_DELAY, "1s");
    cfg.setProperty(Property.GC_CYCLE_START, "0s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.TSERV_WAL_MAX_SIZE, "2M");
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @AfterEach
  @Override
  public void tearDown() {
    try {
      super.tearDown();
    } finally {
      if (walStore != null) {
        walStore.close();
        walStore = null;
      }
    }
  }

  @Test
  public void recoversDfsWalAfterSwitchingCurrentProvider() throws Exception {
    var control = getCluster().getClusterControl();
    control.stop(GARBAGE_COLLECTOR);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      client.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "sync");

      VerifyParams params = new VerifyParams(getClientProperties(), tableName);
      params.rows = 2_000;
      params.dataSize = 1_024;
      TestIngest.ingest(client, params);
      VerifyIngest.verifyIngest(client, params);

      Set<WalStatePath> initialWals = new WalStateManager(getServerContext()).getAllState();
      Set<String> dfsReferences =
          initialWals.stream().filter(wal -> wal.providerId().equals(DfsWalProvider.PROVIDER_ID))
              .map(wal -> wal.path().toString()).collect(Collectors.toSet());
      assertFalse(dfsReferences.isEmpty(), "Initial WALs should use the DFS provider");

      Set<ProcessReference> tabletServers =
          getCluster().getProcesses().get(TABLET_SERVER).stream().collect(Collectors.toSet());
      assertFalse(tabletServers.isEmpty(), "Expected a running TabletServer process");
      ServiceLockPath tabletServerLock = getServerContext().getServerPaths()
          .getTabletServer(DEFAULT_RG_ONLY, all(), true).stream().findFirst().orElseThrow();
      ServiceLock.deleteLock(getServerContext().getZooSession().asReaderWriter(), tabletServerLock);

      Wait.waitFor(() -> tabletServers.stream().noneMatch(ref -> ref.getProcess().isAlive()),
          "TabletServer did not exit after its ZooKeeper lock was removed");

      // Preserve the WALs and metadata, but stop all processes before changing the instance-wide
      // site property. The WAL provider map continues to include both providers.
      getCluster().stop();
      setCurrentWalProvider(InMemoryWalProvider.class.getName());
      getCluster().start();

      VerifyIngest.verifyIngest(client, params);

      VerifyParams newRows = new VerifyParams(getClientProperties(), tableName);
      newRows.startRow = params.rows;
      newRows.rows = 1;
      newRows.dataSize = params.dataSize;
      TestIngest.ingest(client, newRows);
      VerifyIngest.verifyIngest(client, newRows);

      WalProvider memoryProvider =
          getServerContext().getWalProviderRegistry().get(InMemoryWalProvider.PROVIDER_ID);
      Set<WalStatePath> walsAfterSwitch = new WalStateManager(getServerContext()).getAllState();
      Set<String> memoryReferences = walsAfterSwitch.stream()
          .filter(wal -> wal.state() == WalState.OPEN
              && wal.providerId().equals(InMemoryWalProvider.PROVIDER_ID))
          .map(wal -> wal.path().toString()).collect(Collectors.toSet());
      assertFalse(memoryReferences.isEmpty(), "New WALs should use the in-memory provider");
      for (String reference : memoryReferences) {
        assertTrue(memoryProvider.exists(reference), "In-memory WAL should exist: " + reference);
      }
    }
  }

  private void setCurrentWalProvider(String providerClass) throws IOException {
    var config = getCluster().getConfig();
    config.setProperty(Property.INSTANCE_WAL_PROVIDER, providerClass);
    Properties siteProperties = new Properties();
    siteProperties.putAll(config.getSiteConfig());
    try (OutputStream output =
        Files.newOutputStream(config.getConfDir().toPath().resolve("accumulo.properties"))) {
      siteProperties.store(output, "Updated WAL provider for integration test restart");
    }
  }
}
