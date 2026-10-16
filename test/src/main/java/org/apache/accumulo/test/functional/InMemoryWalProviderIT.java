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

import static org.apache.accumulo.minicluster.ServerType.GARBAGE_COLLECTOR;
import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.spi.wal.WalProvider;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.log.DfsWalProvider;
import org.apache.accumulo.server.log.WalStateManager;
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

public class InMemoryWalProviderIT extends ConfigurableMacBase {

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
    String providerClass = InMemoryWalProvider.class.getName();
    String providerConfig = "{\"" + DfsWalProvider.class.getName() + "\":{},\"" + providerClass
        + "\":{\"host\":\"" + options.get(InMemoryWalProvider.HOST_OPTION) + "\",\"port\":\""
        + options.get(InMemoryWalProvider.PORT_OPTION) + "\"}}";
    cfg.setProperty(Property.INSTANCE_WAL_PROVIDER, providerClass);
    cfg.setProperty(Property.INSTANCE_WAL_PROVIDERS, providerConfig);
    cfg.setProperty(Property.TSERV_WAL_MAX_SIZE, "1M");
    cfg.setProperty(Property.GC_CYCLE_START, "0s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.MANAGER_RECOVERY_DELAY, "1s");
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
  public void recoversAndGarbageCollectsWalThroughProvider() throws Exception {
    var control = getCluster().getClusterControl();
    control.stop(GARBAGE_COLLECTOR);
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      VerifyParams params = new VerifyParams(getClientProperties(), tableName);
      params.rows = 2_000;
      params.dataSize = 1_024;
      TestIngest.ingest(client, params);
      VerifyIngest.verifyIngest(client, params);

      Set<WalStatePath> walStates = new WalStateManager(getServerContext()).getAllState();
      var provider =
          getServerContext().getWalProviderRegistry().get(InMemoryWalProvider.PROVIDER_ID);
      assertTrue(getServerContext().getWalProviderRegistry().getIds()
          .containsAll(Set.of(DfsWalProvider.PROVIDER_ID, InMemoryWalProvider.PROVIDER_ID)));
      Set<String> walReferences = walStates.stream()
          .filter(state -> state.providerId().equals(InMemoryWalProvider.PROVIDER_ID))
          .map(state -> state.path().toString()).collect(Collectors.toSet());
      assertFalse(walReferences.isEmpty(), "SPI-created WALs should be present");
      for (String reference : walReferences) {
        assertTrue(provider.exists(reference), "SPI-created WAL should exist: " + reference);
      }

      control.stopAllServers(TABLET_SERVER);
      control.startAllServers(TABLET_SERVER);
      VerifyIngest.verifyIngest(client, params);
      client.tableOperations().flush(tableName, null, null, true);
      for (String systemTable : new String[] {SystemTables.METADATA.tableName(),
          SystemTables.ROOT.tableName(), SystemTables.FATE.tableName()}) {
        client.tableOperations().flush(systemTable, null, null, true);
      }

      control.start(GARBAGE_COLLECTOR);
      Wait.waitFor(() -> anyWalDeleted(provider, walReferences),
          "Garbage collector did not delete recovered WALs through their provider");
    }
  }

  private static boolean anyWalDeleted(WalProvider provider, Set<String> references)
      throws IOException {
    for (String reference : references) {
      if (!provider.exists(reference)) {
        return true;
      }
    }
    return false;
  }
}
