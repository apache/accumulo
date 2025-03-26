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
package org.apache.accumulo.test.fate;

import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import com.google.common.collect.MoreCollectors;

/**
 * A class with utilities for testing FATE
 */
public class FateTestUtil {
  // A FateOperation for testing purposes when a FateOperation is needed and whose value needs to
  // be a FateOperation workable by USER and META FATEs
  public static final Fate.FateOperation TEST_FATE_OP = Fate.FateOperation.TABLE_COMPACT;

  /**
   * Create the fate table with the exact configuration as the real Fate user instance table
   * including table properties and TabletAvailability. For use in testing UserFateStore
   */
  public static void createFateTable(ClientContext client, String table) throws Exception {
    final var fateTableProps =
        client.tableOperations().getTableProperties(AccumuloTable.FATE.tableName());

    TabletAvailability availability;
    try (var tabletStream = client.tableOperations()
        .getTabletInformation(AccumuloTable.FATE.tableName(), new Range())) {
      availability = tabletStream.map(TabletInformation::getTabletAvailability).distinct()
          .collect(MoreCollectors.onlyElement());
    }

    var newTableConf = new NewTableConfiguration().withInitialTabletAvailability(availability)
        .withoutDefaultIterators().setProperties(fateTableProps);
    client.tableOperations().create(table, newTableConf);
    var testFateTableProps = client.tableOperations().getTableProperties(table);

    // ensure that create did not set any other props
    assertEquals(fateTableProps, testFateTableProps);
  }

  public static <T> Optional<FateId> seedTransaction(FateStore<T> store, Fate.FateOperation fateOp,
      FateKey fateKey, Repo<T> repo, boolean autoCleanUp) {
    CompletableFuture<Optional<FateId>> fateIdFuture;
    try (var seeder = store.beginSeeding()) {
      fateIdFuture = seeder.attemptToSeedTransaction(fateOp, fateKey, repo, autoCleanUp);
    }
    try {
      return fateIdFuture.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a config with all FATE operations assigned to a single pool of size numThreads for both
   * USER and META FATE operations
   */
  public static ConfigurationCopy createTestFateConfig(int numThreads) {
    ConfigurationCopy config = new ConfigurationCopy();
    // this value isn't important, just needs to be set
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_USER_CONFIG, "{\"" + Fate.FateOperation.getAllUserFateOps()
        .stream().map(Enum::name).collect(Collectors.joining(",")) + "\": " + numThreads + "}");
    config.set(Property.MANAGER_FATE_META_CONFIG, "{\"" + Fate.FateOperation.getAllMetaFateOps()
        .stream().map(Enum::name).collect(Collectors.joining(",")) + "\": " + numThreads + "}");
    config.set(Property.MANAGER_FATE_IDLE_CHECK_INTERVAL, "60m");
    return config;
  }

  /**
   * Contains the necessary utilities for setting up (and shutting down) a ZooKeeper instance for
   * use in testing MetaFateStore
   */
  @Tag(ZOOKEEPER_TESTING_SERVER)
  public static class MetaFateZKSetup {
    private static ZooKeeperTestingServer szk;
    private static ZooSession zk;

    /**
     * Sets up the ZooKeeper instance and creates the paths needed for testing MetaFateStore
     */
    public static void setup(@TempDir File tempDir) throws Exception {
      szk = new ZooKeeperTestingServer(tempDir);
      zk = szk.newClient();
      var zrw = zk.asReaderWriter();
      zrw.mkdirs(Constants.ZFATE);
      zrw.mkdirs(Constants.ZTABLE_LOCKS);
    }

    /**
     * Tears down the ZooKeeper instance
     */
    public static void teardown() throws Exception {
      szk.close();
    }

    public static ZooSession getZk() {
      return zk;
    }

  }
}
