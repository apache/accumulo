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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class EmptyServerLockCleanupIT extends SharedMiniClusterBase {
  private static final String COMPACTOR_QUEUE = "cleanup-test";
  private static final long CLEANUP_TIMEOUT = Duration.ofMinutes(6).toMillis();

  private static class EmptyServerLockCleanupITConfiguration
      implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setNumScanServers(0);
      cfg.setNumCompactors(0);
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    EmptyServerLockCleanupITConfiguration c = new EmptyServerLockCleanupITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);
    getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);
  }

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(7);
  }

  @Test
  public void testEmptyServerLockNodesAreCleanedUp() throws Exception {
    ServerContext ctx = getCluster().getServerContext();
    ZooReaderWriter zrw = ctx.getZooReaderWriter();
    String zooRoot = ctx.getZooKeeperRoot();
    String scanServerPath = zooRoot + Constants.ZSSERVERS + "/localhost:12345";
    String compactorQueuePath = zooRoot + Constants.ZCOMPACTORS + "/" + COMPACTOR_QUEUE;
    String compactorPath = compactorQueuePath + "/localhost:12345";

    zrw.putPersistentData(scanServerPath, new byte[0], ZooUtil.NodeExistsPolicy.FAIL);
    zrw.mkdirs(compactorQueuePath);
    zrw.putPersistentData(compactorPath, new byte[0], ZooUtil.NodeExistsPolicy.FAIL);

    assertTrue(zrw.exists(scanServerPath), "Expected empty scan-server node yet to be created");
    assertTrue(zrw.exists(compactorPath), "Expected empty compactor node yet to be created");

    Wait.waitFor(() -> !zrw.exists(scanServerPath) && !zrw.exists(compactorPath), CLEANUP_TIMEOUT,
        Wait.SLEEP_MILLIS,
        "The Manager and Compaction Coordinator should remove empty server lock nodes");

  }
}
