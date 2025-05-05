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
package org.apache.accumulo.minicluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.NamespaceMapping;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class MiniAccumuloClusterExistingZooKeepersTest extends WithTestNames {
  private static final File BASE_DIR = new File(System.getProperty("user.dir")
      + "/target/mini-tests/" + MiniAccumuloClusterExistingZooKeepersTest.class.getName());

  private static final String SECRET = "superSecret";

  private MiniAccumuloConfig config;

  @BeforeEach
  public void setupTestCluster() {
    assertTrue(BASE_DIR.mkdirs() || BASE_DIR.isDirectory());
    File testDir = new File(BASE_DIR, testName());
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());

    // disable adminServer, which runs on port 8080 by default and we don't need
    System.setProperty("zookeeper.admin.enableServer", "false");
    config = new MiniAccumuloConfig(testDir, SECRET);
  }

  @Test
  public void canConnectViaExistingZooKeeper() throws Exception {
    try (TestingServer zooKeeper = new TestingServer(); MiniAccumuloCluster accumulo =
        new MiniAccumuloCluster(config.setExistingZooKeepers(zooKeeper.getConnectString()))) {
      accumulo.start();
      assertEquals(zooKeeper.getConnectString(), accumulo.getZooKeepers());

      try (AccumuloClient client =
          Accumulo.newClient().from(accumulo.getClientProperties()).as("root", SECRET).build()) {

        String tableName = "foo";
        client.tableOperations().create(tableName);
        Map<String,String> tableIds = client.tableOperations().tableIdMap();
        String tableId = tableIds.get(tableName);
        assertNotNull(tableId);

        String zkTableMapPath = String.format("%s%s/%s/tables",
            ZooUtil.getRoot(client.instanceOperations().getInstanceId()), Constants.ZNAMESPACES,
            Namespace.DEFAULT.id());
        try (CuratorFramework curatorClient =
            CuratorFrameworkFactory.newClient(zooKeeper.getConnectString(), new RetryOneTime(1))) {
          curatorClient.start();
          assertNotNull(curatorClient.checkExists().forPath(zkTableMapPath));
          assertEquals(tableName, NamespaceMapping
              .deserializeMap(curatorClient.getData().forPath(zkTableMapPath)).get(tableId));
        }
      }
    }
  }
}
