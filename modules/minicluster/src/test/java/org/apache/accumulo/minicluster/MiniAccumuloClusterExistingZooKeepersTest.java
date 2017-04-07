/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.minicluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniAccumuloClusterExistingZooKeepersTest {
  private static final File BASE_DIR = new File(System.getProperty("user.dir") + "/target/mini-tests/"
      + MiniAccumuloClusterExistingZooKeepersTest.class.getName());

  private static final String SECRET = "superSecret";

  private static final Logger log = LoggerFactory.getLogger(MiniAccumuloClusterExistingZooKeepersTest.class);
  private TestingServer zooKeeper;
  private MiniAccumuloCluster accumulo;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setupTestCluster() throws Exception {
    assertTrue(BASE_DIR.mkdirs() || BASE_DIR.isDirectory());
    File testDir = new File(BASE_DIR, testName.getMethodName());
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());

    zooKeeper = new TestingServer();

    MiniAccumuloConfig config = new MiniAccumuloConfig(testDir, SECRET);
    config.getImpl().setExistingZooKeepers(zooKeeper.getConnectString());
    accumulo = new MiniAccumuloCluster(config);
    accumulo.start();
  }

  @After
  public void teardownTestCluster() {
    if (accumulo != null) {
      try {
        accumulo.stop();
      } catch (IOException | InterruptedException e) {
        log.warn("Failure during tear down", e);
      }
    }

    if (zooKeeper != null) {
      try {
        zooKeeper.close();
      } catch (IOException e) {
        log.warn("Failure stopping test ZooKeeper server");
      }
    }
  }

  @Test
  public void canConnectViaExistingZooKeeper() throws Exception {
    Connector conn = accumulo.getConnector("root", SECRET);
    Instance instance = conn.getInstance();
    assertEquals(zooKeeper.getConnectString(), instance.getZooKeepers());

    String tableName = "foo";
    conn.tableOperations().create(tableName);
    Map<String,String> tableIds = conn.tableOperations().tableIdMap();
    assertTrue(tableIds.containsKey(tableName));

    String zkTablePath = String.format("/accumulo/%s/tables/%s/name", instance.getInstanceID(), tableIds.get(tableName));
    try (CuratorFramework client = CuratorFrameworkFactory.newClient(zooKeeper.getConnectString(), new RetryOneTime(1))) {
      client.start();
      assertNotNull(client.checkExists().forPath(zkTablePath));
      assertEquals(tableName, new String(client.getData().forPath(zkTablePath)));
    }
  }
}
