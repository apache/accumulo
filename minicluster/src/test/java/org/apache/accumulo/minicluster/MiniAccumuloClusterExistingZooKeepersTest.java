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
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MiniAccumuloClusterExistingZooKeepersTest {
  private static final File BASE_DIR = new File(System.getProperty("user.dir")
      + "/target/mini-tests/" + MiniAccumuloClusterExistingZooKeepersTest.class.getName());

  private static final String SECRET = "superSecret";

  private MiniAccumuloConfig config;

  @Rule
  public TestName testName = new TestName();

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "intput determined by test")
  @Before
  public void setupTestCluster() {
    assertTrue(BASE_DIR.mkdirs() || BASE_DIR.isDirectory());
    File testDir = new File(BASE_DIR, testName.getMethodName());
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());
    config = new MiniAccumuloConfig(testDir, SECRET);
  }

  @Test
  public void canConnectViaExistingZooKeeper() throws Exception {
    try(TestingServer zooKeeper = new TestingServer();
        MiniAccumuloCluster accumulo = new MiniAccumuloCluster(config.setZooKeeperPort(zooKeeper.getPort()))
    ) {
      accumulo.start();

      AccumuloClient client = accumulo.getAccumuloClient("root", new PasswordToken(SECRET));
      ClientContext context = new ClientContext(accumulo.getClientInfo());

      String tableName = "foo";
      client.tableOperations().create(tableName);
      Map<String, String> tableIds = client.tableOperations().tableIdMap();
      assertTrue(tableIds.containsKey(tableName));

      String zkTablePath = String.format("/accumulo/%s/tables/%s/name", context.getInstanceID(),
              tableIds.get(tableName));
      try (CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zooKeeper.getConnectString(),
              new RetryOneTime(1))) {
        zkClient.start();
        assertNotNull(zkClient.checkExists().forPath(zkTablePath));
        assertEquals(tableName, new String(zkClient.getData().forPath(zkTablePath)));
      }
    }
  }
}
