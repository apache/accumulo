/*
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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import com.google.common.collect.Iterables;

public class DeleteRowsTest {
  private static final Logger log = Logger.getLogger(DeleteRowsTest.class);

  @Rule
  public TestName testName = new TestName();

  private static String secret = "superSecret";
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), secret);
    cfg.setNumTservers(1);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
    
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }

  @Test(timeout = 5 * 60 * 1000)
  public void test() throws Exception {
    ZooKeeperInstance zk = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector c = zk.getConnector("root", new PasswordToken(secret));
    for (int i = 0; i < 20; i++) {
      final String tableName = testName.getMethodName() + i;
      log.debug("Creating " + tableName);
      c.tableOperations().create(tableName);
      log.debug("Deleting rows from " + tableName);
      c.tableOperations().deleteRows(tableName, null, null);
      log.debug("Verifying no rows were found");
      Scanner scanner = c.createScanner(tableName, new Authorizations());
      assertEquals(0, Iterables.size(scanner));
    }
  }
}
