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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.server.mini.MiniAccumuloCluster;
import org.apache.accumulo.server.mini.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MetaSplitTest {
  
  private static String secret = "superSecret";
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;

  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), secret);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();

  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }
  
  private void addSplits(TableOperations opts, String ... points) throws Exception {
    SortedSet<Text> splits = new TreeSet<Text>();
    for (String point : points) {
      splits.add(new Text(point));
    }
    opts.addSplits(Constants.METADATA_TABLE_NAME, splits);
  }
  
  @Test(timeout = 60000)
  public void testMetaSplit() throws Exception {
    Instance instance = new ZooKeeperInstance(cluster.getConfig().getInstanceName(), cluster.getConfig().getZooKeepers());
    Connector connector = instance.getConnector("root", new PasswordToken(secret));
    TableOperations opts = connector.tableOperations();
    for (int i = 1; i <= 10; i++) {
      opts.create("" + i);
    }
    opts.merge(Constants.METADATA_TABLE_NAME, new Text("01"), new Text("02"));
    assertEquals(2, opts.listSplits(Constants.METADATA_TABLE_NAME).size());
    addSplits(opts, "4 5 6 7 8".split(" "));
    assertEquals(7, opts.listSplits(Constants.METADATA_TABLE_NAME).size());
    opts.merge(Constants.METADATA_TABLE_NAME, new Text("6"), new Text("9"));
    assertEquals(5, opts.listSplits(Constants.METADATA_TABLE_NAME).size());
    addSplits(opts, "44 55 66 77 88".split(" "));
    assertEquals(10, opts.listSplits(Constants.METADATA_TABLE_NAME).size());
    opts.merge(Constants.METADATA_TABLE_NAME, new Text("5"), new Text("7"));
    assertEquals(7, opts.listSplits(Constants.METADATA_TABLE_NAME).size());
    opts.merge(Constants.METADATA_TABLE_NAME, null, null);
    assertEquals(1, opts.listSplits(Constants.METADATA_TABLE_NAME).size());
  }
  
}
