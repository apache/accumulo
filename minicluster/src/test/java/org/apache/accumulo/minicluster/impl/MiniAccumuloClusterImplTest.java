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
package org.apache.accumulo.minicluster.impl;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MiniAccumuloClusterImplTest {
  public static File testDir;

  private static MiniAccumuloClusterImpl accumulo;

  private static final int NUM_TSERVERS = 2;

  private static String TEST_TABLE = "test";
  private static String testTableID;

  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);

    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    baseDir.mkdirs();
    testDir = new File(baseDir, MiniAccumuloClusterImplTest.class.getName());
    FileUtils.deleteQuietly(testDir);
    testDir.mkdir();

    MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(testDir, "superSecret").setJDWPEnabled(true);
    // expressly set number of tservers since we assert it later, in case the default changes
    config.setNumTservers(NUM_TSERVERS);
    accumulo = new MiniAccumuloClusterImpl(config);
    accumulo.start();
    // create a table to ensure there are some entries in the !0 table
    Connector conn = accumulo.getConnector("root", new PasswordToken("superSecret"));
    TableOperations tableops = conn.tableOperations();
    tableops.create(TEST_TABLE);
    testTableID = tableops.tableIdMap().get(TEST_TABLE);

    Scanner s = conn.createScanner(TEST_TABLE, Authorizations.EMPTY);
    for (@SuppressWarnings("unused")
    Entry<Key,Value> e : s) {}
  }

  @Test(timeout = 10000)
  public void testAccurateProcessListReturned() throws Exception {
    Map<ServerType,Collection<ProcessReference>> procs = accumulo.getProcesses();

    Assert.assertTrue(procs.containsKey(ServerType.GARBAGE_COLLECTOR));

    for (ServerType t : new ServerType[] {ServerType.MASTER, ServerType.TABLET_SERVER, ServerType.ZOOKEEPER}) {
      Assert.assertTrue(procs.containsKey(t));
      Collection<ProcessReference> procRefs = procs.get(t);
      Assert.assertTrue(1 <= procRefs.size());

      for (ProcessReference procRef : procRefs) {
        Assert.assertNotNull(procRef);
      }
    }
  }

  @Test(timeout = 60000)
  public void saneMonitorInfo() throws Exception {
    MasterMonitorInfo stats;
    while (true) {
      stats = accumulo.getMasterMonitorInfo();
      if (null != stats.tServerInfo && stats.tServerInfo.size() == NUM_TSERVERS) {
        break;
      }
    }
    List<MasterState> validStates = Arrays.asList(MasterState.values());
    List<MasterGoalState> validGoals = Arrays.asList(MasterGoalState.values());
    Assert.assertTrue("master state should be valid.", validStates.contains(stats.state));
    Assert.assertTrue("master goal state should be in " + validGoals + ". is " + stats.goalState, validGoals.contains(stats.goalState));
    Assert.assertNotNull("should have a table map.", stats.tableMap);
    Assert.assertTrue("root table should exist in " + stats.tableMap.keySet(), stats.tableMap.keySet().contains(RootTable.ID));
    Assert.assertTrue("meta table should exist in " + stats.tableMap.keySet(), stats.tableMap.keySet().contains(MetadataTable.ID));
    Assert.assertTrue("our test table should exist in " + stats.tableMap.keySet(), stats.tableMap.keySet().contains(testTableID));
    Assert.assertNotNull("there should be tservers.", stats.tServerInfo);
    Assert.assertEquals(NUM_TSERVERS, stats.tServerInfo.size());
  }

  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
  }

}
