/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.miniclusterImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterators;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class MiniAccumuloClusterImplTest {
  public static File testDir;

  private static MiniAccumuloClusterImpl accumulo;

  private static final int NUM_TSERVERS = 2;

  private static String TEST_TABLE = "test";
  private static String testTableID;

  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    testDir = new File(baseDir, MiniAccumuloClusterImplTest.class.getName());
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());

    MiniAccumuloConfigImpl config =
        new MiniAccumuloConfigImpl(testDir, "superSecret").setJDWPEnabled(true);
    // expressly set number of tservers since we assert it later, in case the default changes
    config.setNumTservers(NUM_TSERVERS);
    accumulo = new MiniAccumuloClusterImpl(config);
    accumulo.start();
    // create a table to ensure there are some entries in the !0 table
    AccumuloClient client = accumulo.createAccumuloClient("root", new PasswordToken("superSecret"));
    TableOperations tableops = client.tableOperations();
    tableops.create(TEST_TABLE);
    testTableID = tableops.tableIdMap().get(TEST_TABLE);

    Scanner s = client.createScanner(TEST_TABLE, Authorizations.EMPTY);
    Iterators.size(s.iterator());
  }

  @Test(timeout = 10000)
  public void testAccurateProcessListReturned() throws Exception {
    Map<ServerType,Collection<ProcessReference>> procs = accumulo.getProcesses();

    assertTrue(procs.containsKey(ServerType.GARBAGE_COLLECTOR));

    for (ServerType t : new ServerType[] {ServerType.MASTER, ServerType.TABLET_SERVER,
        ServerType.ZOOKEEPER}) {
      assertTrue(procs.containsKey(t));
      Collection<ProcessReference> procRefs = procs.get(t);
      assertTrue(1 <= procRefs.size());

      for (ProcessReference procRef : procRefs) {
        assertNotNull(procRef);
      }
    }
  }

  @Test(timeout = 60000)
  public void saneMonitorInfo() throws Exception {
    MasterMonitorInfo stats;
    while (true) {
      stats = accumulo.getMasterMonitorInfo();
      if (stats.tableMap.size() <= 2) {
        continue;
      }

      if (null != stats.tServerInfo && stats.tServerInfo.size() == NUM_TSERVERS) {
        break;
      }
    }
    List<MasterState> validStates = Arrays.asList(MasterState.values());
    List<MasterGoalState> validGoals = Arrays.asList(MasterGoalState.values());
    assertTrue("master state should be valid.", validStates.contains(stats.state));
    assertTrue("master goal state should be in " + validGoals + ". is " + stats.goalState,
        validGoals.contains(stats.goalState));
    assertNotNull("should have a table map.", stats.tableMap);
    assertTrue("root table should exist in " + stats.tableMap.keySet(),
        stats.tableMap.containsKey(RootTable.ID.canonical()));
    assertTrue("meta table should exist in " + stats.tableMap.keySet(),
        stats.tableMap.containsKey(MetadataTable.ID.canonical()));
    assertTrue("our test table should exist in " + stats.tableMap.keySet(),
        stats.tableMap.containsKey(testTableID));
    assertNotNull("there should be tservers.", stats.tServerInfo);
    assertEquals(NUM_TSERVERS, stats.tServerInfo.size());
  }

  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
  }

}
