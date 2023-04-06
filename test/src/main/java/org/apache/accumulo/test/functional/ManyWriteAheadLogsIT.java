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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.log.WalStateManager.WalState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManyWriteAheadLogsIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(ManyWriteAheadLogsIT.class);

  private String majcDelay, walSize;

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // configure a smaller wal size so the wals will roll frequently in the test
    cfg.setProperty(Property.TSERV_WAL_MAX_SIZE, "1M");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1");
    cfg.setProperty(Property.GC_CYCLE_START, "1");
    cfg.setProperty(Property.MANAGER_RECOVERY_DELAY, "1s");
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "1");
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    // idle compactions may addess the problem this test is creating, however they will not prevent
    // lots of closed WALs for all write patterns. This test ensures code that directly handles many
    // tablets referencing many different WALs is working.
    cfg.setProperty(Property.TABLE_MINC_COMPACT_IDLETIME, "1h");
    cfg.setNumTservers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @BeforeEach
  public void alterConfig() throws Exception {
    if (getClusterType() == ClusterType.MINI) {
      return;
    }
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      InstanceOperations iops = client.instanceOperations();
      Map<String,String> conf = iops.getSystemConfiguration();
      majcDelay = conf.get(Property.TSERV_MAJC_DELAY.getKey());
      walSize = conf.get(Property.TSERV_WAL_MAX_SIZE.getKey());
      iops.setProperty(Property.TSERV_MAJC_DELAY.getKey(), "1");
      iops.setProperty(Property.TSERV_WAL_MAX_SIZE.getKey(), "1M");

      getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getClusterControl().startAllServers(ServerType.TABLET_SERVER);
    }
  }

  @AfterEach
  public void resetConfig() throws Exception {
    if (majcDelay != null) {
      try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
        InstanceOperations iops = client.instanceOperations();
        iops.setProperty(Property.TSERV_MAJC_DELAY.getKey(), majcDelay);
        iops.setProperty(Property.TSERV_WAL_MAX_SIZE.getKey(), walSize);
      }
      getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getClusterControl().startAllServers(ServerType.TABLET_SERVER);
    }
  }

  /**
   * This creates a situation where many tablets reference many different write ahead logs. However
   * not single tablet references a lot of write ahead logs. Want to ensure the tablet server forces
   * minor compactions for this situation.
   */
  @Test
  public void testMany() throws Exception {
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 1; i < 100; i++) {
      splits.add(new Text(String.format("%05x", i * 100)));
    }

    ServerContext context = getServerContext();

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String[] tableNames = getUniqueNames(2);

      String manyWALsTable = tableNames[0];
      String rollWALsTable = tableNames[1];

      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
      c.tableOperations().create(manyWALsTable, ntc);

      c.tableOperations().create(rollWALsTable);

      Set<String> allWalsSeen = new HashSet<>();

      addOpenWals(context, allWalsSeen);

      // This test creates the table manyWALsTable with a lot of tablets and writes a little bit to
      // each tablet. In between writing a little bit to each tablet a lot of data is written to
      // another table called rollWALsTable. Writing a lot causes the write ahead logs to roll. This
      // write pattern should cause the tablets in the manyWALsTable table to reference many closed
      // WALs. If nothing is done about all of these closed WALs, then it could cause a large burden
      // at recovery time.

      try (BatchWriter manyWALsWriter = c.createBatchWriter(manyWALsTable);
          BatchWriter rollWALsWriter = c.createBatchWriter(rollWALsTable)) {

        byte[] val = new byte[768];

        for (int i = 0; i < 100; i++) {
          int startRow = i * 100;

          // write a small amount of data to each tablet in the table
          for (int j = 0; j < 10; j++) {
            int row = startRow + j;
            Mutation m = new Mutation(String.format("%05x", row));
            random.nextBytes(val);
            m.put("f", "q", "v");

            manyWALsWriter.addMutation(m);
          }
          manyWALsWriter.flush();

          // write a lot of data to second table to forces the logs to roll
          for (int j = 0; j < 1000; j++) {
            Mutation m = new Mutation(String.format("%03d", j));
            random.nextBytes(val);

            m.put("f", "q", Base64.getEncoder().encodeToString(val));

            rollWALsWriter.addMutation(m);
          }

          rollWALsWriter.flush();

          // keep track of the open WALs as the test runs. Should see a lot of open WALs over the
          // lifetime of the test, but never a lot at any one time.
          addOpenWals(context, allWalsSeen);
        }
      }

      assertTrue(allWalsSeen.size() >= 50,
          "Number of WALs seen was less than expected " + allWalsSeen.size());

      // the total number of closed write ahead logs should get small
      int closedLogs = countClosedWals(context);
      while (closedLogs > 3) {
        log.debug("Waiting for wals to shrink " + closedLogs);
        Thread.sleep(250);
        closedLogs = countClosedWals(context);
      }
    }
  }

  private void addOpenWals(ServerContext c, Set<String> allWalsSeen) throws Exception {

    int open = 0;
    int attempts = 0;
    boolean foundWal = false;

    while (open == 0) {
      attempts++;
      Map<String,WalState> wals = WALSunnyDayIT._getWals(c);
      Set<Entry<String,WalState>> es = wals.entrySet();
      for (Entry<String,WalState> entry : es) {
        if (entry.getValue() == WalState.OPEN) {
          open++;
          allWalsSeen.add(entry.getKey());
          foundWal = true;
        } else {
          // log CLOSED or UNREFERENCED to help debug this test
          log.debug("The WalState for {} is {}", entry.getKey(), entry.getValue());
        }
      }

      if (!foundWal) {
        Thread.sleep(50);
        if (attempts % 50 == 0) {
          log.debug("No open WALs found in {} attempts.", attempts);
        }
      }
    }

    log.debug("It took {} attempt(s) to find {} open WALs", attempts, open);
    assertTrue(open > 0 && open < 4, "Open WALs not in expected range " + open);
  }

  private int countClosedWals(ServerContext c) throws Exception {
    int count = 0;
    Map<String,WalState> wals = WALSunnyDayIT._getWals(c);
    for (WalState ws : wals.values()) {
      if (ws == WalState.CLOSED) {
        count++;
      }
    }

    return count;
  }
}
