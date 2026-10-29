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

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP2;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.MAX_DATA;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.getExpectedGroups;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.row;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.verify;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.compaction.ExternalCompactionTestUtils;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class MultipleManagerCompactionIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
    cfg.getClusterServerConfiguration().setNumManagers(3);
    // This test could kill a manager after its written a compaction to the metadata table, but
    // before it returns it to the compactor via RPC which creates a dead compaction. Need to speed
    // up the dead compaction detection to handle this or else the test will hang.
    cfg.setProperty(Property.COMPACTION_COORDINATOR_DEAD_COMPACTOR_CHECK_INTERVAL, "5s");
    // Speed up time to detect a dead manager process by lowering ZK timeout
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "10s");
  }

  @Test
  public void test() throws Exception {
    String[] names = this.getUniqueNames(2);
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      // wait for three coordinator locations to show up in zookeeper
      Wait.waitFor(
          () -> getServerContext().getCoordinatorLocations().sortedUniqueHost().size() == 3);
      assertEquals(getExpectedGroups(),
          getServerContext().getCoordinatorLocations().locations().keySet());

      String table1 = names[0];
      createTable(client, table1, "cs1");

      String table2 = names[1];
      createTable(client, table2, "cs2");

      writeData(client, table1);
      writeData(client, table2);

      compact(client, table1, 2, GROUP1, true);
      verify(client, table1, 2);

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(row(MAX_DATA / 2)));
      client.tableOperations().addSplits(table2, splits);

      compact(client, table2, 3, GROUP2, true);
      verify(client, table2, 3);

      getCluster().getConfig().getClusterServerConfiguration().setNumManagers(5);
      getCluster().getClusterControl().start(ServerType.MANAGER);

      // wait for five coordinator locations to show up in zookeeper
      Wait.waitFor(
          () -> getServerContext().getCoordinatorLocations().sortedUniqueHost().size() == 5);
      assertEquals(getExpectedGroups(),
          getServerContext().getCoordinatorLocations().locations().keySet());

      compact(client, table1, 3, GROUP1, true);
      verify(client, table1, 6);

      compact(client, table2, 5, GROUP2, true);
      verify(client, table2, 15);

      // kill three managers
      for (var proc : getCluster().getProcesses().get(ServerType.MANAGER).stream().limit(3)
          .toList()) {
        getCluster().getClusterControl().killProcess(ServerType.MANAGER, proc);
      }

      // wait for three coordinator locations to show up in zookeeper
      Wait.waitFor(
          () -> getServerContext().getCoordinatorLocations().sortedUniqueHost().size() == 2,
          120_000);
      assertEquals(getExpectedGroups(),
          getServerContext().getCoordinatorLocations().locations().keySet());

      compact(client, table1, 5, GROUP1, true);
      verify(client, table1, 30);

      compact(client, table2, 2, GROUP2, true);
      verify(client, table2, 30);

    }
  }

}
