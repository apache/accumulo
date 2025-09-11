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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class WaitForBalanceIT extends ConfigurableMacBase {

  private static final int NUM_SPLITS = 50;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      assertEquals(2, c.instanceOperations().getServers(Type.TABLET_SERVER).size());
      // ensure the metadata table is online
      try (Scanner scanner =
          c.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY)) {
        scanner.forEach((k, v) -> {});
      }
      c.instanceOperations().waitForBalance();
      assertTrue(isBalanced(c));
      final String tableName = getUniqueNames(1)[0];
      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.withInitialTabletAvailability(TabletAvailability.HOSTED);
      c.tableOperations().create(tableName, ntc);
      c.instanceOperations().waitForBalance();
      final SortedSet<Text> partitionKeys = new TreeSet<>();
      for (int i = 0; i < NUM_SPLITS; i++) {
        partitionKeys.add(new Text("" + i));
      }
      c.tableOperations().addSplits(tableName, partitionKeys);
      c.instanceOperations().waitForBalance();
      assertTrue(isBalanced(c));

      // Add another tserver to force a rebalance
      getCluster().getConfig().getClusterServerConfiguration().setNumDefaultTabletServers(3);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);
      Wait.waitFor(() -> c.instanceOperations().getServers(Type.TABLET_SERVER).size() == 3);
      c.instanceOperations().waitForBalance();
      assertTrue(isBalanced(c));
    }
  }

  private boolean isBalanced(AccumuloClient c) throws Exception {
    final Map<ServerId,Integer> tserverCounts = new HashMap<>();
    c.instanceOperations().getServers(Type.TABLET_SERVER).forEach(ts -> tserverCounts.put(ts, 0));
    int offline = 0;
    for (String tableName : new String[] {SystemTables.METADATA.tableName(),
        SystemTables.ROOT.tableName()}) {
      try (Scanner s = c.createScanner(tableName, Authorizations.EMPTY)) {
        s.setRange(TabletsSection.getRange());
        s.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
        TabletColumnFamily.PREV_ROW_COLUMN.fetch(s);
        TServerInstance location = null;
        for (Entry<Key,Value> entry : s) {
          Key key = entry.getKey();
          if (key.getColumnFamily().equals(CurrentLocationColumnFamily.NAME)) {
            location = TServerInstance.deserialize(entry.getValue().toString());
          } else if (TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)) {
            if (location == null) {
              offline++;
            } else {
              Integer count = tserverCounts.get(location.getServer());
              if (count == null) {
                count = 0;
              }
              count = count + 1;
              tserverCounts.put(location.getServer(), count);
            }
            location = null;
          }
        }
      }
    }
    if (offline > 0) {
      System.out.println("Offline tablets " + offline);
      return false;
    }
    int average = 0;
    for (Integer i : tserverCounts.values()) {
      average += i;
    }
    average /= tserverCounts.size();
    System.out.println(tserverCounts);
    int tablesCount = c.tableOperations().list().size();
    for (Entry<ServerId,Integer> hostCount : tserverCounts.entrySet()) {
      if (Math.abs(average - hostCount.getValue()) > tablesCount) {
        System.out.println(
            "Average " + average + " count " + hostCount.getKey() + ": " + hostCount.getValue());
        return false;
      }
    }
    return true;
  }

}
