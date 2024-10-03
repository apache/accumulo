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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.balancer.TableLoadBalancer;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class WaitForBalanceIT extends ConfigurableMacBase {

  private static final int NUM_SPLITS = 50;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "5s");
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      // ensure the metadata table is online
      try (Scanner scanner =
          c.createScanner(AccumuloTable.METADATA.tableName(), Authorizations.EMPTY)) {
        scanner.forEach((k, v) -> {});
      }
      c.instanceOperations().waitForBalance();

      final List<String> defaultTabletServers =
          new ArrayList<>(c.instanceOperations().getTabletServers());

      final String tableName = getUniqueNames(1)[0];
      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.withInitialTabletAvailability(TabletAvailability.HOSTED);
      final SortedSet<Text> partitionKeys = new TreeSet<>();
      for (int i = 0; i < NUM_SPLITS; i++) {
        partitionKeys.add(new Text("" + i));
      }
      ntc.withSplits(partitionKeys);
      c.tableOperations().create(tableName, ntc);
      c.instanceOperations().waitForBalance();
      Wait.waitFor(() -> isTableBalanced(c, tableName, defaultTabletServers));

      getCluster().getConfig().getClusterServerConfiguration().addTabletServerResourceGroup("TEST",
          2);
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);
      c.tableOperations().setProperty(tableName, TableLoadBalancer.TABLE_ASSIGNMENT_GROUP_PROPERTY,
          "TEST");
      Wait.waitFor(() -> c.instanceOperations().getTabletServers().size() == 4);
      final List<String> testTabletServers =
          new ArrayList<>(c.instanceOperations().getTabletServers());
      assertTrue(testTabletServers.removeAll(defaultTabletServers));
      // Wait for the TabletManagementIterator to pick up tablet changes
      Thread.sleep(7000);
      c.instanceOperations().waitForBalance();
      Wait.waitFor(() -> isTableBalanced(c, tableName, testTabletServers));

      c.tableOperations().removeProperty(tableName,
          TableLoadBalancer.TABLE_ASSIGNMENT_GROUP_PROPERTY);
      getCluster().getClusterControl().stopTabletServerGroup("TEST");
      Wait.waitFor(() -> c.instanceOperations().getTabletServers().size() == 2, 60_000);
      // Wait for the TabletManagementIterator to pick up tablet changes
      Thread.sleep(7000);
      c.instanceOperations().waitForBalance();
      Wait.waitFor(() -> isTableBalanced(c, tableName, defaultTabletServers));
    }
  }

  private boolean isTableBalanced(AccumuloClient c, String tableName,
      List<String> expectedTabletServers) throws Exception {
    final Map<String,Integer> counts = new HashMap<>();
    TableId tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));
    int offline = 0;
    try (Scanner s = c.createScanner(AccumuloTable.METADATA.tableName(), Authorizations.EMPTY)) {
      s.setRange(TabletsSection.getRange(tid));
      s.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
      TabletColumnFamily.PREV_ROW_COLUMN.fetch(s);
      String location = null;
      for (Entry<Key,Value> entry : s) {
        Key key = entry.getKey();
        if (key.getColumnFamily().equals(CurrentLocationColumnFamily.NAME)) {
          location = entry.getValue().toString();
        } else if (TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)) {
          if (location == null) {
            offline++;
          } else {
            Integer count = counts.get(location);
            if (count == null) {
              count = 0;
            }
            count = count + 1;
            counts.put(location, count);
          }
          location = null;
        }
      }
    }
    if (offline > 0) {
      System.out.println("Offline tablets " + offline);
      return false;
    }
    int average = 0;
    for (Integer i : counts.values()) {
      average += i;
    }
    average /= counts.size();
    System.out.println(counts);

    List<String> actualTabletServers = new ArrayList<>(counts.keySet());
    Collections.sort(actualTabletServers);
    Collections.sort(expectedTabletServers);
    if (!actualTabletServers.equals(expectedTabletServers)) {
      System.out.println("Expected tablets to be assigned to " + expectedTabletServers
          + ", assigned to " + actualTabletServers);
      return false;
    }

    int tablesCount = c.tableOperations().list().size();
    for (Entry<String,Integer> hostCount : counts.entrySet()) {
      if (Math.abs(average - hostCount.getValue()) > tablesCount) {
        System.out.println(
            "Average " + average + " count " + hostCount.getKey() + ": " + hostCount.getValue());
        return false;
      }
    }
    return true;
  }

}
