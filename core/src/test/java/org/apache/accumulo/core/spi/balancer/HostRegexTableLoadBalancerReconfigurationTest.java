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
package org.apache.accumulo.core.spi.balancer;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.balancer.AssignmentParamsImpl;
import org.apache.accumulo.core.manager.balancer.BalanceParamsImpl;
import org.apache.accumulo.core.manager.balancer.TabletStatisticsImpl;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.spi.balancer.data.TabletStatistics;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.junit.jupiter.api.Test;

public class HostRegexTableLoadBalancerReconfigurationTest
    extends BaseHostRegexTableLoadBalancerTest {

  private final Map<TabletId,TabletServerId> assignments = new HashMap<>();

  @Test
  public void testConfigurationChanges() {
    HashMap<String,TableId> tables = new HashMap<>();
    tables.put(FOO.getTableName(), FOO.getId());
    tables.put(BAR.getTableName(), BAR.getId());
    tables.put(BAZ.getTableName(), BAZ.getId());

    ConfigurationCopy config = new ConfigurationCopy(SiteConfiguration.empty().build());
    DEFAULT_TABLE_PROPERTIES.forEach(config::set);
    ConfigurationImpl configImpl = new ConfigurationImpl(config);
    BalancerEnvironment environment = createMock(BalancerEnvironment.class);
    expect(environment.getConfiguration()).andReturn(configImpl).anyTimes();
    expect(environment.getTableIdMap()).andReturn(tables).anyTimes();
    expect(environment.getConfiguration(anyObject(TableId.class))).andReturn(configImpl).anyTimes();
    replay(environment);
    init(environment);

    Map<TabletId,TabletServerId> unassigned = new HashMap<>();
    for (List<TabletId> tablets : tableTablets.values()) {
      for (TabletId tablet : tablets) {
        unassigned.put(tablet, null);
      }
    }
    this.getAssignments(
        new AssignmentParamsImpl(Collections.unmodifiableSortedMap(allTabletServers),
            Collections.unmodifiableMap(unassigned), assignments));
    assertEquals(15, assignments.size());
    // Ensure unique tservers
    for (Entry<TabletId,TabletServerId> e : assignments.entrySet()) {
      for (Entry<TabletId,TabletServerId> e2 : assignments.entrySet()) {
        if (e.getKey().equals(e2.getKey())) {
          continue;
        }
        if (e.getValue().equals(e2.getValue())) {
          fail("Assignment failure. " + e.getKey() + " and " + e2.getKey()
              + " are assigned to the same host: " + e.getValue());
        }
      }
    }
    // Ensure assignments are correct
    for (Entry<TabletId,TabletServerId> e : assignments.entrySet()) {
      if (!tabletInBounds(e.getKey(), e.getValue())) {
        fail("tablet not in bounds: " + e.getKey() + " -> " + e.getValue().getHost());
      }
    }
    Set<TabletId> migrations = new HashSet<>();
    List<TabletMigration> migrationsOut = new ArrayList<>();
    // Wait to trigger the out of bounds check which will call our version of
    // getOnlineTabletsForTable
    UtilWaitThread.sleep(3000);
    this.balance(new BalanceParamsImpl(Collections.unmodifiableSortedMap(allTabletServers),
        migrations, migrationsOut));
    assertEquals(0, migrationsOut.size());
    // Change property, simulate call by TableConfWatcher

    config.set(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(), "r01.*");

    // Wait to trigger the out of bounds check and the repool check
    UtilWaitThread.sleep(10000);
    this.balance(new BalanceParamsImpl(Collections.unmodifiableSortedMap(allTabletServers),
        migrations, migrationsOut));
    assertEquals(5, migrationsOut.size());
    for (TabletMigration migration : migrationsOut) {
      assertTrue(migration.getNewTabletServer().getHost().startsWith("192.168.0.1")
          || migration.getNewTabletServer().getHost().startsWith("192.168.0.2")
          || migration.getNewTabletServer().getHost().startsWith("192.168.0.3")
          || migration.getNewTabletServer().getHost().startsWith("192.168.0.4")
          || migration.getNewTabletServer().getHost().startsWith("192.168.0.5"));
    }
  }

  @Override
  public List<TabletStatistics> getOnlineTabletsForTable(TabletServerId tserver, TableId tableId) {
    List<TabletStatistics> tablets = new ArrayList<>();
    // Report assignment information
    for (Entry<TabletId,TabletServerId> e : this.assignments.entrySet()) {
      if (e.getValue().equals(tserver) && e.getKey().getTable().equals(tableId)) {
        TabletStats ts = new TabletStats();
        TabletId tid = e.getKey();
        ts.setExtent(
            new KeyExtent(tid.getTable(), tid.getEndRow(), tid.getPrevEndRow()).toThrift());
        tablets.add(new TabletStatisticsImpl(ts));
      }
    }
    return tablets;
  }
}
