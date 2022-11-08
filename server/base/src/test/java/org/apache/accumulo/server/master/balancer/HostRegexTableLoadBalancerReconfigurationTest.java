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
package org.apache.accumulo.server.master.balancer;

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
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.junit.jupiter.api.Test;

@Deprecated(since = "2.1.0")
public class HostRegexTableLoadBalancerReconfigurationTest
    extends BaseHostRegexTableLoadBalancerTest {

  private Map<KeyExtent,TServerInstance> assignments = new HashMap<>();

  @Test
  public void testConfigurationChanges() {
    ServerContext context1 = createMockContext();
    replay(context1);
    final TestServerConfigurationFactory factory = new TestServerConfigurationFactory(context1);
    ServerContext context2 = createMockContext();
    expect(context2.getConfiguration()).andReturn(factory.getSystemConfiguration()).anyTimes();
    expect(context2.getTableConfiguration(FOO.getId()))
        .andReturn(factory.getTableConfiguration(FOO.getId())).anyTimes();
    expect(context2.getTableConfiguration(BAR.getId()))
        .andReturn(factory.getTableConfiguration(BAR.getId())).anyTimes();
    expect(context2.getTableConfiguration(BAZ.getId()))
        .andReturn(factory.getTableConfiguration(BAZ.getId())).anyTimes();
    replay(context2);
    init(context2);
    Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
    for (List<KeyExtent> extents : tableExtents.values()) {
      for (KeyExtent ke : extents) {
        unassigned.put(ke, null);
      }
    }
    this.getAssignments(Collections.unmodifiableSortedMap(allTabletServers),
        Collections.unmodifiableMap(unassigned), assignments);
    assertEquals(15, assignments.size());
    // Ensure unique tservers
    for (Entry<KeyExtent,TServerInstance> e : assignments.entrySet()) {
      for (Entry<KeyExtent,TServerInstance> e2 : assignments.entrySet()) {
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
    for (Entry<KeyExtent,TServerInstance> e : assignments.entrySet()) {
      if (!tabletInBounds(e.getKey(), e.getValue())) {
        fail("tablet not in bounds: " + e.getKey() + " -> " + e.getValue().getHost());
      }
    }
    Set<KeyExtent> migrations = new HashSet<>();
    List<TabletMigration> migrationsOut = new ArrayList<>();
    // Wait to trigger the out of bounds check which will call our version of
    // getOnlineTabletsForTable
    UtilWaitThread.sleep(3000);
    this.balance(Collections.unmodifiableSortedMap(allTabletServers), migrations, migrationsOut);
    assertEquals(0, migrationsOut.size());
    // Change property, simulate call by TableConfWatcher

    ((ConfigurationCopy) factory.getSystemConfiguration())
        .set(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(), "r01.*");

    // Wait to trigger the out of bounds check and the repool check
    UtilWaitThread.sleep(10000);
    this.balance(Collections.unmodifiableSortedMap(allTabletServers), migrations, migrationsOut);
    assertEquals(5, migrationsOut.size());
    for (TabletMigration migration : migrationsOut) {
      assertTrue(migration.newServer.getHost().startsWith("192.168.0.1")
          || migration.newServer.getHost().startsWith("192.168.0.2")
          || migration.newServer.getHost().startsWith("192.168.0.3")
          || migration.newServer.getHost().startsWith("192.168.0.4")
          || migration.newServer.getHost().startsWith("192.168.0.5"));
    }
  }

  @Override
  public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, TableId tableId) {
    List<TabletStats> tablets = new ArrayList<>();
    // Report assignment information
    for (Entry<KeyExtent,TServerInstance> e : this.assignments.entrySet()) {
      if (e.getValue().equals(tserver) && e.getKey().tableId().equals(tableId)) {
        TabletStats ts = new TabletStats();
        ts.setExtent(e.getKey().toThrift());
        tablets.add(ts);
      }
    }
    return tablets;
  }
}
