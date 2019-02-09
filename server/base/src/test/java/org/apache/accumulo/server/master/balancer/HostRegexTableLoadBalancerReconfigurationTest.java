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
package org.apache.accumulo.server.master.balancer;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.junit.Test;

public class HostRegexTableLoadBalancerReconfigurationTest
    extends BaseHostRegexTableLoadBalancerTest {

  private Map<KeyExtent,TServerInstance> assignments = new HashMap<>();

  @Test
  public void testConfigurationChanges() {
    ServerContext context1 = createMockContext();
    replay(context1);
    final TestServerConfigurationFactory factory = new TestServerConfigurationFactory(context1);
    ServerContext context2 = createMockContext();
    expect(context2.getServerConfFactory()).andReturn(factory).anyTimes();
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
        fail("tablet not in bounds: " + e.getKey() + " -> " + e.getValue().host());
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
    DEFAULT_TABLE_PROPERTIES
        .put(HostRegexTableLoadBalancer.HOST_BALANCER_PREFIX + BAR.getTableName(), "r01.*");
    this.propertiesChanged();
    // Wait to trigger the out of bounds check and the repool check
    UtilWaitThread.sleep(10000);
    this.balance(Collections.unmodifiableSortedMap(allTabletServers), migrations, migrationsOut);
    assertEquals(5, migrationsOut.size());
    for (TabletMigration migration : migrationsOut) {
      assertTrue(migration.newServer.host().startsWith("192.168.0.1")
          || migration.newServer.host().startsWith("192.168.0.2")
          || migration.newServer.host().startsWith("192.168.0.3")
          || migration.newServer.host().startsWith("192.168.0.4")
          || migration.newServer.host().startsWith("192.168.0.5"));
    }
  }

  @Override
  public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, TableId tableId) {
    List<TabletStats> tablets = new ArrayList<>();
    // Report assignment information
    for (Entry<KeyExtent,TServerInstance> e : this.assignments.entrySet()) {
      if (e.getValue().equals(tserver) && e.getKey().getTableId().equals(tableId)) {
        TabletStats ts = new TabletStats();
        ts.setExtent(e.getKey().toThrift());
        tablets.add(ts);
      }
    }
    return tablets;
  }
}
