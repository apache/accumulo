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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated since 2.1.0. Use {@link org.apache.accumulo.core.spi.balancer.TableLoadBalancer}
 *             instead.
 */
@Deprecated(since = "2.1.0")
public class TableLoadBalancer extends TabletBalancer {

  private static final Logger log = LoggerFactory.getLogger(TableLoadBalancer.class);

  Map<TableId,TabletBalancer> perTableBalancers = new HashMap<>();

  public TableLoadBalancer() {
    log.warn(
        "{} has been deprecated and will be removed in a future release. Please update your "
            + "configuration to use the equivalent {} instead.",
        getClass().getName(),
        org.apache.accumulo.core.spi.balancer.TableLoadBalancer.class.getName());
  }

  private TabletBalancer constructNewBalancerForTable(String clazzName, TableId tableId)
      throws Exception {
    String context = null;
    context = ClassLoaderUtil.tableContext(this.context.getTableConfiguration(tableId));
    Class<? extends TabletBalancer> clazz =
        ClassLoaderUtil.loadClass(context, clazzName, TabletBalancer.class);
    Constructor<? extends TabletBalancer> constructor = clazz.getConstructor(TableId.class);
    return constructor.newInstance(tableId);
  }

  protected String getLoadBalancerClassNameForTable(TableId table) {
    TableState tableState = context.getTableManager().getTableState(table);
    if (tableState == TableState.ONLINE) {
      return this.context.getTableConfiguration(table).get(Property.TABLE_LOAD_BALANCER);
    }
    return null;
  }

  protected TabletBalancer getBalancerForTable(TableId tableId) {
    TabletBalancer balancer = perTableBalancers.get(tableId);

    String clazzName = getLoadBalancerClassNameForTable(tableId);

    if (clazzName == null) {
      clazzName = DefaultLoadBalancer.class.getName();
    }
    if (balancer != null) {
      if (!clazzName.equals(balancer.getClass().getName())) {
        // the balancer class for this table does not match the class specified in the configuration
        try {
          // attempt to construct a balancer with the specified class
          TabletBalancer newBalancer = constructNewBalancerForTable(clazzName, tableId);
          if (newBalancer != null) {
            balancer = newBalancer;
            perTableBalancers.put(tableId, balancer);
            balancer.init(this.context);
          }

          log.info("Loaded new class {} for table {}", clazzName, tableId);
        } catch (Exception e) {
          log.warn("Failed to load table balancer class {} for table {}", clazzName, tableId, e);
        }
      }
    }
    if (balancer == null) {
      try {
        balancer = constructNewBalancerForTable(clazzName, tableId);
        log.info("Loaded class {} for table {}", clazzName, tableId);
      } catch (Exception e) {
        log.warn("Failed to load table balancer class {} for table {}", clazzName, tableId, e);
      }

      if (balancer == null) {
        log.info("Using balancer {} for table {}", DefaultLoadBalancer.class.getName(), tableId);
        balancer = new DefaultLoadBalancer(tableId);
      }
      perTableBalancers.put(tableId, balancer);
      balancer.init(this.context);
    }
    return balancer;
  }

  @Override
  public void getAssignments(SortedMap<TServerInstance,TabletServerStatus> current,
      Map<KeyExtent,TServerInstance> unassigned, Map<KeyExtent,TServerInstance> assignments) {
    // separate the unassigned into tables
    Map<TableId,Map<KeyExtent,TServerInstance>> groupedUnassigned = new HashMap<>();
    unassigned.forEach((ke, lastTserver) -> groupedUnassigned
        .computeIfAbsent(ke.tableId(), k -> new HashMap<>()).put(ke, lastTserver));
    for (Entry<TableId,Map<KeyExtent,TServerInstance>> e : groupedUnassigned.entrySet()) {
      Map<KeyExtent,TServerInstance> newAssignments = new HashMap<>();
      getBalancerForTable(e.getKey()).getAssignments(current, e.getValue(), newAssignments);
      assignments.putAll(newAssignments);
    }
  }

  private TableOperations tops = null;

  protected TableOperations getTableOperations() {
    if (tops == null) {
      tops = this.context.tableOperations();
    }
    return tops;
  }

  @Override
  public long balance(SortedMap<TServerInstance,TabletServerStatus> current,
      Set<KeyExtent> migrations, List<TabletMigration> migrationsOut) {
    long minBalanceTime = 5_000;
    // Iterate over the tables and balance each of them
    TableOperations t = getTableOperations();
    if (t == null) {
      return minBalanceTime;
    }
    for (String s : t.tableIdMap().values()) {
      ArrayList<TabletMigration> newMigrations = new ArrayList<>();
      long tableBalanceTime =
          getBalancerForTable(TableId.of(s)).balance(current, migrations, newMigrations);
      if (tableBalanceTime < minBalanceTime) {
        minBalanceTime = tableBalanceTime;
      }
      migrationsOut.addAll(newMigrations);
    }
    return minBalanceTime;
  }
}
