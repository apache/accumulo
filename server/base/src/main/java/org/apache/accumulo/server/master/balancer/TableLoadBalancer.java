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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableLoadBalancer extends TabletBalancer {

  private static final Logger log = LoggerFactory.getLogger(TableLoadBalancer.class);

  Map<Table.ID,TabletBalancer> perTableBalancers = new HashMap<>();

  private TabletBalancer constructNewBalancerForTable(String clazzName, Table.ID tableId) throws Exception {
    String context = null;
    context = this.context.getServerConfigurationFactory().getTableConfiguration(tableId).get(Property.TABLE_CLASSPATH);
    Class<? extends TabletBalancer> clazz;
    if (context != null && !context.equals(""))
      clazz = AccumuloVFSClassLoader.getContextManager().loadClass(context, clazzName, TabletBalancer.class);
    else
      clazz = AccumuloVFSClassLoader.loadClass(clazzName, TabletBalancer.class);
    Constructor<? extends TabletBalancer> constructor = clazz.getConstructor(Table.ID.class);
    return constructor.newInstance(tableId);
  }

  protected String getLoadBalancerClassNameForTable(Table.ID table) {
    TableState tableState = TableManager.getInstance().getTableState(table);
    if (tableState == null)
      return null;
    if (tableState.equals(TableState.ONLINE))
      return this.context.getServerConfigurationFactory().getTableConfiguration(table).get(Property.TABLE_LOAD_BALANCER);
    return null;
  }

  protected TabletBalancer getBalancerForTable(Table.ID tableId) {
    TabletBalancer balancer = perTableBalancers.get(tableId);

    String clazzName = getLoadBalancerClassNameForTable(tableId);

    if (clazzName == null)
      clazzName = DefaultLoadBalancer.class.getName();
    if (balancer != null) {
      if (clazzName.equals(balancer.getClass().getName()) == false) {
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
  public void getAssignments(SortedMap<TServerInstance,TabletServerStatus> current, Map<KeyExtent,TServerInstance> unassigned,
      Map<KeyExtent,TServerInstance> assignments) {
    // separate the unassigned into tables
    Map<Table.ID,Map<KeyExtent,TServerInstance>> groupedUnassigned = new HashMap<>();
    for (Entry<KeyExtent,TServerInstance> e : unassigned.entrySet()) {
      Map<KeyExtent,TServerInstance> tableUnassigned = groupedUnassigned.get(e.getKey().getTableId());
      if (tableUnassigned == null) {
        tableUnassigned = new HashMap<>();
        groupedUnassigned.put(e.getKey().getTableId(), tableUnassigned);
      }
      tableUnassigned.put(e.getKey(), e.getValue());
    }
    for (Entry<Table.ID,Map<KeyExtent,TServerInstance>> e : groupedUnassigned.entrySet()) {
      Map<KeyExtent,TServerInstance> newAssignments = new HashMap<>();
      getBalancerForTable(e.getKey()).getAssignments(current, e.getValue(), newAssignments);
      assignments.putAll(newAssignments);
    }
  }

  private TableOperations tops = null;

  protected TableOperations getTableOperations() {
    if (tops == null)
      try {
        tops = this.context.getConnector().tableOperations();
      } catch (AccumuloException e) {
        log.error("Unable to access table operations from within table balancer", e);
      } catch (AccumuloSecurityException e) {
        log.error("Unable to access table operations from within table balancer", e);
      }
    return tops;
  }

  @Override
  public long balance(SortedMap<TServerInstance,TabletServerStatus> current, Set<KeyExtent> migrations, List<TabletMigration> migrationsOut) {
    long minBalanceTime = 5 * 1000;
    // Iterate over the tables and balance each of them
    TableOperations t = getTableOperations();
    if (t == null)
      return minBalanceTime;
    for (String s : t.tableIdMap().values()) {
      ArrayList<TabletMigration> newMigrations = new ArrayList<>();
      long tableBalanceTime = getBalancerForTable(Table.ID.of(s)).balance(current, migrations, newMigrations);
      if (tableBalanceTime < minBalanceTime)
        minBalanceTime = tableBalanceTime;
      migrationsOut.addAll(newMigrations);
    }
    return minBalanceTime;
  }
}
