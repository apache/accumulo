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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.PluginEnvironment.Configuration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.manager.balancer.AssignmentParamsImpl;
import org.apache.accumulo.core.manager.balancer.BalanceParamsImpl;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TabletBalancer that balances Tablets for a Table using the TabletBalancer defined by
 * {@link Property#TABLE_LOAD_BALANCER}. This allows for different Tables to specify different
 * TabletBalancer classes.
 * <p>
 * Note that in versions prior to 4.0 this class would pass all known TabletServers to the Table
 * load balancers. In version 4.0 this changed with the introduction of the
 * {@link TABLE_ASSIGNMENT_GROUP_PROPERTY} table property. If defined, this balancer passes the
 * TabletServers that have the corresponding {@link Property#TSERV_GROUP_NAME} property to the Table
 * load balancer.
 *
 * @since 2.1.0
 */
// ELASTICITY_TODO: Should we deprecate this balancer in 3.0 and rename it
// in 4.0 due to the behavior change?
public class TableLoadBalancer implements TabletBalancer {

  private static final Logger log = LoggerFactory.getLogger(TableLoadBalancer.class);

  public static final String TABLE_ASSIGNMENT_GROUP_PROPERTY = "table.custom.assignment.group";

  protected BalancerEnvironment environment;
  Map<TableId,TabletBalancer> perTableBalancers = new HashMap<>();

  @Override
  public void init(BalancerEnvironment balancerEnvironment) {
    this.environment = balancerEnvironment;
  }

  private TabletBalancer constructNewBalancerForTable(String clazzName, TableId tableId)
      throws Exception {
    String context = environment.tableContext(tableId);
    Class<? extends TabletBalancer> clazz =
        ClassLoaderUtil.loadClass(context, clazzName, TabletBalancer.class);
    Constructor<? extends TabletBalancer> constructor = clazz.getConstructor(TableId.class);
    return constructor.newInstance(tableId);
  }

  protected String getLoadBalancerClassNameForTable(TableId table) {
    if (environment.isTableOnline(table)) {
      return environment.getConfiguration(table).get(Property.TABLE_LOAD_BALANCER.getKey());
    }
    return null;
  }

  protected String getResourceGroupNameForTable(TableId tid) {
    String resourceGroup = environment.getConfiguration(tid).get(TABLE_ASSIGNMENT_GROUP_PROPERTY);
    if (!StringUtils.isEmpty(resourceGroup)) {
      return resourceGroup;
    }
    return Constants.DEFAULT_RESOURCE_GROUP_NAME;
  }

  protected TabletBalancer getBalancerForTable(TableId tableId) {
    TabletBalancer balancer = perTableBalancers.get(tableId);

    String clazzName = getLoadBalancerClassNameForTable(tableId);

    if (clazzName == null) {
      clazzName = SimpleLoadBalancer.class.getName();
    }
    if (balancer != null) {
      if (!clazzName.equals(balancer.getClass().getName())) {
        // the balancer class for this table does not match the class specified in the configuration
        try {
          balancer = constructNewBalancerForTable(clazzName, tableId);
          perTableBalancers.put(tableId, balancer);
          balancer.init(environment);

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
        log.info("Using balancer {} for table {}", SimpleLoadBalancer.class.getName(), tableId);
        balancer = new SimpleLoadBalancer(tableId);
      }
      perTableBalancers.put(tableId, balancer);
      balancer.init(environment);
    }
    return balancer;
  }

  private SortedMap<TabletServerId,TServerStatus> getCurrentSetForTable(
      SortedMap<TabletServerId,TServerStatus> allTServers,
      Map<String,Set<TabletServerId>> groupedTServers, String resourceGroup) {

    String groupName =
        resourceGroup == null ? Constants.DEFAULT_RESOURCE_GROUP_NAME : resourceGroup;
    Set<TabletServerId> tserversInGroup = groupedTServers.get(groupName);
    if (tserversInGroup == null || tserversInGroup.isEmpty()) {
      if (groupName.equals(Constants.DEFAULT_RESOURCE_GROUP_NAME)) {
        throw new IllegalStateException(
            "There are no TabletServers in the default resource group!");
      } else {
        log.warn("No TabletServers in assignment group {}, using Tablet Servers in default group",
            groupName);
        groupName = Constants.DEFAULT_RESOURCE_GROUP_NAME;
        tserversInGroup = groupedTServers.get(groupName);
        if (tserversInGroup == null || tserversInGroup.isEmpty()) {
          throw new IllegalStateException(
              "There are no TabletServers in the default resource group!");
        }
      }
    }
    SortedMap<TabletServerId,TServerStatus> group = new TreeMap<>();
    final String groupNameInUse = groupName;
    tserversInGroup.forEach(tsid -> {
      TServerStatus tss = allTServers.get(tsid);
      if (tss == null) {
        throw new IllegalStateException("TabletServer " + tsid + " in " + groupNameInUse
            + " TabletServer group, but not in set of all TabletServers");
      }
      group.put(tsid, tss);
    });
    return group;
  }

  @Override
  public void getAssignments(AssignmentParameters params) {
    // separate the unassigned into tables
    Map<TableId,Map<TabletId,TabletServerId>> groupedUnassigned = new HashMap<>();
    params.unassignedTablets().forEach((tid, lastTserver) -> groupedUnassigned
        .computeIfAbsent(tid.getTable(), k -> new HashMap<>()).put(tid, lastTserver));
    for (Entry<TableId,Map<TabletId,TabletServerId>> e : groupedUnassigned.entrySet()) {
      final String tableResourceGroup = getResourceGroupNameForTable(e.getKey());
      final Map<TabletId,TabletServerId> newAssignments = new HashMap<>();
      // get the group of tservers for this table
      final SortedMap<TabletServerId,TServerStatus> groupedTServers = getCurrentSetForTable(
          params.currentStatus(), params.currentResourceGroups(), tableResourceGroup);
      getBalancerForTable(e.getKey()).getAssignments(new AssignmentParamsImpl(groupedTServers,
          params.currentResourceGroups(), e.getValue(), newAssignments));

      newAssignments.forEach((tid, tsid) -> {
        if (!groupedTServers.containsKey(tsid)) {
          log.warn(
              "table balancer assigned {} to tablet server {} that is not in the assigned resource group {}",
              tid, tsid, tableResourceGroup);
        }
        params.addAssignment(tid, tsid);
      });
    }
  }

  @Override
  public long balance(BalanceParameters params) {
    long minBalanceTime = 5_000;
    // Iterate over the tables and balance each of them
    for (TableId tableId : environment.getTableIdMap().values()) {
      final String tableResourceGroup = getResourceGroupNameForTable(tableId);
      // get the group of tservers for this table
      SortedMap<TabletServerId,TServerStatus> groupedTServers = getCurrentSetForTable(
          params.currentStatus(), params.currentResourceGroups(), tableResourceGroup);
      ArrayList<TabletMigration> newMigrations = new ArrayList<>();
      long tableBalanceTime =
          getBalancerForTable(tableId).balance(new BalanceParamsImpl(groupedTServers,
              params.currentResourceGroups(), params.currentMigrations(), newMigrations));
      if (tableBalanceTime < minBalanceTime) {
        minBalanceTime = tableBalanceTime;
      }
      params.migrationsOut().addAll(newMigrations);
    }
    return minBalanceTime;
  }

  @Override
  public boolean isHostedInResourceGroup(Configuration conf, TabletServerId currentLocation,
      Map<String,Set<TabletServerId>> currentTServerGrouping) {
    Objects.requireNonNull(conf, "conf cannot be null");
    Objects.requireNonNull(currentLocation, "current location cannot be null");
    Objects.requireNonNull(currentTServerGrouping, "tserver grouping cannot be null");
    String property = Constants.DEFAULT_RESOURCE_GROUP_NAME;
    if (conf.get(TABLE_ASSIGNMENT_GROUP_PROPERTY) != null) {
      property = conf.get(TABLE_ASSIGNMENT_GROUP_PROPERTY);
    }
    if (property.equals(Constants.DEFAULT_RESOURCE_GROUP_NAME)) {
      return true;
    }
    Set<TabletServerId> tservers = currentTServerGrouping.get(property);
    if (tservers == null) {
      log.warn("No TabletServers for group {} ", property);
      // return true as there are no tablet servers in the defined group
      // We don't want to cause churn
      return true;
    }
    return tservers.contains(currentLocation);

  }

}
