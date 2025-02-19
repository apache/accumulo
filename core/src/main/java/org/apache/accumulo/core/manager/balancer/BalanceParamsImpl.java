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
package org.apache.accumulo.core.manager.balancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;

public class BalanceParamsImpl implements TabletBalancer.BalanceParameters {
  private final SortedMap<TabletServerId,TServerStatus> currentStatus;
  private final Set<TabletId> currentMigrations;
  private final List<TabletMigration> migrationsOut;
  private final SortedMap<TServerInstance,TabletServerStatus> thriftCurrentStatus;
  private final Set<KeyExtent> thriftCurrentMigrations;
  private final Map<String,Set<TabletServerId>> tserverResourceGroups;
  private final DataLevel currentDataLevel;

  public static BalanceParamsImpl fromThrift(SortedMap<TabletServerId,TServerStatus> currentStatus,
      Map<String,Set<TServerInstance>> currentTServerGrouping,
      SortedMap<TServerInstance,TabletServerStatus> thriftCurrentStatus,
      Set<KeyExtent> thriftCurrentMigrations, DataLevel currentLevel) {
    Set<TabletId> currentMigrations = thriftCurrentMigrations.stream().map(TabletIdImpl::new)
        .collect(Collectors.toUnmodifiableSet());

    Map<String,Set<TabletServerId>> tserverGroups = new HashMap<>();
    currentTServerGrouping.forEach((k, v) -> {
      Set<TabletServerId> servers = new HashSet<>();
      v.forEach(tsi -> servers.add(TabletServerIdImpl.fromThrift(tsi)));
      tserverGroups.put(k, servers);
    });

    return new BalanceParamsImpl(currentStatus, tserverGroups, currentMigrations, new ArrayList<>(),
        thriftCurrentStatus, thriftCurrentMigrations, currentLevel);
  }

  public BalanceParamsImpl(SortedMap<TabletServerId,TServerStatus> currentStatus,
      Map<String,Set<TabletServerId>> currentGroups, Set<TabletId> currentMigrations,
      List<TabletMigration> migrationsOut, DataLevel currentLevel) {
    this.currentStatus = currentStatus;
    this.tserverResourceGroups = currentGroups;
    this.currentMigrations = currentMigrations;
    this.migrationsOut = migrationsOut;
    this.thriftCurrentStatus = null;
    this.thriftCurrentMigrations = null;
    this.currentDataLevel = currentLevel;
  }

  private BalanceParamsImpl(SortedMap<TabletServerId,TServerStatus> currentStatus,
      Map<String,Set<TabletServerId>> currentGroups, Set<TabletId> currentMigrations,
      List<TabletMigration> migrationsOut,
      SortedMap<TServerInstance,TabletServerStatus> thriftCurrentStatus,
      Set<KeyExtent> thriftCurrentMigrations, DataLevel currentLevel) {
    this.currentStatus = currentStatus;
    this.tserverResourceGroups = currentGroups;
    this.currentMigrations = currentMigrations;
    this.migrationsOut = migrationsOut;
    this.thriftCurrentStatus = thriftCurrentStatus;
    this.thriftCurrentMigrations = thriftCurrentMigrations;
    this.currentDataLevel = currentLevel;
  }

  @Override
  public SortedMap<TabletServerId,TServerStatus> currentStatus() {
    return currentStatus;
  }

  @Override
  public Set<TabletId> currentMigrations() {
    return currentMigrations;
  }

  @Override
  public List<TabletMigration> migrationsOut() {
    return migrationsOut;
  }

  public SortedMap<TServerInstance,TabletServerStatus> thriftCurrentStatus() {
    return thriftCurrentStatus;
  }

  public Set<KeyExtent> thriftCurrentMigrations() {
    return thriftCurrentMigrations;
  }

  public void addMigration(KeyExtent extent, TServerInstance oldServer, TServerInstance newServer) {
    TabletId id = new TabletIdImpl(extent);
    TabletServerId oldTsid = new TabletServerIdImpl(oldServer);
    TabletServerId newTsid = new TabletServerIdImpl(newServer);
    migrationsOut.add(new TabletMigration(id, oldTsid, newTsid));
  }

  @Override
  public Map<String,Set<TabletServerId>> currentResourceGroups() {
    return tserverResourceGroups;
  }

  @Override
  public String currentLevel() {
    return currentDataLevel.name();
  }
}
