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
package org.apache.accumulo.server.manager.state;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import com.google.common.base.Suppliers;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * An immutable snapshot of the information needed by the TabletGroupWatcher and the
 * {@link TabletManagementIterator} to make decisions about tablets.
 */
public class TabletManagementParameters {

  private static final Gson GSON =
      new GsonBuilder().enableComplexMapKeySerialization().disableHtmlEscaping().create();

  private final ManagerState managerState;
  private final Map<Ample.DataLevel,Boolean> parentUpgradeMap;
  private final Set<TableId> onlineTables;
  private final Set<TServerInstance> serversToShutdown;
  private final Map<KeyExtent,TServerInstance> migrations;

  private final Ample.DataLevel level;

  private final Supplier<Map<TServerInstance,String>> resourceGroups;
  private final Map<String,Set<TServerInstance>> tserverGroups;
  private final Map<FateId,Map<String,String>> compactionHints;
  private final Set<TServerInstance> onlineTservers;
  private final boolean canSuspendTablets;
  private final Map<Path,Path> volumeReplacements;
  private final SteadyTime steadyTime;

  public TabletManagementParameters(ManagerState managerState,
      Map<Ample.DataLevel,Boolean> parentUpgradeMap, Set<TableId> onlineTables,
      LiveTServerSet.LiveTServersSnapshot liveTServersSnapshot,
      Set<TServerInstance> serversToShutdown, Map<KeyExtent,TServerInstance> migrations,
      Ample.DataLevel level, Map<FateId,Map<String,String>> compactionHints,
      boolean canSuspendTablets, Map<Path,Path> volumeReplacements, SteadyTime steadyTime) {
    this.managerState = managerState;
    this.parentUpgradeMap = Map.copyOf(parentUpgradeMap);
    // TODO could filter by level
    this.onlineTables = Set.copyOf(onlineTables);
    // This is already immutable, so no need to copy
    this.onlineTservers = liveTServersSnapshot.getTservers();
    this.serversToShutdown = Set.copyOf(serversToShutdown);
    // TODO could filter by level
    this.migrations = Map.copyOf(migrations);
    this.level = level;
    // This is already immutable, so no need to copy
    this.tserverGroups = liveTServersSnapshot.getTserverGroups();
    this.compactionHints = makeImmutable(compactionHints);
    this.resourceGroups = Suppliers.memoize(() -> {
      Map<TServerInstance,String> resourceGroups = new HashMap<>();
      TabletManagementParameters.this.tserverGroups.forEach((resourceGroup, tservers) -> tservers
          .forEach(tserver -> resourceGroups.put(tserver, resourceGroup)));
      return Map.copyOf(resourceGroups);
    });
    this.canSuspendTablets = canSuspendTablets;
    this.volumeReplacements = Map.copyOf(volumeReplacements);
    this.steadyTime = Objects.requireNonNull(steadyTime);
  }

  private TabletManagementParameters(JsonData jdata) {
    this.managerState = jdata.managerState;
    this.parentUpgradeMap = Map.copyOf(jdata.parentUpgradeMap);
    this.onlineTables = jdata.onlineTables.stream().map(TableId::of).collect(toUnmodifiableSet());
    this.onlineTservers =
        jdata.onlineTservers.stream().map(TServerInstance::new).collect(toUnmodifiableSet());
    this.serversToShutdown =
        jdata.serversToShutdown.stream().map(TServerInstance::new).collect(toUnmodifiableSet());
    this.migrations = jdata.migrations.entrySet().stream()
        .collect(toUnmodifiableMap(entry -> JsonData.strToExtent(entry.getKey()),
            entry -> new TServerInstance(entry.getValue())));
    this.level = jdata.level;
    this.compactionHints = makeImmutable(jdata.compactionHints.entrySet().stream()
        .collect(Collectors.toMap(entry -> FateId.from(entry.getKey()), Map.Entry::getValue)));
    this.tserverGroups = jdata.tserverGroups.entrySet().stream().collect(toUnmodifiableMap(
        Map.Entry::getKey,
        entry -> entry.getValue().stream().map(TServerInstance::new).collect(toUnmodifiableSet())));
    this.resourceGroups = Suppliers.memoize(() -> {
      Map<TServerInstance,String> resourceGroups = new HashMap<>();
      TabletManagementParameters.this.tserverGroups.forEach((resourceGroup, tservers) -> tservers
          .forEach(tserver -> resourceGroups.put(tserver, resourceGroup)));
      return Map.copyOf(resourceGroups);
    });
    this.canSuspendTablets = jdata.canSuspendTablets;
    this.volumeReplacements = Collections.unmodifiableMap(jdata.volumeReplacements);
    this.steadyTime = SteadyTime.from(jdata.steadyTime, TimeUnit.NANOSECONDS);
  }

  public ManagerState getManagerState() {
    return managerState;
  }

  public Map<Ample.DataLevel,Boolean> getParentUpgradeMap() {
    return parentUpgradeMap;
  }

  public boolean isParentLevelUpgraded() {
    return parentUpgradeMap.get(level);
  }

  public Set<TServerInstance> getOnlineTsevers() {
    return onlineTservers;
  }

  public Set<TServerInstance> getServersToShutdown() {
    return serversToShutdown;
  }

  public boolean isTableOnline(TableId tableId) {
    return onlineTables.contains(tableId);
  }

  public Map<KeyExtent,TServerInstance> getMigrations() {
    return migrations;
  }

  public Ample.DataLevel getLevel() {
    return level;
  }

  public String getResourceGroup(TServerInstance tserver) {
    return resourceGroups.get().get(tserver);
  }

  public Map<String,Set<TServerInstance>> getGroupedTServers() {
    return tserverGroups;
  }

  public Set<TableId> getOnlineTables() {
    return onlineTables;
  }

  public Map<FateId,Map<String,String>> getCompactionHints() {
    return compactionHints;
  }

  public boolean canSuspendTablets() {
    return canSuspendTablets;
  }

  public Map<Path,Path> getVolumeReplacements() {
    return volumeReplacements;
  }

  public SteadyTime getSteadyTime() {
    return steadyTime;
  }

  private static Map<FateId,Map<String,String>>
      makeImmutable(Map<FateId,Map<String,String>> compactionHints) {
    var copy = new HashMap<FateId,Map<String,String>>();
    compactionHints.forEach((ftxid, hints) -> copy.put(ftxid, Map.copyOf(hints)));
    return Collections.unmodifiableMap(copy);
  }

  private static class JsonData {

    final ManagerState managerState;
    final Map<Ample.DataLevel,Boolean> parentUpgradeMap;
    final Collection<String> onlineTables;
    final Collection<String> onlineTservers;
    final Collection<String> serversToShutdown;
    final Map<String,String> migrations;

    final Ample.DataLevel level;

    final Map<String,Set<String>> tserverGroups;

    final Map<String,Map<String,String>> compactionHints;

    final boolean canSuspendTablets;
    final Map<Path,Path> volumeReplacements;
    final long steadyTime;

    private static String toString(KeyExtent extent) {
      DataOutputBuffer buffer = new DataOutputBuffer();
      try {
        extent.writeTo(buffer);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      return Base64.getEncoder()
          .encodeToString(Arrays.copyOf(buffer.getData(), buffer.getLength()));

    }

    private static KeyExtent strToExtent(String kes) {
      byte[] data = Base64.getDecoder().decode(kes);
      DataInputBuffer buffer = new DataInputBuffer();
      buffer.reset(data, data.length);
      try {
        return KeyExtent.readFrom(buffer);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    JsonData(TabletManagementParameters params) {
      managerState = params.managerState;
      parentUpgradeMap = params.parentUpgradeMap;
      onlineTables = params.onlineTables.stream().map(AbstractId::canonical).collect(toList());
      onlineTservers = params.getOnlineTsevers().stream().map(TServerInstance::getHostPortSession)
          .collect(toList());
      serversToShutdown = params.serversToShutdown.stream().map(TServerInstance::getHostPortSession)
          .collect(toList());
      migrations = params.migrations.entrySet().stream().collect(
          toMap(entry -> toString(entry.getKey()), entry -> entry.getValue().getHostPortSession()));
      level = params.level;
      tserverGroups = params.getGroupedTServers().entrySet().stream()
          .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().stream()
              .map(TServerInstance::getHostPortSession).collect(toSet())));
      compactionHints = params.compactionHints.entrySet().stream()
          .collect(Collectors.toMap(entry -> entry.getKey().canonical(), Map.Entry::getValue));
      canSuspendTablets = params.canSuspendTablets;
      volumeReplacements = params.volumeReplacements;
      steadyTime = params.steadyTime.getNanos();
    }

  }

  public String serialize() {
    return GSON.toJson(new JsonData(this));
  }

  public static TabletManagementParameters deserialize(String json) {
    JsonData jdata = GSON.fromJson(json, JsonData.class);
    return new TabletManagementParameters(jdata);
  }

}
