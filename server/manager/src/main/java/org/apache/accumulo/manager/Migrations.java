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
package org.apache.accumulo.manager;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Migrations {
  private final EnumMap<DataLevel,SortedMap<KeyExtent,TServerInstance>> migrations;

  private static final Logger log = LoggerFactory.getLogger(Migrations.class);

  public Migrations() {
    this.migrations = new EnumMap<>(DataLevel.class);
    for (var dataLevel : DataLevel.values()) {
      migrations.put(dataLevel, Collections.synchronizedSortedMap(new TreeMap<>()));
    }
  }

  private void removeIf(Predicate<Map.Entry<KeyExtent,TServerInstance>> removalTest) {
    for (var dataLevel : DataLevel.values()) {
      var mmap = migrations.get(dataLevel);
      mmap.entrySet().removeIf(entry -> {
        if (removalTest.test(entry)) {
          log.trace("Removed migration {} -> {}", entry.getKey(), entry.getValue());
          return true;
        }
        return false;
      });
    }
  }

  public void removeTable(TableId tableId) {
    removeIf(entry -> entry.getKey().tableId().equals(tableId));
  }

  public void removeExtents(Set<KeyExtent> extents) {
    extents.forEach(this::removeExtent);
  }

  public TServerInstance removeExtent(KeyExtent extent) {
    var tserver = migrations.get(DataLevel.of(extent.tableId())).remove(extent);
    if (tserver != null) {
      log.trace("Removed migration {} -> {}", extent, tserver);
    }
    return tserver;
  }

  public void removeServers(Set<TServerInstance> servers) {
    removeIf(entry -> servers.contains(entry.getValue()));
  }

  public void put(KeyExtent extent, TServerInstance tServerInstance) {
    migrations.get(DataLevel.of(extent.tableId())).put(extent, tServerInstance);
    log.trace("Added migration {} -> {}", extent, tServerInstance);
  }

  public TServerInstance get(KeyExtent extent) {
    return migrations.get(DataLevel.of(extent.tableId())).get(extent);
  }

  public Set<KeyExtent> snapshotAll() {
    var copy = new HashSet<KeyExtent>();
    migrations.values().forEach(mset -> {
      synchronized (mset) {
        copy.addAll(mset.keySet());
      }
    });
    return Collections.unmodifiableSet(copy);
  }

  public Set<KeyExtent> snapshot(DataLevel dl) {
    return Set.copyOf(migrations.get(dl).keySet());
  }

  public Map<DataLevel,Set<KeyExtent>> mutableCopy() {
    Map<DataLevel,Set<KeyExtent>> copy = new EnumMap<>(DataLevel.class);
    for (var dataLevel : DataLevel.values()) {
      copy.put(dataLevel, new HashSet<>(migrations.get(dataLevel).keySet()));
    }
    return copy;
  }

  public boolean isEmpty() {
    for (var mset : migrations.values()) {
      if (!mset.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  public int size() {
    int size = 0;
    for (var mset : migrations.values()) {
      size += mset.size();
    }
    return size;
  }

  public boolean isEmpty(DataLevel dataLevel) {
    return migrations.get(dataLevel).isEmpty();
  }

  public boolean contains(KeyExtent extent) {
    return migrations.get(DataLevel.of(extent.tableId())).containsKey(extent);
  }
}
