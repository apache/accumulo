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
package org.apache.accumulo.core.util.tables;

import static java.util.Collections.emptySortedMap;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.clientImpl.NamespaceMapping.deserializeMap;
import static org.apache.accumulo.core.clientImpl.NamespaceMapping.serializeMap;

import java.util.Map;
import java.util.SortedMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.zookeeper.ZcStat;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSortedMap;

public class TableMapping {

  private final ClientContext context;
  private final NamespaceId namespaceId;
  private volatile SortedMap<TableId,String> currentTableMap = emptySortedMap();
  private volatile SortedMap<String,TableId> currentTableReverseMap = emptySortedMap();
  private volatile long lastMzxid;

  public TableMapping(ClientContext context, NamespaceId namespaceId) {
    this.context = context;
    this.namespaceId = namespaceId;
  }

  public void put(final ClientContext context, TableId tableId, String tableName,
      TableOperation operation)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    if (isBuiltInZKTable(tableId)) {
      throw new AssertionError("Putting built-in tables in map should not be possible after init");
    }
    var zoo = context.getZooSession().asReaderWriter();
    requireNonNull(zoo);
    requireNonNull(tableId);
    requireNonNull(namespaceId);
    requireNonNull(tableName);
    String zTableMapPath = getZTableMapPath(namespaceId);
    zoo.mutateExisting(zTableMapPath, data -> {
      var tables = deserializeMap(data);
      final String currentName = tables.get(tableId.canonical());
      if (tableName.equals(currentName)) {
        return null; // mapping already exists; operation is idempotent, so no change needed
      }
      if (currentName != null) {
        throw new AcceptableThriftTableOperationException(null, tableId.canonical(), operation,
            TableOperationExceptionType.EXISTS, "Table Id already exists");
      }
      if (tables.containsValue(tableName)) {
        throw new AcceptableThriftTableOperationException(null, tableId.canonical(), operation,
            TableOperationExceptionType.EXISTS, "Table name already exists");
      }
      tables.put(tableId.canonical(), tableName);
      return serializeMap(tables);
    });
  }

  public void remove(final ClientContext context, final TableId tableId)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    if (isBuiltInZKTable(tableId)) {
      throw new AssertionError("Removing built-in tables in map should not be possible");
    }
    var zoo = context.getZooSession().asReaderWriter();
    requireNonNull(zoo);
    requireNonNull(tableId);
    zoo.mutateExisting(getZTableMapPath(getNamespaceOfTableId(zoo, tableId)), data -> {
      var tables = deserializeMap(data);
      if (!tables.containsKey(tableId.canonical())) {
        throw new AcceptableThriftTableOperationException(null, tableId.canonical(),
            TableOperation.DELETE, TableOperationExceptionType.NOTFOUND,
            "Table already removed while processing");
      }
      tables.remove(tableId.canonical());
      return serializeMap(tables);
    });
  }

  public void rename(final ClientContext context, final TableId tableId, final String oldName,
      final String newName)
      throws InterruptedException, KeeperException, AcceptableThriftTableOperationException {
    if (isBuiltInZKTable(tableId)) {
      throw new AssertionError("Renaming built-in tables in map should not be possible");
    }
    var zoo = context.getZooSession().asReaderWriter();
    requireNonNull(zoo);
    requireNonNull(tableId);
    requireNonNull(namespaceId);
    requireNonNull(oldName);
    requireNonNull(newName);
    String zTableMapPath = getZTableMapPath(namespaceId);
    zoo.mutateExisting(zTableMapPath, current -> {
      var tables = deserializeMap(current);
      final String currentName = tables.get(tableId.canonical());
      if (newName.equals(currentName)) {
        return null; // assume in this case the operation is running again, so we are done
      }
      if (!oldName.equals(currentName)) {
        throw new AcceptableThriftTableOperationException(null, oldName, TableOperation.RENAME,
            TableOperationExceptionType.NOTFOUND, "Name changed while processing");
      }
      if (tables.containsValue(newName)) {
        throw new AcceptableThriftTableOperationException(null, newName, TableOperation.RENAME,
            TableOperationExceptionType.EXISTS, "Table name already exists");
      }
      tables.put(tableId.canonical(), newName);
      return serializeMap(tables);
    });
  }

  private synchronized void update(NamespaceId namespaceId) {
    final ZooCache zc = context.getZooCache();
    final ZcStat stat = new ZcStat();
    final String zTableMapPath = getZTableMapPath(namespaceId);

    byte[] data = zc.get(zTableMapPath, stat);
    if (stat.getMzxid() > lastMzxid) {
      if (data == null) {
        throw new IllegalStateException(zTableMapPath + " node should not be null");
      }
      Map<String,String> idToName = deserializeMap(data);
      if (namespaceId.equals(Namespace.ACCUMULO.id())) {
        for (TableId tid : AccumuloTable.allTableIds()) {
          if (!idToName.containsKey(tid.canonical())) {
            throw new IllegalStateException("Built-in tables are not present in map");
          }
        }
      }
      var converted = ImmutableSortedMap.<TableId,String>naturalOrder();
      var convertedReverse = ImmutableSortedMap.<String,TableId>naturalOrder();
      idToName.forEach((idString, name) -> {
        var id = TableId.of(idString);
        converted.put(id, name);
        convertedReverse.put(name, id);
      });
      currentTableMap = converted.build();
      currentTableReverseMap = convertedReverse.build();

      lastMzxid = stat.getMzxid();
    } else if (data == null) { // If namespace happens to be deleted
      currentTableMap = emptySortedMap();
      currentTableReverseMap = emptySortedMap();
    }
  }

  public static String getZTableMapPath(NamespaceId namespaceId) {
    return Constants.ZNAMESPACES + "/" + namespaceId + Constants.ZTABLES;
  }

  private static boolean isBuiltInZKTable(TableId tableId) {
    return AccumuloTable.allTableIds().contains(tableId);
  }

  private static NamespaceId getNamespaceOfTableId(ZooReaderWriter zoo, TableId tableId)
      throws IllegalArgumentException, InterruptedException, KeeperException {
    for (String nid : zoo.getChildren(Constants.ZNAMESPACES)) {
      for (String tid : zoo.getChildren(Constants.ZNAMESPACES + "/" + nid)) {
        if (TableId.of(tid).equals(tableId)) {
          return NamespaceId.of(nid);
        }
      }
    }
    throw new KeeperException.NoNodeException(
        "TableId " + tableId.canonical() + " does not exist in ZooKeeper");
  }

  public SortedMap<TableId,String> getIdToNameMap() {
    update(namespaceId);
    return currentTableMap;
  }

  public SortedMap<String,TableId> getNameToIdMap() {
    update(namespaceId);
    return currentTableReverseMap;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass()).add("namespaceId", namespaceId)
        .add("currentTableMap", currentTableMap).toString();
  }

}
