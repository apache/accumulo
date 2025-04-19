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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.zookeeper.ZooCache;

import com.google.common.collect.ImmutableMap;

public class TableZooHelper {

  private final ClientContext context;

  public TableZooHelper(ClientContext context) {
    this.context = Objects.requireNonNull(context);
  }

  /**
   * Lookup table ID in ZK.
   *
   * @throws TableNotFoundException if not found; if the namespace was not found, this has a
   *         getCause() of NamespaceNotFoundException
   */
  public TableId getTableId(String tableName) throws TableNotFoundException {
    Pair<String,String> qualified = TableNameUtil.qualify(tableName);
    NamespaceId nid = context.getNamespaces().getNameToIdMap().get(qualified.getFirst());
    if (nid == null) {
      throw new TableNotFoundException(tableName, new NamespaceNotFoundException(null,
          qualified.getFirst(), "No mapping found for namespace"));
    }
    TableId tid = context.getTableMapping(nid).getNameToIdMap().get(qualified.getSecond());
    if (tid == null) {
      throw new TableNotFoundException(null, tableName,
          "No entry for this table found in the given namespace mapping");
    }
    return tid;
  }

  public String getTableName(TableId tableId) throws TableNotFoundException {
    Map<NamespaceId,String> namespaceMapping = context.getNamespaces().getIdToNameMap();
    for (NamespaceId namespaceId : namespaceMapping.keySet()) {
      var tableIdToNameMap = context.getTableMapping(namespaceId).getIdToNameMap();
      if (tableIdToNameMap.containsKey(tableId)) {
        return TableNameUtil.qualified(tableIdToNameMap.get(tableId),
            namespaceMapping.get(namespaceId));
      }
    }
    throw new TableNotFoundException(tableId.canonical(), null,
        "No entry for this table Id found in table mappings");
  }

  private Map<String,String> loadQualifiedTableMapping(boolean reverse) {
    final var builder = ImmutableMap.<String,String>builder();
    for (NamespaceId namespaceId : context.getNamespaces().getIdToNameMap().keySet()) {
      var idToNameMap = context.getTableMapping(namespaceId).getIdToNameMap();
      for (TableId tableId : idToNameMap.keySet()) {
        String fullyQualifiedName;
        try {
          fullyQualifiedName = TableNameUtil.qualified(idToNameMap.get(tableId),
              Namespaces.getNamespaceName(context, namespaceId));
        } catch (NamespaceNotFoundException e) {
          throw new RuntimeException(
              "getNamespaceName() failed to find namespace for namespaceId: " + namespaceId, e);
        }
        if (reverse) { // True: Name to Id. False: Id to Name.
          builder.put(fullyQualifiedName, tableId.canonical());
        } else {
          builder.put(tableId.canonical(), fullyQualifiedName);
        }
      }
    }
    return builder.build();
  }

  public Map<String,TableId> getQualifiedNameToIdMap() {
    var result = new HashMap<String,TableId>();
    for (var entry : loadQualifiedTableMapping(true).entrySet()) {
      result.put(entry.getKey(), TableId.of(entry.getValue()));
    }
    return result;
  }

  public Map<TableId,String> getIdtoQualifiedNameMap() {
    var result = new HashMap<TableId,String>();
    for (var entry : loadQualifiedTableMapping(false).entrySet()) {
      result.put(TableId.of(entry.getKey()), entry.getValue());
    }
    return result;
  }

  public boolean tableNodeExists(TableId tableId) {
    for (NamespaceId namespaceId : context.getNamespaces().getIdToNameMap().keySet()) {
      if (context.getTableMapping(namespaceId).getIdToNameMap().containsKey(tableId)) {
        return true;
      }
    }
    return false;
  }

  public void clearTableListCache() {
    context.getZooCache().clear(Constants.ZTABLES);
    context.getZooCache().clear(Constants.ZNAMESPACES);
  }

  public String getPrintableTableInfoFromId(TableId tableId) {
    try {
      return _printableTableInfo(getTableName(tableId), tableId);
    } catch (TableNotFoundException e) {
      return _printableTableInfo(null, tableId);
    }
  }

  public String getPrintableTableInfoFromName(String tableName) {
    try {
      return _printableTableInfo(tableName, getTableId(tableName));
    } catch (TableNotFoundException e) {
      return _printableTableInfo(tableName, null);
    }
  }

  private String _printableTableInfo(String tableName, TableId tableId) {
    return String.format("%s(ID:%s)", tableName == null ? "?" : tableName,
        tableId == null ? "?" : tableId.canonical());
  }

  /**
   * Get the current state of the table using the tableid. The boolean clearCache, if true will
   * clear the table state in zookeeper before fetching the state. Added with ACCUMULO-4574.
   *
   * @param tableId the table id
   * @param clearCachedState if true clear the table state in zookeeper before checking status
   * @return the table state.
   */
  public TableState getTableState(TableId tableId, boolean clearCachedState) {
    String statePath = Constants.ZTABLES + "/" + tableId.canonical() + Constants.ZTABLE_STATE;
    if (clearCachedState) {
      context.getZooCache().clear(statePath);
    }
    ZooCache zc = context.getZooCache();
    byte[] state = zc.get(statePath);
    if (state == null) {
      return TableState.UNKNOWN;
    }
    return TableState.valueOf(new String(state, UTF_8));
  }

  /**
   * Returns the namespace id for a given table ID.
   *
   * @param tableId The tableId
   * @return The namespace id which this table resides in.
   * @throws IllegalArgumentException if the table doesn't exist in ZooKeeper
   */
  public NamespaceId getNamespaceId(TableId tableId) throws TableNotFoundException {
    checkArgument(tableId != null, "tableId is null");

    if (AccumuloTable.allTableIds().contains(tableId)) {
      return Namespace.ACCUMULO.id();
    }
    for (NamespaceId namespaceId : context.getNamespaces().getIdToNameMap().keySet()) {
      if (context.getTableMapping(namespaceId).getIdToNameMap().containsKey(tableId)) {
        return namespaceId;
      }
    }
    throw new TableNotFoundException(tableId.canonical(), null,
        "No namespace found containing the given table ID " + tableId);
  }
}
