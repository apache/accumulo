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

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.NamespaceMapping;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.zookeeper.ZooCache;

import com.google.common.collect.ImmutableMap;

/**
 * Used for thread safe caching of immutable table ID maps. See ACCUMULO-4778.
 */
public class TableMap {
  private final Map<String,TableId> tableNameToIdMap;
  private final Map<TableId,String> tableIdToNameMap;

  private final ZooCache zooCache;
  private final long updateCount;

  public TableMap(ClientContext context) throws NamespaceNotFoundException {

    this.zooCache = context.getZooCache();
    // important to read this first
    this.updateCount = zooCache.getUpdateCount();

    final var tableNameToIdBuilder = ImmutableMap.<String,TableId>builder();
    final var tableIdToNameBuilder = ImmutableMap.<TableId,String>builder();

    List<String> namespaceIds = zooCache.getChildren(Constants.ZNAMESPACES);
    for (String namespaceId : namespaceIds) {
      byte[] rawTableMap =
          zooCache.get(Constants.ZNAMESPACES + "/" + namespaceId + Constants.ZTABLES);
      Map<String,String> tableMap = NamespaceMapping.deserializeMap(rawTableMap);
      for (Map.Entry<String,String> entry : tableMap.entrySet()) {
        String fullyQualifiedName = TableNameUtil.qualified(entry.getValue(),
            Namespaces.getNamespaceName(context, NamespaceId.of(namespaceId)));
        TableId tableId = TableId.of(entry.getKey());
        tableNameToIdBuilder.put(fullyQualifiedName, tableId);
        tableIdToNameBuilder.put(tableId, fullyQualifiedName);
      }
    }
    tableNameToIdMap = tableNameToIdBuilder.build();
    tableIdToNameMap = tableIdToNameBuilder.build();
  }

  public Map<String,TableId> getNameToIdMap() {
    return tableNameToIdMap;
  }

  public Map<TableId,String> getIdtoNameMap() {
    return tableIdToNameMap;
  }

  public boolean isCurrent(ZooCache zc) {
    return this.zooCache == zc && this.updateCount == zc.getUpdateCount();
  }
}
