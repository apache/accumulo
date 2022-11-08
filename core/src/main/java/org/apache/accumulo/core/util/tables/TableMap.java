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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * Used for thread safe caching of immutable table ID maps. See ACCUMULO-4778.
 */
public class TableMap {
  private static final Logger log = LoggerFactory.getLogger(TableMap.class);

  private final Map<String,TableId> tableNameToIdMap;
  private final Map<TableId,String> tableIdToNameMap;

  private final ZooCache zooCache;
  private final long updateCount;

  public TableMap(ClientContext context) {

    this.zooCache = context.getZooCache();
    // important to read this first
    this.updateCount = zooCache.getUpdateCount();

    List<String> tableIds = zooCache.getChildren(context.getZooKeeperRoot() + Constants.ZTABLES);
    Map<NamespaceId,String> namespaceIdToNameMap = new HashMap<>();
    final var tableNameToIdBuilder = ImmutableMap.<String,TableId>builder();
    final var tableIdToNameBuilder = ImmutableMap.<TableId,String>builder();

    // use StringBuilder to construct zPath string efficiently across many tables
    StringBuilder zPathBuilder = new StringBuilder();
    zPathBuilder.append(context.getZooKeeperRoot()).append(Constants.ZTABLES).append("/");
    int prefixLength = zPathBuilder.length();

    for (String tableIdStr : tableIds) {
      // reset StringBuilder to prefix length before appending ID and suffix
      zPathBuilder.setLength(prefixLength);
      zPathBuilder.append(tableIdStr).append(Constants.ZTABLE_NAME);
      byte[] tableName = zooCache.get(zPathBuilder.toString());
      zPathBuilder.setLength(prefixLength);
      zPathBuilder.append(tableIdStr).append(Constants.ZTABLE_NAMESPACE);
      byte[] nId = zooCache.get(zPathBuilder.toString());

      String namespaceName = Namespace.DEFAULT.name();
      // create fully qualified table name
      if (nId == null) {
        namespaceName = null;
      } else {
        NamespaceId namespaceId = NamespaceId.of(new String(nId, UTF_8));
        if (!namespaceId.equals(Namespace.DEFAULT.id())) {
          try {
            namespaceName = namespaceIdToNameMap.get(namespaceId);
            if (namespaceName == null) {
              namespaceName = Namespaces.getNamespaceName(context, namespaceId);
              namespaceIdToNameMap.put(namespaceId, namespaceName);
            }
          } catch (NamespaceNotFoundException e) {
            log.error("Table (" + tableIdStr + ") contains reference to namespace (" + namespaceId
                + ") that doesn't exist", e);
            continue;
          }
        }
      }
      if (tableName != null && namespaceName != null) {
        String tableNameStr = TableNameUtil.qualified(new String(tableName, UTF_8), namespaceName);
        TableId tableId = TableId.of(tableIdStr);
        tableNameToIdBuilder.put(tableNameStr, tableId);
        tableIdToNameBuilder.put(tableId, tableNameStr);
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
