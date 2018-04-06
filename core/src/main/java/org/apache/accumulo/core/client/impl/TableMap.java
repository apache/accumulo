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
package org.apache.accumulo.core.client.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.client.impl.Tables.qualified;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * Used for thread safe caching of immutable table ID maps. See ACCUMULO-4778.
 */
public class TableMap {
  private static final Logger log = LoggerFactory.getLogger(TableMap.class);

  private final Map<String,Table.ID> tableNameToIdMap;
  private final Map<Table.ID,String> tableIdToNameMap;

  public TableMap(Instance instance, ZooCache zooCache) {
    List<String> tableIds = zooCache.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES);
    Map<Namespace.ID,String> namespaceIdToNameMap = new HashMap<>();
    ImmutableMap.Builder<String,Table.ID> tableNameToIdBuilder = new ImmutableMap.Builder<>();
    ImmutableMap.Builder<Table.ID,String> tableIdToNameBuilder = new ImmutableMap.Builder<>();
    // use StringBuilder to construct zPath string efficiently across many tables
    StringBuilder zPathBuilder = new StringBuilder();
    zPathBuilder.append(ZooUtil.getRoot(instance)).append(Constants.ZTABLES).append("/");
    int prefixLength = zPathBuilder.length();

    for (String tableIdStr : tableIds) {
      // reset StringBuilder to prefix length before appending ID and suffix
      zPathBuilder.setLength(prefixLength);
      zPathBuilder.append(tableIdStr).append(Constants.ZTABLE_NAME);
      byte[] tableName = zooCache.get(zPathBuilder.toString());
      zPathBuilder.setLength(prefixLength);
      zPathBuilder.append(tableIdStr).append(Constants.ZTABLE_NAMESPACE);
      byte[] nId = zooCache.get(zPathBuilder.toString());

      String namespaceName = Namespace.DEFAULT;
      // create fully qualified table name
      if (nId == null) {
        namespaceName = null;
      } else {
        Namespace.ID namespaceId = Namespace.ID.of(new String(nId, UTF_8));
        if (!namespaceId.equals(Namespace.ID.DEFAULT)) {
          try {
            namespaceName = namespaceIdToNameMap.get(namespaceId);
            if (namespaceName == null) {
              namespaceName = Namespaces.getNamespaceName(instance, namespaceId);
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
        String tableNameStr = qualified(new String(tableName, UTF_8), namespaceName);
        Table.ID tableId = Table.ID.of(tableIdStr);
        tableNameToIdBuilder.put(tableNameStr, tableId);
        tableIdToNameBuilder.put(tableId, tableNameStr);
      }
    }
    tableNameToIdMap = tableNameToIdBuilder.build();
    tableIdToNameMap = tableIdToNameBuilder.build();
  }

  public Map<String,Table.ID> getNameToIdMap() {
    return tableNameToIdMap;
  }

  public Map<Table.ID,String> getIdtoNameMap() {
    return tableIdToNameMap;
  }
}
