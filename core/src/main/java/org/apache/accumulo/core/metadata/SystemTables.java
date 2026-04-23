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
package org.apache.accumulo.core.metadata;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;

/**
 * Defines the name and id of all tables in the accumulo table namespace.
 */
public enum SystemTables {

  ROOT("root", "+r"),
  METADATA("metadata", "!0"),
  FATE("fate", "+fate"),
  SCAN_REF("scanref", "+scanref");

  private final String simpleName;
  private final String qualifiedName;
  private final TableId tableId;

  public String tableName() {
    return qualifiedName;
  }

  public String simpleTableName() {
    return simpleName;
  }

  public TableId tableId() {
    return tableId;
  }

  private SystemTables(String simpleName, String id) {
    this.simpleName = simpleName;
    this.qualifiedName = namespaceName() + "." + simpleName;
    this.tableId = TableId.of(id);
  }

  private static final Set<TableId> ALL_IDS =
      Arrays.stream(values()).map(SystemTables::tableId).collect(Collectors.toUnmodifiableSet());
  private static final Set<String> ALL_NAMES =
      Arrays.stream(values()).map(SystemTables::tableName).collect(Collectors.toUnmodifiableSet());

  private static final Map<String,String> TABLE_ID_TO_SIMPLE_NAME =
      Arrays.stream(values()).collect(Collectors.toUnmodifiableMap(
          sysTable -> sysTable.tableId().canonical(), sysTable -> sysTable.simpleName));

  public static NamespaceId namespaceId() {
    return Namespace.ACCUMULO.id();
  }

  public static String namespaceName() {
    return Namespace.ACCUMULO.name();
  }

  public static Set<TableId> tableIds() {
    return ALL_IDS;
  }

  public static Set<String> tableNames() {
    return ALL_NAMES;
  }

  public static boolean containsTableId(TableId tableId) {
    return ALL_IDS.contains(tableId);
  }

  public static boolean containsTableName(String tableName) {
    return ALL_NAMES.contains(tableName);
  }

  public static Map<String,String> tableIdToSimpleNameMap() {
    return TABLE_ID_TO_SIMPLE_NAME;
  }
}
