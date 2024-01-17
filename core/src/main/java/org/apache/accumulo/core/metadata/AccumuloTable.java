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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.TableId;

/**
 * Defines the name and id of all tables in the accumulo table namespace.
 */
public enum AccumuloTable {

  ROOT("root", "+r"), METADATA("metadata", "!0"), FATE("fate", "+fate");

  private final String name;
  private final TableId tableId;

  public String tableName() {
    return name;
  }

  public TableId tableId() {
    return tableId;
  }

  AccumuloTable(String name, String id) {
    this.name = Namespace.ACCUMULO.name() + "." + name;
    this.tableId = TableId.of(id);
  }

  private static final Set<TableId> ALL_IDS =
      Arrays.stream(values()).map(AccumuloTable::tableId).collect(Collectors.toUnmodifiableSet());

  public static Set<TableId> allTableIds() {
    return ALL_IDS;
  }
}
