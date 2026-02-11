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
package org.apache.accumulo.manager.tableOps.clone;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;

class CloneInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  private final TableId srcTableId;
  private final String tableName;
  private TableId tableId;
  private final NamespaceId namespaceId;
  private final NamespaceId srcNamespaceId;
  private final Map<String,String> propertiesToSet;
  private final Set<String> propertiesToExclude;
  private final boolean keepOffline;
  private final String user;

  public CloneInfo(NamespaceId srcNamespaceId, TableId srcTableId, NamespaceId dstNamespaceId,
      String dstTableName, Map<String,String> propertiesToSet, Set<String> propertiesToExclude,
      boolean keepOffline, String user) {
    super();
    this.srcNamespaceId = srcNamespaceId;
    this.srcTableId = srcTableId;
    this.tableName = dstTableName;
    this.namespaceId = dstNamespaceId;
    this.propertiesToSet = propertiesToSet;
    this.propertiesToExclude = propertiesToExclude;
    this.keepOffline = keepOffline;
    this.user = user;
  }

  public TableId getSrcTableId() {
    return srcTableId;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableId(TableId dstTableId) {
    this.tableId = dstTableId;
  }

  public TableId getTableId() {
    return tableId;
  }

  public NamespaceId getNamespaceId() {
    return namespaceId;
  }

  public NamespaceId getSrcNamespaceId() {
    return srcNamespaceId;
  }

  public Map<String,String> getPropertiesToSet() {
    return propertiesToSet;
  }

  public Set<String> getPropertiesToExclude() {
    return propertiesToExclude;
  }

  public boolean isKeepOffline() {
    return keepOffline;
  }

  public String getUser() {
    return user;
  }

}
