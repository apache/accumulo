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
package org.apache.accumulo.master.tableOps;

import java.io.Serializable;
import java.util.Map;

import org.apache.accumulo.core.client.admin.InitialTableState;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;

public class TableInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  private String tableName;
  private TableId tableId;
  private NamespaceId namespaceId;

  private char timeType;
  private String user;

  // Record requested initial state at creation
  private InitialTableState initialTableState;

  // Track information related to initial split creation
  private int initialSplitSize;
  private String splitFile;
  private String splitDirsFile;

  public Map<String,String> props;
  public String defaultTabletDir = null;

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public TableId getTableId() {
    return tableId;
  }

  public void setTableId(TableId tableId) {
    this.tableId = tableId;
  }

  public NamespaceId getNamespaceId() {
    return namespaceId;
  }

  public void setNamespaceId(NamespaceId namespaceId) {
    this.namespaceId = namespaceId;
  }

  public char getTimeType() {
    return timeType;
  }

  public void setTimeType(char timeType) {
    this.timeType = timeType;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getSplitFile() {
    return splitFile;
  }

  public void setSplitFile(String splitFile) {
    this.splitFile = splitFile;
  }

  public String getSplitDirsFile() {
    return splitDirsFile;
  }

  public void setSplitDirsFile(String splitDirsFile) {
    this.splitDirsFile = splitDirsFile;
  }

  public InitialTableState getInitialTableState() {
    return initialTableState;
  }

  public void setInitialTableState(InitialTableState initialTableState) {
    this.initialTableState = initialTableState;
  }

  public int getInitialSplitSize() {
    return initialSplitSize;
  }

  public void setInitialSplitSize(int initialSplitSize) {
    this.initialSplitSize = initialSplitSize;
  }

}
