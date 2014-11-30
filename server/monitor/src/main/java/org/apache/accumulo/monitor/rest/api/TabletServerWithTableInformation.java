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
package org.apache.accumulo.monitor.rest.api;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class TabletServerWithTableInformation {

  private TabletServerInformation tserverInfo;
  private TableInformation tableInfo;

  public TabletServerWithTableInformation(TabletServerInformation tserverInfo, TableInformation tableInfo) {
    this.tserverInfo = tserverInfo;
    this.tableInfo = tableInfo;
  }

  @JsonProperty("tserver")
  public TabletServerInformation getTserver() {
    return tserverInfo;
  }

  @JsonProperty("tserver")
  public void setTserver(TabletServerInformation tserverInfo) {
    this.tserverInfo = tserverInfo;
  }

  @JsonProperty("table")
  public TableInformation getTable() {
    return tableInfo;
  }

  @JsonProperty("table")
  public void setTable(TableInformation tableInfo) {
    this.tableInfo = tableInfo;
  }
}
