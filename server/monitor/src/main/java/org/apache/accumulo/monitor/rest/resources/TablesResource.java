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
package org.apache.accumulo.monitor.rest.resources;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.api.TableInformation;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.TableInfoUtil;

@Path("/tables")
@Produces(MediaType.APPLICATION_JSON)
public class TablesResource {

  @GET
  public List<TableInformation> getTables() {
    Map<String,String> tidToNameMap = Tables.getIdToNameMap(HdfsZooInstance.getInstance());
    SortedMap<String,TableInfo> tableStats = new TreeMap<>();

    if (Monitor.getMmi() != null && Monitor.getMmi().tableMap != null)
      for (Entry<String,TableInfo> te : Monitor.getMmi().tableMap.entrySet())
        tableStats.put(Tables.getPrintableTableNameFromId(tidToNameMap, te.getKey()), te.getValue());

    Map<String,Double> compactingByTable = TableInfoUtil.summarizeTableStats(Monitor.getMmi());
    TableManager tableManager = TableManager.getInstance();

    List<TableInformation> tables = new ArrayList<>();
    for (Entry<String,String> entry : Tables.getNameToIdMap(HdfsZooInstance.getInstance()).entrySet()) {
      String tableName = entry.getKey(), tableId = entry.getValue();
      TableInfo tableInfo = tableStats.get(tableName);
      if (null != tableInfo) {
        Double holdTime = compactingByTable.get(tableId);
        if (holdTime == null)
          holdTime = Double.valueOf(0.);

        tables.add(new TableInformation(tableName, tableId, tableInfo, holdTime, tableManager.getTableState(tableId).name()));
      }
    }

    return tables;
  }
}
