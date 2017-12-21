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
package org.apache.accumulo.monitor.rest.tables;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.TableInfoUtil;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *
 * Generates the JSON of tables for DataTables
 *
 * @since 2.0.0
 *
 */
@Path("/dataTables")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class DataTablesResource {

    /**
     * Generates a list of all the tables
     *
     * @return list with all tables
     */
    @GET
    public static TableInformationList getTables() {

        TableInformationList tableList = new TableInformationList();
        SortedMap<Table.ID,TableInfo> tableStats = new TreeMap<>();

        if (Monitor.getMmi() != null && Monitor.getMmi().tableMap != null)
            for (Map.Entry<String,TableInfo> te : Monitor.getMmi().tableMap.entrySet())
                tableStats.put(Table.ID.of(te.getKey()), te.getValue());

        Map<String,Double> compactingByTable = TableInfoUtil.summarizeTableStats(Monitor.getMmi());
        TableManager tableManager = TableManager.getInstance();

        // Add tables to the list
        for (Map.Entry<String,Table.ID> entry : Tables.getNameToIdMap(HdfsZooInstance.getInstance()).entrySet()) {
            String tableName = entry.getKey();
            Table.ID tableId = entry.getValue();
            TableInfo tableInfo = tableStats.get(tableId);
            TableState tableState = tableManager.getTableState(tableId);

            if (null != tableInfo && !tableState.equals(TableState.OFFLINE)) {
                Double holdTime = compactingByTable.get(tableId.canonicalID());
                if (holdTime == null)
                    holdTime = Double.valueOf(0.);

                tableList.addTable(new TableInformation(tableName, tableId, tableInfo, holdTime, tableState.name()));
            } else {
                tableList.addTable(new TableInformation(tableName, tableId, tableState.name()));
            }
        }
        return tableList;
    }

}
