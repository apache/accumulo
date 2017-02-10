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
import java.util.TreeSet;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.api.TableInformation;
import org.apache.accumulo.monitor.rest.api.TableNamespace;
import org.apache.accumulo.monitor.rest.api.TablesList;
import org.apache.accumulo.monitor.rest.api.TabletServer;
import org.apache.accumulo.monitor.rest.api.TabletServers;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.apache.hadoop.io.Text;

@Path("/tables")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class TablesResource {

  private static final TabletServerStatus NO_STATUS = new TabletServerStatus();

  @GET
  public TablesList getTables() {
    Map<String,String> tidToNameMap = Tables.getIdToNameMap(HdfsZooInstance.getInstance());
    SortedMap<String,TableInfo> tableStats = new TreeMap<>();

    if (Monitor.getMmi() != null && Monitor.getMmi().tableMap != null)
      for (Entry<String,TableInfo> te : Monitor.getMmi().tableMap.entrySet())
        tableStats.put(Tables.getPrintableTableNameFromId(tidToNameMap, te.getKey()), te.getValue());

    Map<String,Double> compactingByTable = TableInfoUtil.summarizeTableStats(Monitor.getMmi());
    TableManager tableManager = TableManager.getInstance();

    SortedMap<String,String> namespaces = Namespaces.getNameToIdMap(Monitor.getContext().getInstance());

    TablesList tableNamespace = new TablesList();
    List<TableInformation> tables = new ArrayList<>();

    for (String key : namespaces.keySet()) {
      tableNamespace.addTable(new TableNamespace(key));
    }

    for (Entry<String,String> entry : Tables.getNameToIdMap(HdfsZooInstance.getInstance()).entrySet()) {
      String tableName = entry.getKey(), tableId = entry.getValue();
      TableInfo tableInfo = tableStats.get(tableName);
      if (null != tableInfo) {
        Double holdTime = compactingByTable.get(tableId);
        if (holdTime == null)
          holdTime = Double.valueOf(0.);

        for (TableNamespace name : tableNamespace.tables) {
          if (!tableName.contains(".") && name.namespace.equals("")) {
            name.addTable(new TableInformation(tableName, tableId, tableInfo, holdTime, tableManager.getTableState(tableId).name()));
          } else if (tableName.startsWith(name.namespace + ".")) {
            name.addTable(new TableInformation(tableName, tableId, tableInfo, holdTime, tableManager.getTableState(tableId).name()));
          }
        }

        tables.add(new TableInformation(tableName, tableId, tableInfo, holdTime, tableManager.getTableState(tableId).name()));
      } else {
        for (TableNamespace name : tableNamespace.tables) {
          if (!tableName.contains(".") && name.namespace.equals("")) {
            name.addTable(new TableInformation(tableName, tableId, tableManager.getTableState(tableId).name()));
          } else if (tableName.startsWith(name.namespace + ".")) {
            name.addTable(new TableInformation(tableName, tableId, tableManager.getTableState(tableId).name()));
          }
        }
        tables.add(new TableInformation(tableName, tableId, tableManager.getTableState(tableId).name()));
      }
    }

    return tableNamespace;
  }

  @Path("/{tableId}")
  @GET
  public TabletServers getParticipatingTabletServers(@PathParam("tableId") String tableId) throws Exception {
    Instance instance = Monitor.getContext().getInstance();

    TreeSet<String> locs = new TreeSet<>();
    if (RootTable.ID.equals(tableId)) {
      locs.add(instance.getRootTabletLocation());
    } else {
      String systemTableName = MetadataTable.ID.equals(tableId) ? RootTable.NAME : MetadataTable.NAME;
      MetaDataTableScanner scanner = new MetaDataTableScanner(Monitor.getContext(), new Range(KeyExtent.getMetadataEntry(tableId, new Text()),
          KeyExtent.getMetadataEntry(tableId, null)), systemTableName);

      while (scanner.hasNext()) {
        TabletLocationState state = scanner.next();
        if (state.current != null) {
          try {
            locs.add(state.current.hostPort());
          } catch (Exception ex) {}
        }
      }
      scanner.close();
    }

    TabletServers tabletServers = new TabletServers(Monitor.getMmi().tServerInfo.size());

    List<TabletServerStatus> tservers = new ArrayList<>();
    if (Monitor.getMmi() != null) {
      for (TabletServerStatus tss : Monitor.getMmi().tServerInfo) {
        try {
          if (tss.name != null && locs.contains(tss.name))
            tservers.add(tss);
        } catch (Exception ex) {

        }
      }
    }

    for (TabletServerStatus status : tservers) {
      if (status == null)
        status = NO_STATUS;
      TableInfo summary = TableInfoUtil.summarizeTableStats(status);
      if (tableId != null)
        summary = status.tableMap.get(tableId);
      if (summary == null)
        continue;

      TabletServer tabletServerInfo = new TabletServer();
      tabletServerInfo.updateTabletServerInfo(status, summary);

      tabletServers.addTablet(tabletServerInfo);
    }

    return tabletServers;

  }
}
