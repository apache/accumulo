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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.tservers.TabletServer;
import org.apache.accumulo.monitor.rest.tservers.TabletServers;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.apache.hadoop.io.Text;

/**
 *
 * Generates a tables list from the Monitor as a JSON object
 *
 * @since 2.0.0
 *
 */
@Path("/tables")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class TablesResource {

  private static final TabletServerStatus NO_STATUS = new TabletServerStatus();

  /**
   * Generates a table list based on the namespace
   *
   * @param namespace
   *          Namespace used to filter the tables
   * @return Table list
   */
  private static TablesList generateTables(String namespace) {
    SortedMap<String,Namespace.ID> namespaces = Namespaces.getNameToIdMap(Monitor.getContext().getInstance());

    TablesList tableNamespace = new TablesList();

    /*
     * Add the tables that have the selected namespace Asterisk = All namespaces Hyphen = Default namespace
     */
    for (String key : namespaces.keySet()) {
      if (namespace.equals("*") || namespace.equals(key) || (key.isEmpty() && namespace.equals("-"))) {
        tableNamespace.addTable(new TableNamespace(key));
      }
    }

    return generateTables(tableNamespace);
  }

  /**
   * Generates a table list based on the list of namespaces
   *
   * @param tableNamespace
   *          Namespace list
   * @return Table list
   */
  private static TablesList generateTables(TablesList tableNamespace) {
    Instance inst = Monitor.getContext().getInstance();
    SortedMap<String,TableInfo> tableStats = new TreeMap<>();

    if (Monitor.getMmi() != null && Monitor.getMmi().tableMap != null)
      for (Entry<String,TableInfo> te : Monitor.getMmi().tableMap.entrySet())
        tableStats.put(Tables.getPrintableTableInfoFromId(inst, Table.ID.of(te.getKey())), te.getValue());
    Map<String,Double> compactingByTable = TableInfoUtil.summarizeTableStats(Monitor.getMmi());
    TableManager tableManager = TableManager.getInstance();
    List<TableInformation> tables = new ArrayList<>();

    // Add tables to the list
    for (Entry<String,Table.ID> entry : Tables.getNameToIdMap(HdfsZooInstance.getInstance()).entrySet()) {
      String tableName = entry.getKey();
      Table.ID tableId = entry.getValue();
      TableInfo tableInfo = tableStats.get(tableName);
      if (null != tableInfo) {
        Double holdTime = compactingByTable.get(tableId.canonicalID());
        if (holdTime == null)
          holdTime = Double.valueOf(0.);

        for (TableNamespace name : tableNamespace.tables) {
          // Check if table has the default namespace
          if (!tableName.contains(".") && name.namespace.isEmpty()) {
            name.addTable(new TableInformation(tableName, tableId, tableInfo, holdTime, tableManager.getTableState(tableId).name()));
          } else if (tableName.startsWith(name.namespace + ".")) {
            name.addTable(new TableInformation(tableName, tableId, tableInfo, holdTime, tableManager.getTableState(tableId).name()));
          }
        }
        tables.add(new TableInformation(tableName, tableId, tableInfo, holdTime, tableManager.getTableState(tableId).name()));
      } else {
        for (TableNamespace name : tableNamespace.tables) {
          if (!tableName.contains(".") && name.namespace.isEmpty()) {
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

  /**
   * Generates a list of all the tables
   *
   * @return list with all tables
   */
  @GET
  public static TablesList getTables() {
    return generateTables("*");
  }

  /**
   * Generates a list with the selected namespace
   *
   * @param namespace
   *          Namespace to filter tables
   * @return list with selected tables
   */
  @GET
  @Path("namespace/{namespace}")
  public TablesList getTable(@PathParam("namespace") String namespace) {
    return generateTables(namespace);
  }

  /**
   * Generates a list with the list of namespaces
   *
   * @param namespaceList
   *          List of namespaces separated by a comma
   * @return list with selected tables
   */
  @GET
  @Path("namespaces/{namespaces}")
  public TablesList getTableWithNamespace(@PathParam("namespaces") String namespaceList) {
    SortedMap<String,Namespace.ID> namespaces = Namespaces.getNameToIdMap(Monitor.getContext().getInstance());

    TablesList tableNamespace = new TablesList();
    /*
     * Add the tables that have the selected namespace Asterisk = All namespaces Hyphen = Default namespace
     */
    for (String namespace : namespaceList.split(",")) {
      for (String key : namespaces.keySet()) {
        if (namespace.equals("*") || namespace.equals(key) || (key.isEmpty() && namespace.equals("-"))) {
          tableNamespace.addTable(new TableNamespace(key));
        }
      }
    }

    return generateTables(tableNamespace);
  }

  /**
   * Generates a list of participating tservers for a table
   *
   * @param tableIdStr
   *          Table ID to find participating tservers
   * @return List of participating tservers
   */
  @Path("{tableId}")
  @GET
  public TabletServers getParticipatingTabletServers(@PathParam("tableId") String tableIdStr) throws Exception {
    Instance instance = Monitor.getContext().getInstance();
    Table.ID tableId = Table.ID.of(tableIdStr);

    TabletServers tabletServers = new TabletServers(Monitor.getMmi().tServerInfo.size());

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
          } catch (Exception ex) {
            scanner.close();
            return tabletServers;
          }
        }
      }
      scanner.close();
    }

    List<TabletServerStatus> tservers = new ArrayList<>();
    if (Monitor.getMmi() != null) {
      for (TabletServerStatus tss : Monitor.getMmi().tServerInfo) {
        try {
          if (tss.name != null && locs.contains(tss.name))
            tservers.add(tss);
        } catch (Exception ex) {
          return tabletServers;
        }
      }
    }

    // Adds tservers to the list
    for (TabletServerStatus status : tservers) {
      if (status == null)
        status = NO_STATUS;
      TableInfo summary = TableInfoUtil.summarizeTableStats(status);
      if (tableId != null)
        summary = status.tableMap.get(tableId.canonicalID());
      if (summary == null)
        continue;

      TabletServer tabletServerInfo = new TabletServer();
      tabletServerInfo.updateTabletServerInfo(status, summary);

      tabletServers.addTablet(tabletServerInfo);
    }

    return tabletServers;

  }

  /**
   * Generates the list of existing namespaces
   *
   * @return list of namespaces as a JSON object
   */
  @Path("namespaces")
  @GET
  public Map<String,List<String>> getNamespaces() {
    Instance inst = Monitor.getContext().getInstance();
    return Collections.singletonMap("namespaces", Namespaces.getNameToIdMap(inst).keySet().stream().collect(Collectors.toList()));
  }
}
