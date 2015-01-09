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
package org.apache.accumulo.monitor.servlets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.util.Table;
import org.apache.accumulo.monitor.util.TableRow;
import org.apache.accumulo.monitor.util.celltypes.CompactionsType;
import org.apache.accumulo.monitor.util.celltypes.DurationType;
import org.apache.accumulo.monitor.util.celltypes.NumberType;
import org.apache.accumulo.monitor.util.celltypes.TableLinkType;
import org.apache.accumulo.monitor.util.celltypes.TableStateType;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.apache.hadoop.io.Text;

public class TablesServlet extends BasicServlet {

  private static final long serialVersionUID = 1L;

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Table Status";
  }

  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse response, StringBuilder sb) throws Exception {
    Map<String,String> tidToNameMap = Tables.getIdToNameMap(HdfsZooInstance.getInstance());
    String tableId = req.getParameter("t");

    doProblemsBanner(sb);

    if (tableId == null || tableId.isEmpty() || tidToNameMap.containsKey(tableId) == false) {
      doTableList(req, sb, tidToNameMap);
      return;
    }

    doTableDetails(req, sb, tidToNameMap, tableId);
  }

  static void doProblemsBanner(StringBuilder sb) {
    int numProblems = Monitor.getProblemSummary().entrySet().size();
    if (numProblems > 0)
      banner(sb, "error", String.format("<a href='/problems'>Table Problems: %d Total</a>", numProblems));
  }

  static void doTableList(HttpServletRequest req, StringBuilder sb, Map<String,String> tidToNameMap) {
    Table tableList = new Table("tableList", "Table&nbsp;List");
    tableList.addSortableColumn("Table&nbsp;Name", new TableLinkType(), null);
    tableList.addSortableColumn("State", new TableStateType(), null);
    tableList.addSortableColumn("#&nbsp;Tablets", new NumberType<Integer>(), "Tables are broken down into ranges of rows called tablets.");
    tableList.addSortableColumn("#&nbsp;Offline<br />Tablets", new NumberType<Integer>(0, 0), "Tablets unavailable for query or ingest.  "
        + "May be a transient condition when tablets are moved for balancing.");
    tableList.addSortableColumn("Entries", new NumberType<Long>(), "Key/value pairs over each instance, table or tablet.");
    tableList.addSortableColumn("Entries<br />In&nbsp;Memory", new NumberType<Long>(),
        "The total number of key/value pairs stored in memory and not yet written to disk");
    tableList.addSortableColumn("Ingest", new NumberType<Long>(), "The number of Key/Value pairs inserted.  Note that deletes are 'inserted'.");
    tableList.addSortableColumn("Entries<br/>Read", new NumberType<Long>(),
        "The number of Key/Value pairs read on the server side.  Not all key values read may be returned to client because of filtering.");
    tableList.addSortableColumn("Entries<br/>Returned", new NumberType<Long>(),
        "The number of Key/Value pairs returned to clients during queries.  This is <b>not</b> the number of scans.");
    tableList.addSortableColumn("Hold&nbsp;Time", new DurationType(0l, 0l),
        "The amount of time that ingest operations are suspended while waiting for data to be written to disk.");
    tableList.addSortableColumn("Running<br />Scans", new CompactionsType("scans"),
        "Information about the scans threads.  Shows how many threads are running and how much work is queued for the threads.");
    tableList.addSortableColumn("Minor<br />Compactions", new CompactionsType("minor"), "Flushing memory to disk is called a \"minor compaction.\" "
        + "Multiple tablets can be minor compacted simultaneously, but " + "" + "sometimes they must wait for resources to be available.  These "
        + "tablets that are waiting for compaction are \"queued\" and are " + "indicated using parentheses. So <tt> 2 (3)</tt> indicates there are "
        + "two compactions running and three queued waiting for resources.");
    tableList.addSortableColumn("Major<br />Compactions", new CompactionsType("major"),
        "Gathering up many small files and rewriting them as one larger file is called a 'Major Compaction'. "
            + "Major Compactions are performed as a consequence of new files created from Minor Compactions and Bulk Load operations.  "
            + "They reduce the number of files used during queries.");
    SortedMap<String,TableInfo> tableStats = new TreeMap<String,TableInfo>();

    if (Monitor.getMmi() != null && Monitor.getMmi().tableMap != null)
      for (Entry<String,TableInfo> te : Monitor.getMmi().tableMap.entrySet())
        tableStats.put(Tables.getPrintableTableNameFromId(tidToNameMap, te.getKey()), te.getValue());

    Map<String,Double> compactingByTable = TableInfoUtil.summarizeTableStats(Monitor.getMmi());
    TableManager tableManager = TableManager.getInstance();

    for (Entry<String,String> tableName_tableId : Tables.getNameToIdMap(HdfsZooInstance.getInstance()).entrySet()) {
      String tableName = tableName_tableId.getKey();
      String tableId = tableName_tableId.getValue();
      TableInfo tableInfo = tableStats.get(tableName);
      Double holdTime = compactingByTable.get(tableId);
      if (holdTime == null)
        holdTime = new Double(0.);
      TableRow row = tableList.prepareRow();
      row.add(tableId);
      row.add(tableManager.getTableState(tableId));
      row.add(tableInfo == null ? null : tableInfo.tablets);
      row.add(tableInfo == null ? null : tableInfo.tablets - tableInfo.onlineTablets);
      row.add(tableInfo == null ? null : tableInfo.recs);
      row.add(tableInfo == null ? null : tableInfo.recsInMemory);
      row.add(tableInfo == null ? null : tableInfo.ingestRate);
      row.add(tableInfo == null ? null : tableInfo.scanRate);
      row.add(tableInfo == null ? null : tableInfo.queryRate);
      row.add(holdTime.longValue());
      row.add(tableInfo);
      row.add(tableInfo);
      row.add(tableInfo);
      tableList.addRow(row);
    }

    tableList.generate(req, sb);
  }

  private void doTableDetails(HttpServletRequest req, StringBuilder sb, Map<String,String> tidToNameMap, String tableId) {
    String displayName = Tables.getPrintableTableNameFromId(tidToNameMap, tableId);
    Instance instance = HdfsZooInstance.getInstance();
    TreeSet<String> locs = new TreeSet<String>();
    if (RootTable.ID.equals(tableId)) {
      locs.add(instance.getRootTabletLocation());
    } else {
      String systemTableName = MetadataTable.ID.equals(tableId) ? RootTable.NAME : MetadataTable.NAME;
      MetaDataTableScanner scanner = new MetaDataTableScanner(instance, SystemCredentials.get(), new Range(KeyExtent.getMetadataEntry(new Text(tableId),
          new Text()), KeyExtent.getMetadataEntry(new Text(tableId), null)), systemTableName);

      while (scanner.hasNext()) {
        TabletLocationState state = scanner.next();
        if (state.current != null) {
          try {
            locs.add(state.current.hostPort());
          } catch (Exception ex) {
            log.error(ex, ex);
          }
        }
      }
      scanner.close();
    }

    log.debug("Locs: " + locs);

    List<TabletServerStatus> tservers = new ArrayList<TabletServerStatus>();
    if (Monitor.getMmi() != null) {
      for (TabletServerStatus tss : Monitor.getMmi().tServerInfo) {
        try {
          log.debug("tss: " + tss.name);
          if (tss.name != null && locs.contains(tss.name))
            tservers.add(tss);
        } catch (Exception ex) {
          log.error(ex, ex);
        }
      }
    }

    Table tableDetails = new Table("participatingTServers", "Participating&nbsp;Tablet&nbsp;Servers");
    tableDetails.setSubCaption(displayName);
    TServersServlet.doTserverList(req, sb, tservers, tableId, tableDetails);
  }
}
