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

import static com.google.common.base.Charsets.UTF_8;

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.util.celltypes.TServerLinkType;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.master.state.TabletServerState;
import org.apache.accumulo.server.util.TableInfoUtil;

public class XMLServlet extends BasicServlet {
  private static final long serialVersionUID = 1L;

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "XML Report";
  }

  @Override
  protected void pageStart(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
    resp.setContentType("text/xml;charset=" + UTF_8.name());
    sb.append("<?xml version=\"1.0\" encoding=\"" + UTF_8.name() + "\"?>\n");
    sb.append("<stats>\n");
  }

  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
    double totalIngest = 0.;
    double totalQuery = 0.;
    double disk = 0.0;
    long totalEntries = 0L;

    sb.append("\n<servers>\n");
    if (Monitor.getMmi() == null || Monitor.getMmi().tableMap == null) {
      sb.append("</servers>\n");
      return;
    }
    SortedMap<String,TableInfo> tableStats = new TreeMap<String,TableInfo>(Monitor.getMmi().tableMap);

    for (TabletServerStatus status : Monitor.getMmi().tServerInfo) {

      sb.append("\n<server id='").append(status.name).append("'>\n");
      sb.append("<hostname>").append(TServerLinkType.displayName(status.name)).append("</hostname>");
      sb.append("<lastContact>").append(System.currentTimeMillis() - status.lastContact).append("</lastContact>\n");
      sb.append("<osload>").append(status.osLoad).append("</osload>\n");

      TableInfo summary = TableInfoUtil.summarizeTableStats(status);
      sb.append("<compactions>\n");
      sb.append("<major>").append("<running>").append(summary.majors.running).append("</running>").append("<queued>").append(summary.majors.queued)
          .append("</queued>").append("</major>\n");
      sb.append("<minor>").append("<running>").append(summary.minors.running).append("</running>").append("<queued>").append(summary.minors.queued)
          .append("</queued>").append("</minor>\n");
      sb.append("</compactions>\n");

      sb.append("<tablets>").append(summary.tablets).append("</tablets>\n");

      sb.append("<ingest>").append(summary.ingestRate).append("</ingest>\n");
      sb.append("<query>").append(summary.queryRate).append("</query>\n");
      sb.append("<ingestMB>").append(summary.ingestByteRate / 1000000.0).append("</ingestMB>\n");
      sb.append("<queryMB>").append(summary.queryByteRate / 1000000.0).append("</queryMB>\n");
      sb.append("<scans>").append(summary.scans.running + summary.scans.queued).append("</scans>");
      sb.append("<scansessions>").append(Monitor.getLookupRate()).append("</scansessions>\n");
      sb.append("<holdtime>").append(status.holdTime).append("</holdtime>\n");
      totalIngest += summary.ingestRate;
      totalQuery += summary.queryRate;
      totalEntries += summary.recs;
      sb.append("</server>\n");
    }
    sb.append("\n</servers>\n");

    sb.append("\n<masterGoalState>" + Monitor.getMmi().goalState + "</masterGoalState>\n");
    sb.append("\n<masterState>" + Monitor.getMmi().state + "</masterState>\n");

    sb.append("\n<badTabletServers>\n");
    for (Entry<String,Byte> entry : Monitor.getMmi().badTServers.entrySet()) {
      sb.append(String.format("<badTabletServer id='%s' status='%s'/>\n", entry.getKey(), TabletServerState.getStateById(entry.getValue())));
    }
    sb.append("\n</badTabletServers>\n");

    sb.append("\n<tabletServersShuttingDown>\n");
    for (String server : Monitor.getMmi().serversShuttingDown) {
      sb.append(String.format("<server id='%s'/>\n", server));
    }
    sb.append("\n</tabletServersShuttingDown>\n");

    sb.append(String.format("\n<unassignedTablets>%d</unassignedTablets>\n", Monitor.getMmi().unassignedTablets));

    sb.append("\n<deadTabletServers>\n");
    for (DeadServer dead : Monitor.getMmi().deadTabletServers) {
      sb.append(String.format("<deadTabletServer id='%s' lastChange='%d' status='%s'/>\n", dead.server, dead.lastStatus, dead.status));
    }
    sb.append("\n</deadTabletServers>\n");

    sb.append("\n<deadLoggers>\n");
    for (DeadServer dead : Monitor.getMmi().deadTabletServers) {
      sb.append(String.format("<deadLogger id='%s' lastChange='%d' status='%s'/>\n", dead.server, dead.lastStatus, dead.status));
    }
    sb.append("\n</deadLoggers>\n");

    sb.append("\n<tables>\n");
    Instance instance = HdfsZooInstance.getInstance();
    for (Entry<String,TableInfo> entry : tableStats.entrySet()) {
      TableInfo tableInfo = entry.getValue();

      sb.append("\n<table>\n");
      String tableId = entry.getKey();
      String tableName = "unknown";
      String tableState = "unknown";
      try {
        tableName = Tables.getTableName(instance, tableId);
        tableState = Tables.getTableState(instance, tableId).toString();
      } catch (Exception ex) {
        log.warn(ex, ex);
      }
      sb.append("<tablename>").append(tableName).append("</tablename>\n");
      sb.append("<tableId>").append(tableId).append("</tableId>\n");
      sb.append("<tableState>").append(tableState).append("</tableState>\n");
      sb.append("<tablets>").append(tableInfo.tablets).append("</tablets>\n");
      sb.append("<onlineTablets>").append(tableInfo.onlineTablets).append("</onlineTablets>\n");
      sb.append("<recs>").append(tableInfo.recs).append("</recs>\n");
      sb.append("<recsInMemory>").append(tableInfo.recsInMemory).append("</recsInMemory>\n");
      sb.append("<ingest>").append(tableInfo.ingestRate).append("</ingest>\n");
      sb.append("<ingestByteRate>").append(tableInfo.ingestByteRate).append("</ingestByteRate>\n");
      sb.append("<query>").append(tableInfo.queryRate).append("</query>\n");
      sb.append("<queryByteRate>").append(tableInfo.queryRate).append("</queryByteRate>\n");
      int running = 0;
      int queued = 0;
      Compacting compacting = entry.getValue().majors;
      if (compacting != null) {
        running = compacting.running;
        queued = compacting.queued;
      }
      sb.append("<majorCompactions>").append("<running>").append(running).append("</running>").append("<queued>").append(queued).append("</queued>")
          .append("</majorCompactions>\n");
      sb.append("</table>\n");
    }
    sb.append("\n</tables>\n");

    sb.append("\n<totals>\n");
    sb.append("<ingestrate>").append(totalIngest).append("</ingestrate>\n");
    sb.append("<queryrate>").append(totalQuery).append("</queryrate>\n");
    sb.append("<diskrate>").append(disk).append("</diskrate>\n");
    sb.append("<numentries>").append(totalEntries).append("</numentries>\n");
    sb.append("</totals>\n");
  }

  @Override
  protected void pageEnd(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
    sb.append("\n</stats>\n");
  }
}
