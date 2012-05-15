/**
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
package org.apache.accumulo.server.monitor.servlets;

import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.server.monitor.Monitor;
import org.apache.accumulo.server.monitor.util.celltypes.TServerLinkType;

public class JSONServlet extends BasicServlet {
  private static final long serialVersionUID = 1L;
  
  @Override
  protected String getTitle(HttpServletRequest req) {
    return "JSON Report";
  }
  
  @Override
  protected void pageStart(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
    resp.setContentType("application/json");
    sb.append("{ 'servers': [\n");
  }
  
  private static void addServerLine(StringBuilder sb, String ip, String hostname, double osload, double ingest, double query, double ingestMB, double queryMB,
      int scans, double scansessions, long holdtime) {
    sb.append("  {'ip': '").append(ip).append("',\n  'hostname': '").append(hostname).append("',\n  'osload': ").append(osload).append(",\n  'ingest': ")
        .append(ingest).append(",\n  'query': ").append(query).append(",\n  'ingestMB': ").append(ingestMB).append(",\n  'queryMB': ").append(queryMB)
        .append(",\n  'scans': ").append(scans).append(",\n  'scansessions': ").append(scansessions).append(",\n  'holdtime': ").append(holdtime)
        .append("},\n");
  }
  
  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
    if (Monitor.getMmi() == null || Monitor.getMmi().tableMap == null) {
      return;
    }
    
    for (TabletServerStatus status : Monitor.getMmi().tServerInfo) {
      TableInfo summary = Monitor.summarizeTableStats(status);
      addServerLine(sb, status.name, TServerLinkType.displayName(status.name), status.osLoad, summary.ingestRate, summary.queryRate,
          summary.ingestByteRate / 1000000.0, summary.queryByteRate / 1000000.0, summary.scans.running + summary.scans.queued, Monitor.getLookupRate(),
          status.holdTime);
    }
    
    for (Entry<String,Byte> entry : Monitor.getMmi().badTServers.entrySet()) {
      sb.append("  {'ip': '").append(entry.getKey()).append("',\n  'bad':true},\n");
    }
    
    for (DeadServer dead : Monitor.getMmi().deadTabletServers) {
      sb.append("  {'ip': '").append(dead.server).append("',\n  'dead': true},\n");
    }
    if (Monitor.getMmi().tServerInfo.size() > 0 || Monitor.getMmi().badTServers.size() > 0 || Monitor.getMmi().deadTabletServers.size() > 0)
      sb.setLength(sb.length() - 2);
  }
  
  @Override
  protected void pageEnd(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
    sb.append("\n  ]\n}\n");
  }
}
