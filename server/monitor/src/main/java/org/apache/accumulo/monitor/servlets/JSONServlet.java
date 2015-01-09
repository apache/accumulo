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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.util.celltypes.TServerLinkType;
import org.apache.accumulo.server.util.TableInfoUtil;

import com.google.gson.Gson;

public class JSONServlet extends BasicServlet {
  private static final long serialVersionUID = 1L;

  private Gson gson = new Gson();

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "JSON Report";
  }

  @Override
  protected void pageStart(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
    resp.setContentType("application/json");
  }

  private static Map<String,Object> addServer(String ip, String hostname, double osload, double ingest, double query, double ingestMB, double queryMB,
      int scans, double scansessions, long holdtime) {
    Map<String,Object> map = new HashMap<String,Object>();
    map.put("ip", ip);
    map.put("hostname", hostname);
    map.put("osload", osload);
    map.put("ingest", ingest);
    map.put("query", query);
    map.put("ingestMB", ingestMB);
    map.put("queryMB", queryMB);
    map.put("scans", scans);
    map.put("scanssessions", scansessions);
    map.put("holdtime", holdtime);
    return map;
  }

  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
    if (Monitor.getMmi() == null || Monitor.getMmi().tableMap == null) {
      return;
    }

    Map<String,Object> results = new HashMap<String,Object>();
    List<Map<String,Object>> servers = new ArrayList<Map<String,Object>>();

    for (TabletServerStatus status : Monitor.getMmi().tServerInfo) {
      TableInfo summary = TableInfoUtil.summarizeTableStats(status);
      servers.add(addServer(status.name, TServerLinkType.displayName(status.name), status.osLoad, summary.ingestRate, summary.queryRate,
          summary.ingestByteRate / 1000000.0, summary.queryByteRate / 1000000.0, summary.scans.running + summary.scans.queued, Monitor.getLookupRate(),
          status.holdTime));
    }

    for (Entry<String,Byte> entry : Monitor.getMmi().badTServers.entrySet()) {
      Map<String,Object> badServer = new HashMap<String,Object>();
      badServer.put("ip", entry.getKey());
      badServer.put("bad", true);
      servers.add(badServer);
    }

    for (DeadServer dead : Monitor.getMmi().deadTabletServers) {
      Map<String,Object> deadServer = new HashMap<String,Object>();
      deadServer.put("ip", dead.server);
      deadServer.put("dead", true);
      servers.add(deadServer);
    }

    results.put("servers", servers);
    sb.append(gson.toJson(results));
  }

  @Override
  protected void pageEnd(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {}
}
