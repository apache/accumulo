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
package org.apache.accumulo.server.monitor.servlets;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.server.monitor.Monitor;

public class XMLServlet extends BasicServlet {
  private static final long serialVersionUID = 1L;
  
  @Override
  protected String getTitle(HttpServletRequest req) {
    return "XML Report";
  }
  
  @Override
  protected void pageStart(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
    resp.setContentType("text/xml");
    sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    sb.append("<stats>\n");
  }
  
  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
    double totalIngest = 0.;
    double totalQuery = 0.;
    double disk = 0.0;
    long totalEntries = 0L;
    
    sb.append("\n<servers>\n");
    SortedMap<String,TableInfo> tableStats = new TreeMap<String,TableInfo>(Monitor.getMmi().tableMap);
    
    for (TabletServerStatus status : Monitor.getMmi().tServerInfo) {
      
      sb.append("\n<server id='").append(status.name).append("'>\n");
      sb.append("<lastContact>").append(System.currentTimeMillis() - status.lastContact).append("</lastContact>\n");
      
      TableInfo summary = Monitor.summarizeTableStats(status);
      sb.append("<compactions>\n");
      sb.append("<major>").append("<running>").append(summary.major.running).append("</running>").append("<queued>").append(summary.major.queued)
          .append("</queued>").append("</major>\n");
      sb.append("<minor>").append("<running>").append(summary.minor.running).append("</running>").append("<queued>").append(summary.minor.queued)
          .append("</queued>").append("</minor>\n");
      sb.append("</compactions>\n");
      
      sb.append("<tablets>").append(summary.tablets).append("</tablets>\n");
      if (status.loggers != null) {
        sb.append("<loggers>");
        for (String logger : status.loggers)
          sb.append("<logger>" + logger + "</logger>");
        sb.append("</loggers>");
      }
      
      totalIngest += summary.ingestRate;
      totalQuery += summary.queryRate;
      totalEntries += summary.recs;
      sb.append("</server>\n");
    }
    sb.append("\n</servers>\n");
    
    sb.append("\n<tables>\n");
    for (Entry<String,TableInfo> entry : tableStats.entrySet()) {
      TableInfo tableInfo = entry.getValue();
      
      sb.append("\n<table>\n");
      sb.append("<tablename>").append(entry.getKey()).append("</tablename>\n");
      sb.append("<tablets>").append(tableInfo.tablets).append("</tablets>\n");
      sb.append("<onlineTablets>").append(tableInfo.onlineTablets).append("</onlineTablets>\n");
      sb.append("<recs>").append(tableInfo.recs).append("</recs>\n");
      sb.append("<recsInMemory>").append(tableInfo.recsInMemory).append("</recsInMemory>\n");
      sb.append("<ingest>").append(tableInfo.ingestRate).append("</ingest>\n");
      sb.append("<query>").append(tableInfo.queryRate).append("</query>\n");
      int running = 0;
      int queued = 0;
      Compacting compacting = entry.getValue().major;
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
