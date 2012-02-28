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

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.server.monitor.Monitor;

public class VisServlet extends BasicServlet {
  private static final long serialVersionUID = 1L;
  boolean useCircles;
  boolean useIngest;
  int spacing;
  String url;
  
  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Server Activity";
  }
  
  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse response, StringBuilder sb) throws IOException {
    StringBuffer urlsb = req.getRequestURL();
    urlsb.setLength(urlsb.lastIndexOf("/") + 1);
    url = urlsb.toString();
    
    useCircles = true;
    String shape = req.getParameter("shape");
    if (shape != null && (shape.equals("square") || shape.equals("squares"))) {
      useCircles = false;
    }
    
    useIngest = true;
    String motion = req.getParameter("motion");
    if (motion != null && (motion.equals("query"))) {
      useIngest = false;
    }
    
    spacing = 20;
    String size = req.getParameter("size");
    if (size != null) {
      if (size.equals("10"))
        spacing = 10;
      else if (size.equals("40"))
        spacing = 40;
      else if (size.equals("80"))
        spacing = 80;
    }
    
    ArrayList<TabletServerStatus> tservers = new ArrayList<TabletServerStatus>();
    if (Monitor.getMmi() != null)
      tservers.addAll(Monitor.getMmi().tServerInfo);
    
    if (tservers.size() == 0)
      return;
    
    int width = (int) Math.ceil(Math.sqrt(tservers.size())) * spacing;
    int height = (int) Math.ceil(tservers.size() / width) * spacing;
    doSettings(sb, width < 640 ? 640 : width, height < 640 ? 640 : height);
    doScript(sb, tservers);
  }
  
  private void doSettings(StringBuilder sb, int width, int height) {
    sb.append("<div class='left'>\n");
    sb.append("<div id='parameters' class='nowrap'>\n");
    // shape select box
    sb.append("<span class='viscontrol'>Shape: <select id='shape' onchange='setShape(this)'><option>Circles</option><option")
        .append(!useCircles ? " selected='true'" : "").append(">Squares</option></select></span>\n");
    // size select box
    sb.append("&nbsp;&nbsp<span class='viscontrol'>Size: <select id='size' onchange='setSize(this)'><option").append(spacing == 10 ? " selected='true'" : "")
        .append(">10</option><option").append(spacing == 20 ? " selected='true'" : "").append(">20</option><option")
        .append(spacing == 40 ? " selected='true'" : "").append(">40</option><option").append(spacing == 80 ? " selected='true'" : "")
        .append(">80</option></select></span>\n");
    // motion select box
    sb.append("&nbsp;&nbsp<span class='viscontrol'>Motion: <select id='motion' onchange='setMotion(this)'><option>Ingest</option><option")
        .append(!useIngest ? " selected='true'" : "").append(">Query</option></select></span>\n");
    // color select box
    sb.append("&nbsp;&nbsp<span class='viscontrol'>Color: <select><option>OS Load</option></select></span>\n");
    sb.append("&nbsp;&nbsp<span class='viscontrol'>(hover for info, click for details)</span>");
    sb.append("</div>\n\n");
    // floating info box
    sb.append("<div id='vishoverinfo'></div>\n\n");
    // canvas
    sb.append("<br><canvas id='visCanvas' width='").append(width).append("' height='").append(height).append("'>Browser does not support canvas.</canvas>\n\n");
    sb.append("</div>\n\n");
    // initialization of some javascript variables
    sb.append("<script type='text/javascript'>\n");
    sb.append("var numCores = " + ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors() + ";\n");
    sb.append("var xmlurl = '" + url + "xml';\n");
    sb.append("var visurl = '" + url + "vis';\n");
    sb.append("var serverurl = '" + url + "tservers?s=';\n");
    sb.append("</script>\n");
  }
  
  private void doScript(StringBuilder sb, ArrayList<TabletServerStatus> tservers) {
    InputStream data = VisServlet.class.getClassLoader().getResourceAsStream("web/vis.xml");
    if (data != null) {
      byte[] buffer = new byte[1024];
      int n;
      try {
        while ((n = data.read(buffer)) > 0)
          sb.append(new String(buffer, 0, n));
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }
    }
    sb.append("\n");
  }
}
