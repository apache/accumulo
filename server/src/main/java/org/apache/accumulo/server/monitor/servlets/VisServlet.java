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

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.server.monitor.Monitor;

public class VisServlet extends BasicServlet {
  private static final int concurrentScans = Monitor.getSystemConfiguration().getCount(Property.TSERV_READ_AHEAD_MAXCONCURRENT);
  
  private static final long serialVersionUID = 1L;
  boolean useCircles;
  StatType motion;
  StatType color;
  int spacing;
  String url;
  
  public enum StatType {
    osload(ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors(), true, 100, "OS Load"),
    ingest(1000, true, 1, "Ingest Entries"),
    query(10000, true, 1, "Scan Entries"),
    ingestMB(10, true, 10, "Ingest MB"),
    queryMB(5, true, 10, "Scan MB"),
    scans(concurrentScans * 2, false, 1, "Running Scans"),
    scansessions(50, true, 10, "Scan Sessions"),
    holdtime(60000, false, 1, "Hold Time"),
    allavg(1, false, 100, "Overall Avg", true),
    allmax(1, false, 100, "Overall Max", true);
    
    private int max;
    private boolean adjustMax;
    private float significance;
    private String description;
    private boolean derived;
    
    /**
     * @param max
     *          initial estimate of largest possible value for this stat
     * @param adjustMax
     *          indicates whether max should be adjusted based on observed values
     * @param significance
     *          values will be converted by floor(significance*value)/significance
     * @param description
     *          as appears in selection box
     */
    private StatType(int max, boolean adjustMax, float significance, String description) {
      this(max, adjustMax, significance, description, false);
    }
    
    private StatType(int max, boolean adjustMax, float significance, String description, boolean derived) {
      this.max = max;
      this.adjustMax = adjustMax;
      this.significance = significance;
      this.description = description;
      this.derived = derived;
    }
    
    public int getMax() {
      return max;
    }
    
    public boolean getAdjustMax() {
      return adjustMax;
    }
    
    public float getSignificance() {
      return significance;
    }
    
    public String getDescription() {
      return description;
    }
    
    public boolean isDerived() {
      return derived;
    }
    
    public static int numDerived() {
      int count = 0;
      for (StatType st : StatType.values())
        if (st.isDerived())
          count++;
      return count;
    }
  }
  
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
    String s = req.getParameter("shape");
    if (s != null && (s.equals("square") || s.equals("squares"))) {
      useCircles = false;
    }
    
    s = req.getParameter("motion");
    motion = StatType.allmax;
    if (s != null) {
      try {
        motion = StatType.valueOf(s);
      } catch (Exception e) {}
    }
    
    s = req.getParameter("color");
    color = StatType.allavg;
    if (s != null) {
      try {
        color = StatType.valueOf(s);
      } catch (Exception e) {}
    }
    
    spacing = 40;
    String size = req.getParameter("size");
    if (size != null) {
      if (size.equals("10"))
        spacing = 10;
      else if (size.equals("20"))
        spacing = 20;
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
    sb.append("&nbsp;&nbsp<span class='viscontrol'>Motion: <select id='motion' onchange='setMotion(this)'>");
    addOptions(sb, motion);
    sb.append("</select></span>\n");
    // color select box
    sb.append("&nbsp;&nbsp<span class='viscontrol'>Color: <select id='color' onchange='setColor(this)'>");
    addOptions(sb, color);
    sb.append("</select></span>\n");
    sb.append("&nbsp;&nbsp<span class='viscontrol'>(hover for info, click for details)</span>");
    sb.append("</div>\n\n");
    sb.append("<div id='hoverable'>\n");
    // floating info box
    sb.append("<div id='vishoverinfo'></div>\n\n");
    // canvas
    sb.append("<br><canvas id='visCanvas' width='").append(width).append("' height='").append(height).append("'>Browser does not support canvas.</canvas>\n\n");
    sb.append("</div>\n");
    sb.append("</div>\n\n");
  }
  
  private void addOptions(StringBuilder sb, StatType selectedStatType) {
    for (StatType st : StatType.values()) {
      sb.append("<option").append(st.equals(selectedStatType) ? " selected='true'>" : ">").append(st.getDescription()).append("</option>");
    }
  }
  
  private void doScript(StringBuilder sb, ArrayList<TabletServerStatus> tservers) {
    // initialization of some javascript variables
    sb.append("<script type='text/javascript'>\n");
    sb.append("var numCores = " + ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors() + ";\n");
    sb.append("var jsonurl = '" + url + "json';\n");
    sb.append("var visurl = '" + url + "vis';\n");
    sb.append("var serverurl = '" + url + "tservers?s=';\n\n");
    sb.append("// observable stats that can be connected to motion or color\n");
    sb.append("var statNames = {");
    for (StatType st : StatType.values())
      sb.append("'").append(st).append("': ").append(st.derived).append(",");
    sb.setLength(sb.length() - 1);
    sb.append("};\n");
    sb.append("var maxStatValues = {");
    for (StatType st : StatType.values())
      sb.append("'").append(st).append("': ").append(st.getMax()).append(", ");
    sb.setLength(sb.length() - 2);
    sb.append("}; // initial values that are system-dependent may increase based on observed values\n");
    sb.append("var adjustMax = {");
    for (StatType st : StatType.values())
      sb.append("'").append(st).append("': ").append(st.getAdjustMax()).append(", ");
    sb.setLength(sb.length() - 2);
    sb.append("}; // whether to allow increases in the max based on observed values\n");
    sb.append("var significance = {");
    for (StatType st : StatType.values())
      sb.append("'").append(st).append("': ").append(st.getSignificance()).append(", ");
    sb.setLength(sb.length() - 2);
    sb.append("}; // values will be converted by floor(this*value)/this\n");
    sb.append("var numNormalStats = ").append(StatType.values().length - StatType.numDerived()).append(";\n");
    sb.append("</script>\n");
    
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
