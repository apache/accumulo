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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.util.Duration;
import org.apache.accumulo.core.util.NumUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.ZooKeeperStatus;
import org.apache.accumulo.monitor.ZooKeeperStatus.ZooKeeperState;
import org.apache.accumulo.monitor.util.celltypes.NumberType;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DefaultServlet extends BasicServlet {

  private static final long serialVersionUID = 1L;

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Accumulo Overview";
  }

  private void getResource(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    try {
      String path = req.getRequestURI();

      if (path.endsWith(".jpg"))
        resp.setContentType("image/jpeg");

      if (path.endsWith(".html"))
        resp.setContentType("text/html");

      path = path.substring(1);
      InputStream data = BasicServlet.class.getClassLoader().getResourceAsStream(path);
      ServletOutputStream out = resp.getOutputStream();
      try {
        if (data != null) {
          byte[] buffer = new byte[1024];
          int n;
          while ((n = data.read(buffer)) > 0)
            out.write(buffer, 0, n);
        } else {
          out.write(("could not get resource " + path + "").getBytes(UTF_8));
        }
      } finally {
        if (data != null)
          data.close();
      }
    } catch (Throwable t) {
      log.error(t, t);
      throw new IOException(t);
    }
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    if (req.getRequestURI().startsWith("/web"))
      getResource(req, resp);
    else if (req.getRequestURI().startsWith("/monitor"))
      resp.sendRedirect("/master");
    else if (req.getRequestURI().startsWith("/errors"))
      resp.sendRedirect("/problems");
    else
      super.doGet(req, resp);
  }

  public static final int GRAPH_WIDTH = 450;
  public static final int GRAPH_HEIGHT = 150;

  private static void plotData(StringBuilder sb, String title, @SuppressWarnings("rawtypes") List data, boolean points) {
    plotData(sb, title, points, new ArrayList<String>(), data);
  }

  @SuppressWarnings("rawtypes")
  private static void plotData(StringBuilder sb, String title, boolean points, List<String> labels, List... series) {
    sb.append("<div class=\"plotHeading\">");
    sb.append(title);
    sb.append("</div>");
    sb.append("</br>");
    String id = "c" + title.hashCode();
    sb.append("<div id=\"" + id + "\" style=\"width:" + GRAPH_WIDTH + "px;height:" + GRAPH_HEIGHT + "px;\"></div>\n");

    sb.append("<script type=\"text/javascript\">\n");
    sb.append("$(function () {\n");

    for (int i = 0; i < series.length; i++) {

      @SuppressWarnings("unchecked")
      List<Pair<Long,? extends Number>> data = series[i];
      sb.append("    var d" + i + " = [");

      String sep = "";
      for (Pair<Long,? extends Number> point : data) {
        if (point.getSecond() == null)
          continue;

        String y;
        if (point.getSecond() instanceof Double)
          y = String.format("%1.2f", point.getSecond());
        else
          y = point.getSecond().toString();

        sb.append(sep);
        sep = ",";
        sb.append("[" + utc2local(point.getFirst()) + "," + y + "]");
      }
      sb.append("    ];\n");
    }

    String opts = "lines: { show: true }";
    if (points)
      opts = "points: { show: true, radius: 1 }";

    sb.append("    $.plot($(\"#" + id + "\"),");
    String sep = "";

    String colors[] = new String[] {"red", "blue", "green", "black"};

    sb.append("[");
    for (int i = 0; i < series.length; i++) {
      sb.append(sep);
      sep = ",";
      sb.append("{ ");
      if (labels.size() > 0) {
        sb.append("label: \"" + labels.get(i) + "\", ");
      }
      sb.append("data: d" + i + ", " + opts + ", color:\"" + colors[i] + "\" }");
    }
    sb.append("], ");
    sb.append("{yaxis:{}, xaxis:{mode:\"time\",minTickSize: [1, \"minute\"],timeformat: \"%H:%M<br />" + getShortTZName() + "\", ticks:3}});");
    sb.append("   });\n");
    sb.append("</script>\n");
  }

  /**
   * Shows the current time zone (based on the current time) short name
   */
  private static String getShortTZName() {
    TimeZone tz = TimeZone.getDefault();
    return tz.getDisplayName(tz.inDaylightTime(new Date()), TimeZone.SHORT);
  }

  /**
   * Converts a unix timestamp in UTC to one that is relative to the local timezone
   */
  private static Long utc2local(Long utcMillis) {
    Calendar currentCalendar = Calendar.getInstance(); // default timezone
    currentCalendar.setTimeInMillis(utcMillis + currentCalendar.getTimeZone().getOffset(utcMillis));
    return currentCalendar.getTime().getTime();
  }

  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) throws IOException {

    sb.append("<table class='noborder'>\n");
    sb.append("<tr>\n");

    sb.append("<td class='noborder'>\n");
    doAccumuloTable(sb);
    sb.append("</td>\n");

    sb.append("<td class='noborder'>\n");
    doZooKeeperTable(sb);
    sb.append("</td>\n");

    sb.append("</tr></table>\n");
    sb.append("<br/>\n");

    sb.append("<p/><table class=\"noborder\">\n");

    sb.append("<tr><td>\n");
    plotData(sb, "Ingest (Entries/s)", Monitor.getIngestRateOverTime(), false);
    sb.append("</td><td>\n");
    plotData(sb, "Scan (Entries/s)", false, Arrays.asList("Read", "Returned"), Monitor.getScanRateOverTime(), Monitor.getQueryRateOverTime());
    sb.append("</td></tr>\n");

    sb.append("<tr><td>\n");
    plotData(sb, "Ingest (MB/s)", Monitor.getIngestByteRateOverTime(), false);
    sb.append("</td><td>\n");
    plotData(sb, "Scan (MB/s)", Monitor.getQueryByteRateOverTime(), false);
    sb.append("</td></tr>\n");

    sb.append("<tr><td>\n");
    plotData(sb, "Load Average", Monitor.getLoadOverTime(), false);
    sb.append("</td><td>\n");
    plotData(sb, "Seeks", Monitor.getLookupsOverTime(), false);
    sb.append("</td></tr>\n");

    sb.append("<tr><td>\n");
    plotData(sb, "Minor Compactions", Monitor.getMinorCompactionsOverTime(), false);
    sb.append("</td><td>\n");
    plotData(sb, "Major Compactions", Monitor.getMajorCompactionsOverTime(), false);
    sb.append("</td></tr>\n");

    sb.append("<tr><td>\n");
    plotData(sb, "Index Cache Hit Rate", Monitor.getIndexCacheHitRateOverTime(), true);
    sb.append("</td><td>\n");
    plotData(sb, "Data Cache Hit Rate", Monitor.getDataCacheHitRateOverTime(), true);
    sb.append("</td></tr>\n");

    sb.append("</table>\n");
  }

  private void doAccumuloTable(StringBuilder sb) throws IOException {
    // Accumulo
    VolumeManager vm = VolumeManagerImpl.get(ServerConfiguration.getSiteConfiguration());
    MasterMonitorInfo info = Monitor.getMmi();
    sb.append("<table>\n");
    sb.append("<tr><th colspan='2'><a href='/master'>Accumulo Master</a></th></tr>\n");
    if (info == null) {
      sb.append("<tr><td colspan='2'><span class='error'>Master is Down</span></td></tr>\n");
    } else {
      long totalAcuBytesUsed = 0l;
      long totalHdfsBytesUsed = 0l;

      try {
        for (String baseDir : VolumeConfiguration.getVolumeUris(ServerConfiguration.getSiteConfiguration())) {
          final Path basePath = new Path(baseDir);
          final FileSystem fs = vm.getVolumeByPath(basePath).getFileSystem();

          try {
            // Calculate the amount of space used by Accumulo on the FileSystem
            ContentSummary accumuloSummary = fs.getContentSummary(basePath);
            long bytesUsedByAcuOnFs = accumuloSummary.getSpaceConsumed();
            totalAcuBytesUsed += bytesUsedByAcuOnFs;

            // Catch the overflow -- this is big data
            if (totalAcuBytesUsed < bytesUsedByAcuOnFs) {
              log.debug("Overflowed long in bytes used by Accumulo for " + baseDir);
              totalAcuBytesUsed = 0l;
              break;
            }

            // Calculate the total amount of space used on the FileSystem
            ContentSummary volumeSummary = fs.getContentSummary(new Path("/"));
            long bytesUsedOnVolume = volumeSummary.getSpaceConsumed();
            totalHdfsBytesUsed += bytesUsedOnVolume;

            // Catch the overflow -- this is big data
            if (totalHdfsBytesUsed < bytesUsedOnVolume) {
              log.debug("Overflowed long in bytes used in HDFS for " + baseDir);
              totalHdfsBytesUsed = 0;
              break;
            }
          } catch (Exception ex) {
            log.trace("Unable to get disk usage information for " + baseDir, ex);
          }
        }

        String diskUsed = "Unknown";
        String consumed = null;
        if (totalAcuBytesUsed > 0) {
          // Convert Accumulo usage to a readable String
          diskUsed = bytes(totalAcuBytesUsed);

          if (totalHdfsBytesUsed > 0) {
            // Compute amount of space used by Accumulo as a percentage of total space usage.
            consumed = String.format("%.2f%%", totalAcuBytesUsed * 100. / totalHdfsBytesUsed);
          }
        }

        boolean highlight = false;
        tableRow(sb, (highlight = !highlight), "Disk&nbsp;Used", diskUsed);
        if (null != consumed)
          tableRow(sb, (highlight = !highlight), "%&nbsp;of&nbsp;Used&nbsp;DFS", consumed);
        tableRow(sb, (highlight = !highlight), "<a href='/tables'>Tables</a>", NumberType.commas(Monitor.getTotalTables()));
        tableRow(sb, (highlight = !highlight), "<a href='/tservers'>Tablet&nbsp;Servers</a>", NumberType.commas(info.tServerInfo.size(), 1, Long.MAX_VALUE));
        tableRow(sb, (highlight = !highlight), "<a href='/tservers'>Dead&nbsp;Tablet&nbsp;Servers</a>", NumberType.commas(info.deadTabletServers.size(), 0, 0));
        tableRow(sb, (highlight = !highlight), "Tablets", NumberType.commas(Monitor.getTotalTabletCount(), 1, Long.MAX_VALUE));
        tableRow(sb, (highlight = !highlight), "Entries", NumberType.commas(Monitor.getTotalEntries()));
        tableRow(sb, (highlight = !highlight), "Lookups", NumberType.commas(Monitor.getTotalLookups()));
        tableRow(sb, (highlight = !highlight), "Uptime", Duration.format(System.currentTimeMillis() - Monitor.getStartTime()));
      } catch (Exception e) {
        log.debug(e, e);
      }
    }
    sb.append("</table>\n");
  }

  private void doZooKeeperTable(StringBuilder sb) throws IOException {
    // Zookeepers
    sb.append("<table>\n");
    sb.append("<tr><th colspan='3'>Zookeeper</th></tr>\n");
    sb.append("<tr><th>Server</th><th>Mode</th><th>Clients</th></tr>\n");

    boolean highlight = false;
    for (ZooKeeperState k : ZooKeeperStatus.getZooKeeperStatus()) {
      if (k.clients >= 0) {
        tableRow(sb, (highlight = !highlight), k.keeper, k.mode, k.clients);
      } else {
        tableRow(sb, false, k.keeper, "<span class='error'>Down</span>", "");
      }
    }
    sb.append("</table>\n");
  }

  private static String bytes(long big) {
    return NumUtil.bigNumberForSize(big);
  }

  public static void tableRow(StringBuilder sb, boolean highlight, Object... cells) {
    sb.append(highlight ? "<tr class='highlight'>" : "<tr>");
    for (int i = 0; i < cells.length; ++i) {
      Object cell = cells[i];
      String cellValue = cell == null ? "" : String.valueOf(cell).trim();
      sb.append("<td class='").append(i < cells.length - 1 ? "left" : "right").append("'>").append(cellValue.isEmpty() ? "-" : cellValue).append("</td>");
    }
    sb.append("</tr>\n");
  }
}
