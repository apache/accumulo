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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.util.Table;
import org.apache.accumulo.monitor.util.TableRow;
import org.apache.accumulo.monitor.util.celltypes.DurationType;
import org.apache.accumulo.monitor.util.celltypes.NumberType;
import org.apache.accumulo.monitor.util.celltypes.PreciseNumberType;
import org.apache.accumulo.monitor.util.celltypes.ProgressChartType;
import org.apache.accumulo.monitor.util.celltypes.StringType;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.monitor.DedupedLogEvent;
import org.apache.accumulo.server.monitor.LogService;
import org.apache.log4j.Level;

public class MasterServlet extends BasicServlet {

  private static final long serialVersionUID = 1L;

  @Override
  protected String getTitle(HttpServletRequest req) {
    List<String> masters = Monitor.getInstance().getMasterLocations();
    return "Master Server" + (masters.size() == 0 ? "" : ":" + AddressUtil.parseAddress(masters.get(0), false).getHostText());
  }

  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse response, StringBuilder sb) throws IOException {
    Map<String,String> tidToNameMap = Tables.getIdToNameMap(HdfsZooInstance.getInstance());

    doLogEventBanner(sb);
    TablesServlet.doProblemsBanner(sb);
    doMasterStatus(req, sb);
    doRecoveryList(req, sb);
    TablesServlet.doTableList(req, sb, tidToNameMap);
  }

  private void doLogEventBanner(StringBuilder sb) {
    if (LogService.getInstance().getEvents().size() > 0) {
      int error = 0, warning = 0, total = 0;
      for (DedupedLogEvent dev : LogService.getInstance().getEvents()) {
        switch (dev.getEvent().getLevel().toInt()) {
          case Level.FATAL_INT:
          case Level.ERROR_INT:
            error++;
            break;
          case Level.WARN_INT:
            warning++;
            break;
        }
        total++;
      }
      banner(sb, error > 0 ? "error" : "warning", String.format("<a href='/log'>Log Events: %d Error%s, %d Warning%s, %d Total</a>", error, error == 1 ? ""
          : "s", warning, warning == 1 ? "" : "s", total));
    }
  }

  private void doMasterStatus(HttpServletRequest req, StringBuilder sb) throws IOException {

    if (Monitor.getMmi() != null) {
      String gcStatus = "Waiting";
      if (Monitor.getGcStatus() != null) {
        long start = 0;
        String label = "";
        if (Monitor.getGcStatus().current.started != 0 || Monitor.getGcStatus().currentLog.started != 0) {
          start = Math.max(Monitor.getGcStatus().current.started, Monitor.getGcStatus().currentLog.started);
          label = "Running";
        } else if (Monitor.getGcStatus().lastLog.finished != 0) {
          start = Monitor.getGcStatus().lastLog.finished;
        }
        if (start != 0) {
          long diff = System.currentTimeMillis() - start;
          gcStatus = label + " " + DateFormat.getInstance().format(new Date(start));
          gcStatus = gcStatus.replace(" ", "&nbsp;");
          long normalDelay = Monitor.getSystemConfiguration().getTimeInMillis(Property.GC_CYCLE_DELAY);
          if (diff > normalDelay * 2)
            gcStatus = "<span class='warning'>" + gcStatus + "</span>";
        }
      } else {
        gcStatus = "<span class='error'>Down</span>";
      }
      if (Monitor.getMmi().state != MasterState.NORMAL) {
        sb.append("<span class='warning'>Master State: " + Monitor.getMmi().state.name() + " Goal: " + Monitor.getMmi().goalState.name() + "</span>\n");
      }
      if (Monitor.getMmi().serversShuttingDown != null && Monitor.getMmi().serversShuttingDown.size() > 0 && Monitor.getMmi().state == MasterState.NORMAL) {
        sb.append("<span class='warning'>Servers being stopped: " + StringUtil.join(Monitor.getMmi().serversShuttingDown, ", ") + "</span>\n");
      }

      int guessHighLoad = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
      List<String> slaves = new ArrayList<String>();
      for (TabletServerStatus up : Monitor.getMmi().tServerInfo) {
        slaves.add(up.name);
      }
      for (DeadServer down : Monitor.getMmi().deadTabletServers) {
        slaves.add(down.server);
      }
      List<String> masters = Monitor.getInstance().getMasterLocations();

      Table masterStatus = new Table("masterStatus", "Master&nbsp;Status");
      masterStatus.addSortableColumn("Master", new StringType<String>(), "The hostname of the master server");
      masterStatus.addSortableColumn("#&nbsp;Online<br />Tablet&nbsp;Servers", new PreciseNumberType((int) (slaves.size() * 0.8 + 1.0), slaves.size(),
          (int) (slaves.size() * 0.6 + 1.0), slaves.size()), "Number of tablet servers currently available");
      masterStatus.addSortableColumn("#&nbsp;Total<br />Tablet&nbsp;Servers", new PreciseNumberType(), "The total number of tablet servers configured");
      masterStatus.addSortableColumn("Last&nbsp;GC", null, "The last time files were cleaned-up from HDFS.");
      masterStatus.addSortableColumn("#&nbsp;Tablets", new NumberType<Integer>(0, Integer.MAX_VALUE, 2, Integer.MAX_VALUE), null);
      masterStatus.addSortableColumn("#&nbsp;Unassigned<br />Tablets", new NumberType<Integer>(0, 0), null);
      masterStatus.addSortableColumn("Entries", new NumberType<Long>(), "The total number of key/value pairs in Accumulo");
      masterStatus.addSortableColumn("Ingest", new NumberType<Long>(), "The number of Key/Value pairs inserted, per second. "
          + " Note that deleted records are \"inserted\" and will make the ingest " + "rate increase in the near-term.");
      masterStatus.addSortableColumn("Entries<br />Read", new NumberType<Long>(),
          "The total number of Key/Value pairs read on the server side.  Not all may be returned because of filtering.");
      masterStatus.addSortableColumn("Entries<br />Returned", new NumberType<Long>(), "The total number of Key/Value pairs returned as a result of scans.");
      masterStatus.addSortableColumn("Hold&nbsp;Time", new DurationType(0l, 0l), "The maximum amount of time that ingest has been held "
          + "across all servers due to a lack of memory to store the records");
      masterStatus.addSortableColumn("OS&nbsp;Load", new NumberType<Double>(0., guessHighLoad * 1., 0., guessHighLoad * 3.),
          "The one-minute load average on the computer that runs the monitor web server.");
      TableRow row = masterStatus.prepareRow();
      row.add(masters.size() == 0 ? "<div class='error'>Down</div>" : AddressUtil.parseAddress(masters.get(0), false).getHostText());
      row.add(Monitor.getMmi().tServerInfo.size());
      row.add(slaves.size());
      row.add("<a href='/gc'>" + gcStatus + "</a>");
      row.add(Monitor.getTotalTabletCount());
      row.add(Monitor.getMmi().unassignedTablets);
      row.add(Monitor.getTotalEntries());
      row.add(Math.round(Monitor.getTotalIngestRate()));
      row.add(Math.round(Monitor.getTotalScanRate()));
      row.add(Math.round(Monitor.getTotalQueryRate()));
      row.add(Monitor.getTotalHoldTime());
      row.add(ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage());
      masterStatus.addRow(row);
      masterStatus.generate(req, sb);

    } else
      banner(sb, "error", "Master Server Not Running");
  }

  private void doRecoveryList(HttpServletRequest req, StringBuilder sb) {
    MasterMonitorInfo mmi = Monitor.getMmi();
    if (mmi != null) {
      Table recoveryTable = new Table("logRecovery", "Log&nbsp;Recovery");
      recoveryTable.setSubCaption("Some tablets were unloaded in an unsafe manner. Write-ahead logs are being recovered.");
      recoveryTable.addSortableColumn("Server");
      recoveryTable.addSortableColumn("Log");
      recoveryTable.addSortableColumn("Time", new DurationType(), null);
      recoveryTable.addSortableColumn("Copy/Sort", new ProgressChartType(), null);
      int rows = 0;
      for (TabletServerStatus server : mmi.tServerInfo) {
        if (server.logSorts != null) {
          for (RecoveryStatus recovery : server.logSorts) {
            TableRow row = recoveryTable.prepareRow();
            row.add(AddressUtil.parseAddress(server.name, false).getHostText());
            row.add(recovery.name);
            row.add((long) recovery.runtime);
            row.add(recovery.progress);
            recoveryTable.addRow(row);
            rows++;
          }
        }
      }
      if (rows > 0)
        recoveryTable.generate(req, sb);
    }
  }

}
