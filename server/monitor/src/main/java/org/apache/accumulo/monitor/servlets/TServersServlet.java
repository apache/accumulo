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

import java.lang.management.ManagementFactory;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.tabletserver.thrift.ActionStats;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.core.util.Duration;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.util.Table;
import org.apache.accumulo.monitor.util.TableRow;
import org.apache.accumulo.monitor.util.celltypes.CompactionsType;
import org.apache.accumulo.monitor.util.celltypes.DateTimeType;
import org.apache.accumulo.monitor.util.celltypes.DurationType;
import org.apache.accumulo.monitor.util.celltypes.NumberType;
import org.apache.accumulo.monitor.util.celltypes.PercentageType;
import org.apache.accumulo.monitor.util.celltypes.ProgressChartType;
import org.apache.accumulo.monitor.util.celltypes.TServerLinkType;
import org.apache.accumulo.monitor.util.celltypes.TableLinkType;
import org.apache.accumulo.server.master.state.TabletServerState;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.ActionStatsUpdator;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.apache.accumulo.trace.instrument.Tracer;

import com.google.common.net.HostAndPort;

public class TServersServlet extends BasicServlet {

  private static final long serialVersionUID = 1L;
  private static final TabletServerStatus NO_STATUS = new TabletServerStatus();

  static class SecondType extends NumberType<Double> {

    @Override
    public String format(Object obj) {
      if (obj == null)
        return "&mdash;";
      return Duration.format((long) (1000.0 * (Double) obj));
    }

  }

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Tablet Server Status";
  }

  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse response, StringBuilder sb) throws Exception {
    String tserverAddress = req.getParameter("s");

    // Check to make sure tserver is a known address
    boolean tserverExists = false;
    if (tserverAddress != null && tserverAddress.isEmpty() == false) {
      for (TabletServerStatus ts : Monitor.getMmi().getTServerInfo()) {
        if (tserverAddress.equals(ts.getName())) {
          tserverExists = true;
          break;
        }
      }
    }

    if (tserverAddress == null || tserverAddress.isEmpty() || tserverExists == false) {
      doBadTserverList(req, sb);

      doDeadTserverList(req, sb);

      ArrayList<TabletServerStatus> tservers = new ArrayList<TabletServerStatus>();
      if (Monitor.getMmi() != null)
        tservers.addAll(Monitor.getMmi().tServerInfo);

      Table tServerList = new Table("tservers", "Tablet&nbsp;Servers");
      tServerList.setSubCaption("Click on the <span style='color: #0000ff;'>server address</span> to view detailed performance statistics for that server.");

      doTserverList(req, sb, tservers, null, tServerList);
      return;
    }

    double totalElapsedForAll = 0;
    double splitStdDev = 0;
    double minorStdDev = 0;
    double minorQueueStdDev = 0;
    double majorStdDev = 0;
    double majorQueueStdDev = 0;
    double currentMinorAvg = 0;
    double currentMajorAvg = 0;
    double currentMinorStdDev = 0;
    double currentMajorStdDev = 0;
    TabletStats total = new TabletStats(null, new ActionStats(), new ActionStats(), new ActionStats(), 0, 0, 0, 0);

    HostAndPort address = HostAndPort.fromString(tserverAddress);
    TabletStats historical = new TabletStats(null, new ActionStats(), new ActionStats(), new ActionStats(), 0, 0, 0, 0);
    List<TabletStats> tsStats = new ArrayList<TabletStats>();
    try {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, Monitor.getSystemConfiguration());
      try {
        for (String tableId : Monitor.getMmi().tableMap.keySet()) {
          tsStats.addAll(client.getTabletStats(Tracer.traceInfo(), SystemCredentials.get().toThrift(Monitor.getInstance()), tableId));
        }
        historical = client.getHistoricalStats(Tracer.traceInfo(), SystemCredentials.get().toThrift(Monitor.getInstance()));
      } finally {
        ThriftUtil.returnClient(client);
      }
    } catch (Exception e) {
      banner(sb, "error", "No Such Tablet ServerAvailable");
      log.error(e, e);
      return;
    }

    Table perTabletResults = new Table("perTabletResults", "Detailed&nbsp;Current&nbsp;Operations");
    perTabletResults.setSubCaption("Per-tablet&nbsp;Details");
    perTabletResults.addSortableColumn("Table", new TableLinkType(), null);
    perTabletResults.addSortableColumn("Tablet");
    perTabletResults.addSortableColumn("Entries", new NumberType<Long>(), null);
    perTabletResults.addSortableColumn("Ingest", new NumberType<Long>(), null);
    perTabletResults.addSortableColumn("Query", new NumberType<Long>(), null);
    perTabletResults.addSortableColumn("Minor&nbsp;Avg", new SecondType(), null);
    perTabletResults.addSortableColumn("Minor&nbsp;Std&nbsp;Dev", new SecondType(), null);
    perTabletResults.addSortableColumn("Minor&nbsp;Avg&nbsp;e/s", new NumberType<Double>(), null);
    perTabletResults.addSortableColumn("Major&nbsp;Avg", new SecondType(), null);
    perTabletResults.addSortableColumn("Major&nbsp;Std&nbsp;Dev", new SecondType(), null);
    perTabletResults.addSortableColumn("Major&nbsp;Avg&nbsp;e/s", new NumberType<Double>(), null);

    for (TabletStats info : tsStats) {
      if (info.extent == null) {
        historical = info;
        continue;
      }
      total.numEntries += info.numEntries;
      ActionStatsUpdator.update(total.minors, info.minors);
      ActionStatsUpdator.update(total.majors, info.majors);

      KeyExtent extent = new KeyExtent(info.extent);
      String tableId = extent.getTableId().toString();
      MessageDigest digester = MessageDigest.getInstance("MD5");
      if (extent.getEndRow() != null && extent.getEndRow().getLength() > 0) {
        digester.update(extent.getEndRow().getBytes(), 0, extent.getEndRow().getLength());
      }
      String obscuredExtent = Base64.encodeBase64String(digester.digest());
      String displayExtent = String.format("<code>[%s]</code>", obscuredExtent);

      TableRow row = perTabletResults.prepareRow();
      row.add(tableId);
      row.add(displayExtent);
      row.add(info.numEntries);
      row.add(info.ingestRate);
      row.add(info.queryRate);
      row.add(info.minors.num != 0 ? info.minors.elapsed / info.minors.num : null);
      row.add(stddev(info.minors.elapsed, info.minors.num, info.minors.sumDev));
      row.add(info.minors.elapsed != 0 ? info.minors.count / info.minors.elapsed : null);
      row.add(info.majors.num != 0 ? info.majors.elapsed / info.majors.num : null);
      row.add(stddev(info.majors.elapsed, info.majors.num, info.majors.sumDev));
      row.add(info.majors.elapsed != 0 ? info.majors.count / info.majors.elapsed : null);
      perTabletResults.addRow(row);
    }

    // Calculate current averages oldServer adding in historical data
    if (total.minors.num != 0)
      currentMinorAvg = (long) (total.minors.elapsed / total.minors.num);
    if (total.minors.elapsed != 0 && total.minors.num != 0)
      currentMinorStdDev = stddev(total.minors.elapsed, total.minors.num, total.minors.sumDev);
    if (total.majors.num != 0)
      currentMajorAvg = total.majors.elapsed / total.majors.num;
    if (total.majors.elapsed != 0 && total.majors.num != 0 && total.majors.elapsed > total.majors.num)
      currentMajorStdDev = stddev(total.majors.elapsed, total.majors.num, total.majors.sumDev);

    // After these += operations, these variables are now total for current
    // tablets and historical tablets
    ActionStatsUpdator.update(total.minors, historical.minors);
    ActionStatsUpdator.update(total.majors, historical.majors);
    totalElapsedForAll += total.majors.elapsed + historical.splits.elapsed + total.minors.elapsed;

    minorStdDev = stddev(total.minors.elapsed, total.minors.num, total.minors.sumDev);
    minorQueueStdDev = stddev(total.minors.queueTime, total.minors.num, total.minors.queueSumDev);
    majorStdDev = stddev(total.majors.elapsed, total.majors.num, total.majors.sumDev);
    majorQueueStdDev = stddev(total.majors.queueTime, total.majors.num, total.majors.queueSumDev);
    splitStdDev = stddev(historical.splits.num, historical.splits.elapsed, historical.splits.sumDev);

    doDetailTable(req, sb, address, tsStats.size(), total, historical);
    doAllTimeTable(req, sb, total, historical, majorQueueStdDev, minorQueueStdDev, totalElapsedForAll, splitStdDev, majorStdDev, minorStdDev);
    doCurrentTabletOps(req, sb, currentMinorAvg, currentMinorStdDev, currentMajorAvg, currentMajorStdDev);
    perTabletResults.generate(req, sb);
  }

  private void doCurrentTabletOps(HttpServletRequest req, StringBuilder sb, double currentMinorAvg, double currentMinorStdDev, double currentMajorAvg,
      double currentMajorStdDev) {
    Table currentTabletOps = new Table("currentTabletOps", "Current&nbsp;Tablet&nbsp;Operation&nbsp;Results");
    currentTabletOps.addSortableColumn("Minor&nbsp;Average", new SecondType(), null);
    currentTabletOps.addSortableColumn("Minor&nbsp;Std&nbsp;Dev", new SecondType(), null);
    currentTabletOps.addSortableColumn("Major&nbsp;Avg", new SecondType(), null);
    currentTabletOps.addSortableColumn("Major&nbsp;Std&nbsp;Dev", new SecondType(), null);
    currentTabletOps.addRow(currentMinorAvg, currentMinorStdDev, currentMajorAvg, currentMajorStdDev);
    currentTabletOps.generate(req, sb);
  }

  private void doAllTimeTable(HttpServletRequest req, StringBuilder sb, TabletStats total, TabletStats historical, double majorQueueStdDev,
      double minorQueueStdDev, double totalElapsedForAll, double splitStdDev, double majorStdDev, double minorStdDev) {

    Table opHistoryDetails = new Table("opHistoryDetails", "All-Time&nbsp;Tablet&nbsp;Operation&nbsp;Results");
    opHistoryDetails.addSortableColumn("Operation");
    opHistoryDetails.addSortableColumn("Success", new NumberType<Integer>(), null);
    opHistoryDetails.addSortableColumn("Failure", new NumberType<Integer>(), null);
    opHistoryDetails.addSortableColumn("Average<br />Queue&nbsp;Time", new SecondType(), null);
    opHistoryDetails.addSortableColumn("Std.&nbsp;Dev.<br />Queue&nbsp;Time", new SecondType(), null);
    opHistoryDetails.addSortableColumn("Average<br />Time", new SecondType(), null);
    opHistoryDetails.addSortableColumn("Std.&nbsp;Dev.<br />Time", new SecondType(), null);
    opHistoryDetails.addSortableColumn("Percentage&nbsp;Time&nbsp;Spent", new ProgressChartType(totalElapsedForAll), null);

    opHistoryDetails.addRow("Split", historical.splits.num, historical.splits.fail, null, null,
        historical.splits.num != 0 ? (historical.splits.elapsed / historical.splits.num) : null, splitStdDev, historical.splits.elapsed);
    opHistoryDetails.addRow("Major&nbsp;Compaction", total.majors.num, total.majors.fail, total.majors.num != 0 ? (total.majors.queueTime / total.majors.num)
        : null, majorQueueStdDev, total.majors.num != 0 ? (total.majors.elapsed / total.majors.num) : null, majorStdDev, total.majors.elapsed);
    opHistoryDetails.addRow("Minor&nbsp;Compaction", total.minors.num, total.minors.fail, total.minors.num != 0 ? (total.minors.queueTime / total.minors.num)
        : null, minorQueueStdDev, total.minors.num != 0 ? (total.minors.elapsed / total.minors.num) : null, minorStdDev, total.minors.elapsed);
    opHistoryDetails.generate(req, sb);
  }

  private void doDetailTable(HttpServletRequest req, StringBuilder sb, HostAndPort address, int numTablets, TabletStats total, TabletStats historical) {
    Table detailTable = new Table("tServerDetail", "Details");
    detailTable.setSubCaption(address.getHostText() + ":" + address.getPort());
    detailTable.addSortableColumn("Hosted&nbsp;Tablets", new NumberType<Integer>(), null);
    detailTable.addSortableColumn("Entries", new NumberType<Long>(), null);
    detailTable.addSortableColumn("Minor&nbsp;Compacting", new NumberType<Integer>(), null);
    detailTable.addSortableColumn("Major&nbsp;Compacting", new NumberType<Integer>(), null);
    detailTable.addSortableColumn("Splitting", new NumberType<Integer>(), null);
    detailTable.addRow(numTablets, total.numEntries, total.minors.status, total.majors.status, historical.splits.status);
    detailTable.generate(req, sb);
  }

  /*
   * omg there's so much undocumented stuff going on here. First, sumDev is a partial standard deviation computation. It is the (clue 1) sum of the squares of
   * (clue 2) seconds of elapsed time.
   */
  private static double stddev(double elapsed, double num, double sumDev) {
    if (num != 0) {
      double average = elapsed / num;
      return Math.sqrt((sumDev / num) - (average * average));
    }
    return 0;
  }

  private void doBadTserverList(HttpServletRequest req, StringBuilder sb) {
    if (Monitor.getMmi() != null && !Monitor.getMmi().badTServers.isEmpty()) {
      Table badTServerList = new Table("badtservers", "Non-Functioning&nbsp;Tablet&nbsp;Servers", "error");
      badTServerList.setSubCaption("The following tablet servers reported a status other than Online.");
      badTServerList.addSortableColumn("Tablet&nbsp;Server");
      badTServerList.addSortableColumn("Tablet&nbsp;Server&nbsp;Status");
      for (Entry<String,Byte> badserver : Monitor.getMmi().badTServers.entrySet())
        badTServerList.addRow(badserver.getKey(), TabletServerState.getStateById(badserver.getValue()).name());
      badTServerList.generate(req, sb);
    }
  }

  private void doDeadTserverList(HttpServletRequest req, StringBuilder sb) {
    MasterMonitorInfo mmi = Monitor.getMmi();
    if (mmi != null) {
      List<DeadServer> obit = mmi.deadTabletServers;
      Table deadTServerList = new Table("deaddtservers", "Dead&nbsp;Tablet&nbsp;Servers", "error");
      deadTServerList.setSubCaption("The following tablet servers are no longer reachable.");
      doDeadServerTable(req, sb, deadTServerList, obit);
    }
  }

  public static void doDeadServerTable(HttpServletRequest req, StringBuilder sb, Table deadTServerList, List<DeadServer> obit) {
    if (obit != null && !obit.isEmpty()) {
      deadTServerList.addSortableColumn("Server");
      deadTServerList.addSortableColumn("Last&nbsp;Updated", new DateTimeType(DateFormat.MEDIUM, DateFormat.SHORT), null);
      deadTServerList.addSortableColumn("Event");
      deadTServerList.addUnsortableColumn("Clear");
      for (DeadServer dead : obit)
        deadTServerList.addRow(TServerLinkType.displayName(dead.server), dead.lastStatus, dead.status, "<a href='/op?action=clearDeadServer&redir="
            + currentPage(req) + "&server=" + encode(dead.server) + "'>clear</a>");
      deadTServerList.generate(req, sb);
    }
  }

  static void doTserverList(HttpServletRequest req, StringBuilder sb, List<TabletServerStatus> tservers, String tableId, Table tServerList) {
    int guessHighLoad = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
    long now = System.currentTimeMillis();

    double avgLastContact = 0.;
    for (TabletServerStatus status : tservers) {
      avgLastContact += (now - status.lastContact);
    }
    final long MINUTES = 3 * 60 * 1000;
    tServerList.addSortableColumn("Server", new TServerLinkType(), null);
    tServerList.addSortableColumn("Hosted&nbsp;Tablets", new NumberType<Integer>(0, Integer.MAX_VALUE), null);
    tServerList.addSortableColumn("Last&nbsp;Contact", new DurationType(0l, (long) Math.min(avgLastContact * 4, MINUTES)), null);
    tServerList.addSortableColumn("Entries", new NumberType<Long>(), "The number of key/value pairs.");
    tServerList.addSortableColumn("Ingest", new NumberType<Long>(), "The number of key/value pairs inserted. (Note that deletes are also 'inserted')");
    tServerList.addSortableColumn("Query", new NumberType<Long>(), "The number of key/value pairs returned to clients. (Not the number of scans)");
    tServerList.addSortableColumn("Hold&nbsp;Time", new DurationType(), "The amount of time ingest is suspended waiting for data to be written to disk.");
    tServerList.addSortableColumn("Running<br />Scans", new CompactionsType("scans"), "The number of scans running and queued on this tablet server.");
    tServerList
        .addSortableColumn(
            "Minor<br />Compactions",
            new CompactionsType("minor"),
            "The number of minor compactions running and (queued waiting for resources). Minor compactions are the operations where entries are flushed from memory to disk.");
    tServerList.addSortableColumn("Major<br />Compactions", new CompactionsType("major"),
        "The number of major compactions running and (queued waiting for resources). "
            + "Major compactions are the operations where many smaller files are grouped into a larger file, eliminating duplicates and cleaning up deletes.");
    tServerList.addSortableColumn("Index Cache<br />Hit Rate", new PercentageType(), "The recent index cache hit rate.");
    tServerList.addSortableColumn("Data Cache<br />Hit Rate", new PercentageType(), "The recent data cache hit rate.");
    tServerList.addSortableColumn("OS&nbsp;Load", new NumberType<Double>(0., guessHighLoad * 1., 0., guessHighLoad * 3.),
        "The Unix one minute load average. The average number of processes in the run queue over a one minute interval.");

    log.debug("tableId: " + tableId);
    for (TabletServerStatus status : tservers) {
      if (status == null)
        status = NO_STATUS;
      TableInfo summary = TableInfoUtil.summarizeTableStats(status);
      if (tableId != null)
        summary = status.tableMap.get(tableId);
      if (summary == null)
        continue;
      TableRow row = tServerList.prepareRow();
      row.add(status); // add for server name
      row.add(summary.tablets);
      row.add(now - status.lastContact);
      row.add(summary.recs);
      row.add(summary.ingestRate);
      row.add(summary.queryRate);
      row.add(status.holdTime);
      row.add(summary); // add for scans
      row.add(summary); // add for minor compactions
      row.add(summary); // add for major compactions
      double indexCacheHitRate = status.indexCacheHits / (double) Math.max(status.indexCacheRequest, 1);
      row.add(indexCacheHitRate);
      double dataCacheHitRate = status.dataCacheHits / (double) Math.max(status.dataCacheRequest, 1);
      row.add(dataCacheHitRate);
      row.add(status.osLoad);
      tServerList.addRow(row);
    }
    tServerList.generate(req, sb);
  }

}
