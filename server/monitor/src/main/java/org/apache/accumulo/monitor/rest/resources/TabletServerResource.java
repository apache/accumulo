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
package org.apache.accumulo.monitor.rest.resources;

import java.lang.management.ManagementFactory;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.ActionStats;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.api.AllTimeTabletResults;
import org.apache.accumulo.monitor.rest.api.CurrentOperations;
import org.apache.accumulo.monitor.rest.api.CurrentTabletResults;
import org.apache.accumulo.monitor.rest.api.ServerStats;
import org.apache.accumulo.monitor.rest.api.TabletServer;
import org.apache.accumulo.monitor.rest.api.TabletServerDetailInformation;
import org.apache.accumulo.monitor.rest.api.TabletServerSummary;
import org.apache.accumulo.monitor.rest.api.TabletServers;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.util.ActionStatsUpdator;

import com.google.common.net.HostAndPort;

@Path("/{parameter: tservers|json}")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class TabletServerResource {

  private TabletStats total, historical;

  @GET
  public TabletServers getTserverSummary() {
    MasterMonitorInfo mmi = Monitor.getMmi();
    if (null == mmi) {
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

    TabletServers tserverInfo = new TabletServers(mmi.tServerInfo.size());
    for (TabletServerStatus status : mmi.tServerInfo) {
      tserverInfo.addTablet(new TabletServer(status));
    }

    return tserverInfo;
  }

  @Path("/{address}")
  @GET
  public TabletServerSummary getTserverDetails(@PathParam("address") String tserverAddr) throws Exception {

    String tserverAddress = tserverAddr;

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

      return null;
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
    total = new TabletStats(null, new ActionStats(), new ActionStats(), new ActionStats(), 0, 0, 0, 0);
    HostAndPort address = HostAndPort.fromString(tserverAddress);
    historical = new TabletStats(null, new ActionStats(), new ActionStats(), new ActionStats(), 0, 0, 0, 0);
    List<TabletStats> tsStats = new ArrayList<>();

    try {
      ClientContext context = Monitor.getContext();
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, context);
      try {
        for (String tableId : Monitor.getMmi().tableMap.keySet()) {
          tsStats.addAll(client.getTabletStats(Tracer.traceInfo(), context.rpcCreds(), tableId));
        }
        historical = client.getHistoricalStats(Tracer.traceInfo(), context.rpcCreds());
      } finally {
        ThriftUtil.returnClient(client);
      }
    } catch (Exception e) {
      return null;
    }

    List<CurrentOperations> currentOps = doCurrentOperations(tsStats);

    if (total.minors.num != 0)
      currentMinorAvg = (long) (total.minors.elapsed / total.minors.num);
    if (total.minors.elapsed != 0 && total.minors.num != 0)
      currentMinorStdDev = stddev(total.minors.elapsed, total.minors.num, total.minors.sumDev);
    if (total.majors.num != 0)
      currentMajorAvg = total.majors.elapsed / total.majors.num;
    if (total.majors.elapsed != 0 && total.majors.num != 0 && total.majors.elapsed > total.majors.num)
      currentMajorStdDev = stddev(total.majors.elapsed, total.majors.num, total.majors.sumDev);

    ActionStatsUpdator.update(total.minors, historical.minors);
    ActionStatsUpdator.update(total.majors, historical.majors);
    totalElapsedForAll += total.majors.elapsed + historical.splits.elapsed + total.minors.elapsed;

    minorStdDev = stddev(total.minors.elapsed, total.minors.num, total.minors.sumDev);
    minorQueueStdDev = stddev(total.minors.queueTime, total.minors.num, total.minors.queueSumDev);
    majorStdDev = stddev(total.majors.elapsed, total.majors.num, total.majors.sumDev);
    majorQueueStdDev = stddev(total.majors.queueTime, total.majors.num, total.majors.queueSumDev);
    splitStdDev = stddev(historical.splits.num, historical.splits.elapsed, historical.splits.sumDev);

    TabletServerDetailInformation details = doDetails(address, tsStats.size());

    List<AllTimeTabletResults> allTime = doAllTimeResults(majorQueueStdDev, minorQueueStdDev, totalElapsedForAll, splitStdDev, majorStdDev, minorStdDev);

    CurrentTabletResults currentRes = doCurrentTabletResults(currentMinorAvg, currentMinorStdDev, currentMajorAvg, currentMajorStdDev);

    TabletServerSummary tserverDetails = new TabletServerSummary(details, allTime, currentRes, currentOps);

    return tserverDetails;
  }
  
  private static final int concurrentScans = Monitor.getContext().getConfiguration().getCount(Property.TSERV_READ_AHEAD_MAXCONCURRENT);
  
  @Path("/serverStats")
  @GET
  public List<ServerStats> getServerStats() {
	  
	  List<ServerStats> stats = new ArrayList<>();
	  
	  stats.add(new ServerStats(ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors(), true, 100, "OS Load", "osload"));
	  stats.add(new ServerStats(1000, true, 1, "Ingest Entries", "ingest"));
	  stats.add(new ServerStats(10000, true, 1, "Scan Entries", "query"));
	  stats.add(new ServerStats(10, true, 10, "Ingest MB", "ingestMB"));
	  stats.add(new ServerStats(5, true, 10, "Scan MB", "queryMB"));
	  stats.add(new ServerStats(concurrentScans * 2, false, 1, "Running Scans", "scans"));
	  stats.add(new ServerStats(50, true, 10, "Scan Sessions", "scansessions"));
	  stats.add(new ServerStats(60000, false, 1, "Hold Time", "holdtime"));
	  stats.add(new ServerStats(1, false, 100, "Overall Avg", true, "allavg"));
	  stats.add(new ServerStats(1, false, 100, "Overall Max", true, "allmax"));
	  
	  return stats;	  
  }

  private TabletServerDetailInformation doDetails(HostAndPort address, int numTablets) {

    return new TabletServerDetailInformation(numTablets, total.numEntries, total.minors.status, total.majors.status, historical.splits.status);
  }

  private List<AllTimeTabletResults> doAllTimeResults(double majorQueueStdDev, double minorQueueStdDev, double totalElapsedForAll, double splitStdDev,
      double majorStdDev, double minorStdDev) {

    List<AllTimeTabletResults> allTime = new ArrayList<>();

    // Minor Compaction Operation
    allTime.add(new AllTimeTabletResults("Minor&nbsp;Compaction", total.minors.num, total.minors.fail,
        total.minors.num != 0 ? (total.minors.queueTime / total.minors.num) : null, minorQueueStdDev,
        total.minors.num != 0 ? (total.minors.elapsed / total.minors.num) : null, minorStdDev, total.minors.elapsed));

    // Major Compaction Operation
    allTime.add(new AllTimeTabletResults("Major&nbsp;Compaction", total.majors.num, total.majors.fail,
        total.majors.num != 0 ? (total.majors.queueTime / total.majors.num) : null, majorQueueStdDev,
        total.majors.num != 0 ? (total.majors.elapsed / total.majors.num) : null, majorStdDev, total.majors.elapsed));
    // Split Operation
    allTime.add(new AllTimeTabletResults("Split", historical.splits.num, historical.splits.fail, null, null,
        historical.splits.num != 0 ? (historical.splits.elapsed / historical.splits.num) : null, splitStdDev, historical.splits.elapsed));

    return allTime;
  }

  private CurrentTabletResults doCurrentTabletResults(double currentMinorAvg, double currentMinorStdDev, double currentMajorAvg, double currentMajorStdDev) {

    return new CurrentTabletResults(currentMinorAvg, currentMinorStdDev, currentMajorAvg, currentMajorStdDev);
  }

  private List<CurrentOperations> doCurrentOperations(List<TabletStats> tsStats) throws Exception {

    Map<String,String> tidToNameMap = Tables.getIdToNameMap(HdfsZooInstance.getInstance());

    List<CurrentOperations> currentOperations = new ArrayList<>();

    for (TabletStats info : tsStats) {
      if (info.extent == null) {
        historical = info;
        continue;
      }
      total.numEntries += info.numEntries;
      ActionStatsUpdator.update(total.minors, info.minors);
      ActionStatsUpdator.update(total.majors, info.majors);

      KeyExtent extent = new KeyExtent(info.extent);
      String tableId = extent.getTableId();
      MessageDigest digester = MessageDigest.getInstance("MD5");
      if (extent.getEndRow() != null && extent.getEndRow().getLength() > 0) {
        digester.update(extent.getEndRow().getBytes(), 0, extent.getEndRow().getLength());
      }
      String obscuredExtent = Base64.getEncoder().encodeToString(digester.digest());
      String displayExtent = String.format("[%s]", obscuredExtent);

      String tableName = Tables.getPrintableTableNameFromId(tidToNameMap, tableId);

      currentOperations.add(new CurrentOperations(tableName, tableId, displayExtent, info.numEntries, info.ingestRate, info.queryRate,
          info.minors.num != 0 ? info.minors.elapsed / info.minors.num : null, stddev(info.minors.elapsed, info.minors.num, info.minors.sumDev),
          info.minors.elapsed != 0 ? info.minors.count / info.minors.elapsed : null, info.majors.num != 0 ? info.majors.elapsed / info.majors.num : null,
          stddev(info.majors.elapsed, info.majors.num, info.majors.sumDev), info.majors.elapsed != 0 ? info.majors.count / info.majors.elapsed : null));
    }

    return currentOperations;
  }

  private static double stddev(double elapsed, double num, double sumDev) {
    if (num != 0) {
      double average = elapsed / num;
      return Math.sqrt((sumDev / num) - (average * average));
    }
    return 0;
  }
}
