/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.util;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionMap;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.core.util.compaction.RunningCompactionInfo;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.auto.service.AutoService;
import com.google.common.net.HostAndPort;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Admin utility for external compactions
 */
@AutoService(KeywordExecutable.class)
public class ECAdmin implements KeywordExecutable {

  public static class RunningCompactionSummary {
    private final String ecid;
    private final String addr;
    private final TCompactionKind kind;
    private final String groupName;
    private final String ke;
    private final String tableId;
    private String status = "";
    private long lastUpdate = 0;
    private long duration = 0;
    private int numFiles = 0;
    private double progress = 0.0;

    public RunningCompactionSummary(RunningCompaction runningCompaction,
        RunningCompactionInfo runningCompactionInfo) {
      super();
      ecid = runningCompaction.getJob().getExternalCompactionId();
      addr = runningCompaction.getCompactorAddress();
      kind = runningCompaction.getJob().kind;
      groupName = runningCompaction.getGroupName();
      KeyExtent extent = KeyExtent.fromThrift(runningCompaction.getJob().extent);
      ke = extent.obscured();
      tableId = extent.tableId().canonical();
      if (runningCompactionInfo != null) {
        status = runningCompactionInfo.status;
        lastUpdate = runningCompactionInfo.lastUpdate;
        duration = runningCompactionInfo.duration;
        numFiles = runningCompactionInfo.numFiles;
        progress = runningCompactionInfo.progress;
      }

    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }

    public long getLastUpdate() {
      return lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
      this.lastUpdate = lastUpdate;
    }

    public long getDuration() {
      return duration;
    }

    public void setDuration(long duration) {
      this.duration = duration;
    }

    public int getNumFiles() {
      return numFiles;
    }

    public void setNumFiles(int numFiles) {
      this.numFiles = numFiles;
    }

    public double getProgress() {
      return progress;
    }

    public void setProgress(double progress) {
      this.progress = progress;
    }

    public String getEcid() {
      return ecid;
    }

    public String getAddr() {
      return addr;
    }

    public TCompactionKind getKind() {
      return kind;
    }

    public String getGroupName() {
      return groupName;
    }

    public String getKe() {
      return ke;
    }

    public String getTableId() {
      return tableId;
    }

    public void print(PrintStream out) {
      out.format("%s %s %s %s TableId: %s\n", ecid, addr, kind, groupName, tableId);
      out.format("  %s Last Update: %dms Duration: %dms Files: %d Progress: %.2f%%\n", status,
          lastUpdate, duration, numFiles, progress);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(ECAdmin.class);

  @Parameters(commandDescription = "cancel the external compaction with given ECID")
  static class CancelCommand {
    @Parameter(names = "-ecid", description = "<ecid>", required = true)
    String ecid;
  }

  @Parameters(commandDescription = "list the running compactions")
  static class RunningCommand {
    @Parameter(names = {"-d", "--details"},
        description = "display details about the running compactions")
    boolean details = false;

    @Parameter(names = {"-j", "--json"}, description = "format the output as json")
    boolean jsonOutput = false;
  }

  @Parameters(commandDescription = "list all compactors in zookeeper")
  static class ListCompactorsCommand {}

  public static void main(String[] args) {
    new ECAdmin().execute(args);
  }

  @Override
  public String keyword() {
    return "ec-admin";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.CORE;
  }

  @Override
  public String description() {
    return "Executes administrative commands for external compactions";
  }

  @SuppressFBWarnings(value = "DM_EXIT", justification = "System.exit okay for CLI tool")
  @Override
  public void execute(final String[] args) {
    ServerUtilOpts opts = new ServerUtilOpts();
    JCommander cl = new JCommander(opts);
    cl.setProgramName("accumulo ec-admin");

    CancelCommand cancelOps = new CancelCommand();
    cl.addCommand("cancel", cancelOps);

    ListCompactorsCommand listCompactorsOpts = new ListCompactorsCommand();
    cl.addCommand("listCompactors", listCompactorsOpts);

    RunningCommand runningOpts = new RunningCommand();
    cl.addCommand("running", runningOpts);

    cl.parse(args);

    if (opts.help || cl.getParsedCommand() == null) {
      cl.usage();
      return;
    }

    ServerContext context = opts.getServerContext();
    try {
      if (cl.getParsedCommand().equals("listCompactors")) {
        listCompactorsByQueue(context);
      } else if (cl.getParsedCommand().equals("cancel")) {
        cancelCompaction(context, cancelOps.ecid);
      } else if (cl.getParsedCommand().equals("running")) {
        List<RunningCompactionSummary> compactions =
            runningCompactions(context, runningOpts.details);
        if (runningOpts.jsonOutput) {
          try {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(compactions));
          } catch (Exception e) {
            log.error("Error generating JSON output", e);
          }
        } else {
          compactions.forEach(c -> c.print(System.out));
        }
      } else {
        log.error("Unknown command {}", cl.getParsedCommand());
        cl.usage();
        System.exit(1);
      }
    } catch (Exception e) {
      log.error("{}", e.getMessage(), e);
      System.exit(1);
    }
  }

  protected void cancelCompaction(ServerContext context, String ecid) {
    CompactionCoordinatorService.Client coordinatorClient = null;
    ecid = ExternalCompactionId.from(ecid).canonical();
    try {
      coordinatorClient = getCoordinatorClient(context);
      coordinatorClient.cancel(TraceUtil.traceInfo(), context.rpcCreds(), ecid);
      System.out.println("Cancel sent to coordinator for " + ecid);
    } catch (Exception e) {
      throw new IllegalStateException("Exception calling cancel compaction for " + ecid, e);
    } finally {
      ThriftUtil.returnClient(coordinatorClient, context);
    }
  }

  protected void listCompactorsByQueue(ServerContext context) {
    Set<ServerId> compactors = context.instanceOperations().getServers(ServerId.Type.COMPACTOR);
    if (compactors.isEmpty()) {
      System.out.println("No Compactors found.");
    } else {
      Map<String,List<ServerId>> m = new TreeMap<>();
      compactors.forEach(csi -> {
        m.putIfAbsent(csi.getResourceGroup(), new ArrayList<>()).add(csi);
      });
      m.forEach((q, c) -> System.out.println(q + ": " + c));
    }
  }

  protected List<RunningCompactionSummary> runningCompactions(ServerContext context,
      boolean details) {
    CompactionCoordinatorService.Client coordinatorClient = null;
    try {
      coordinatorClient = getCoordinatorClient(context);

      // Fetch running compactions as a list and convert to a map
      TExternalCompactionMap running =
          coordinatorClient.getRunningCompactions(TraceUtil.traceInfo(), context.rpcCreds());

      List<RunningCompactionSummary> results = new ArrayList<>();

      if (running == null || running.getCompactions() == null
          || running.getCompactions().isEmpty()) {
        System.out.println("No running compactions found.");
        return results;
      }

      for (Map.Entry<String,TExternalCompaction> entry : running.getCompactions().entrySet()) {
        TExternalCompaction ec = entry.getValue();
        if (ec == null) {
          continue;
        }
        var summary = new RunningCompactionSummary(new RunningCompaction(ec),
            details ? new RunningCompactionInfo(ec) : null);
        results.add(summary);
      }
      return results;
    } catch (Exception e) {
      throw new IllegalStateException("Unable to get running compactions.", e);
    } finally {
      ThriftUtil.returnClient(coordinatorClient, context);
    }
  }

  private CompactionCoordinatorService.Client getCoordinatorClient(ServerContext context) {
    var coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(context);
    if (coordinatorHost.isEmpty()) {
      throw new IllegalStateException("Unable to find coordinator. Check that it is running.");
    }
    HostAndPort address = coordinatorHost.orElseThrow();
    CompactionCoordinatorService.Client coordinatorClient;
    try {
      coordinatorClient = ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, address, context);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to get Compaction coordinator at " + address, e);
    }
    System.out.println("Connected to coordinator at " + address);
    return coordinatorClient;
  }
}
