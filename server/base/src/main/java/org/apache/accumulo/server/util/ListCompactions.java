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

import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionMap;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.core.util.compaction.RunningCompactionInfo;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.ListCompactions.RunningCommandOpts;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.accumulo.start.spi.UsageGroup;
import org.apache.accumulo.start.spi.UsageGroups;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.auto.service.AutoService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@AutoService(KeywordExecutable.class)
public class ListCompactions extends ServerKeywordExecutable<RunningCommandOpts> {

  public static class RunningCompactionSummary {
    private final String ecid;
    private final String addr;
    private final TCompactionKind kind;
    private final ResourceGroupId groupName;
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
      groupName = runningCompaction.getGroup();
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

    public ResourceGroupId getGroup() {
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

  @Parameters(commandNames = "running", commandDescription = "list the running compactions")
  static class RunningCommandOpts extends ServerUtilOpts {
    @Parameter(names = {"-d", "--details"},
        description = "display details about the running compactions")
    boolean details = false;

    @Parameter(names = {"-j", "--json"}, description = "format the output as json")
    boolean jsonOutput = false;
  }

  public ListCompactions() {
    super(new RunningCommandOpts());
  }

  @Override
  public String keyword() {
    return "list";
  }

  @Override
  public String description() {
    return "List running compactions";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroups.COMPACTION;
  }

  protected List<RunningCompactionSummary> getRunningCompactions(ServerContext context,
      boolean details) {
    CompactionCoordinatorService.Client coordinatorClient = null;
    try {
      coordinatorClient = ExternalCompactionUtil.getCoordinatorClient(context);

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

  @Override
  public void execute(JCommander cl, RunningCommandOpts options) throws Exception {
    ServerContext context = options.getServerContext();
    List<RunningCompactionSummary> compactions = getRunningCompactions(context, options.details);
    if (options.jsonOutput) {
      try {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        System.out.println(gson.toJson(compactions));
      } catch (Exception e) {
        System.out.println("Error generating JSON output");
        e.printStackTrace();
      }
    } else {
      compactions.forEach(c -> c.print(System.out));
    }
  }

}
