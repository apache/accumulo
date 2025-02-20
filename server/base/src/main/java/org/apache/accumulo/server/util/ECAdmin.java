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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Admin utility for external compactions
 */
@AutoService(KeywordExecutable.class)
public class ECAdmin implements KeywordExecutable {
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

    @Parameter(names = {"-f", "--format"},
        description = "output format: plain (default), csv, json")
    String format = "plain";
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
        runningCompactions(context, runningOpts.details, runningOpts.format);
      } else {
        log.error("Unknown command {}", cl.getParsedCommand());
        cl.usage();
        System.exit(1);
      }
    } catch (Exception e) {
      log.error("{}", e.getMessage(), e);
      System.exit(1);
    } finally {
      SingletonManager.setMode(Mode.CLOSED);
    }
  }

  private void cancelCompaction(ServerContext context, String ecid) {
    CompactionCoordinatorService.Client coordinatorClient = null;
    ecid = ExternalCompactionId.from(ecid).canonical();
    try {
      coordinatorClient = getCoordinatorClient(context);
      coordinatorClient.cancel(TraceUtil.traceInfo(), context.rpcCreds(), ecid);
      System.out.println("Cancel sent to coordinator for " + ecid);
    } catch (Exception e) {
      throw new RuntimeException("Exception calling cancel compaction for " + ecid, e);
    } finally {
      ThriftUtil.returnClient(coordinatorClient, context);
    }
  }

  private void listCompactorsByQueue(ServerContext context) {
    var queueToCompactorsMap = ExternalCompactionUtil.getCompactorAddrs(context);
    if (queueToCompactorsMap.isEmpty()) {
      System.out.println("No Compactors found.");
    } else {
      queueToCompactorsMap.forEach((q, compactors) -> System.out.println(q + ": " + compactors));
    }
  }

  private void runningCompactions(ServerContext context, boolean details, String format) {
    CompactionCoordinatorService.Client coordinatorClient = null;
    Map<String,TExternalCompaction> runningCompactionsMap = new HashMap<>();

    // Default to "plain" format if null or empty
    if (format == null || format.trim().isEmpty()) {
      format = "plain";
    } else {
    // Validate format
      Set<String> validFormats = Set.of("plain", "csv", "json");
      if (!validFormats.contains(format.toLowerCase())) {
        throw new IllegalArgumentException(
            "Invalid format: " + format + ". Expected: plain, csv, or json.");
      }
    }

    try {
      coordinatorClient = getCoordinatorClient(context);

      // Fetch running compactions as a list and convert to a map
      TExternalCompactionList running =
          coordinatorClient.getRunningCompactions(TraceUtil.traceInfo(), context.rpcCreds());

      if (running == null || running.getCompactions().isEmpty()) {
        System.out.println("No running compactions found.");
        return;
      }

      for (Map.Entry<String,TExternalCompaction> entry : running.getCompactions().entrySet()) {
        runningCompactionsMap.put(entry.getKey(), entry.getValue());
      }

      List<Map<String,Object>> jsonOutput = new ArrayList<>();

      if ("csv".equalsIgnoreCase(format)) {
        System.out.println(
            "ECID,Compactor,Kind,Queue,TableId,Status,LastUpdate,Duration,NumFiles,Progress");
      }

      for (Map.Entry<String,TExternalCompaction> entry : runningCompactionsMap.entrySet()) {
        TExternalCompaction ec = entry.getValue();
        if (ec == null) {
          continue;
        }

        var runningCompaction = new RunningCompaction(ec);
        String ecid = runningCompaction.getJob().getExternalCompactionId();
        var addr = runningCompaction.getCompactorAddress();
        var kind = runningCompaction.getJob().kind;
        var queueName = runningCompaction.getQueueName();
        var ke = KeyExtent.fromThrift(runningCompaction.getJob().extent);
        String tableId = ke.tableId().canonical();

        String status = "";
        long lastUpdate = 0, duration = 0;
        int numFiles = 0;
        double progress = 0.0;

        if (details) {
          var runningCompactionInfo = new RunningCompactionInfo(ec);
          status = runningCompactionInfo.status;
          lastUpdate = runningCompactionInfo.lastUpdate;
          duration = runningCompactionInfo.duration;
          numFiles = runningCompactionInfo.numFiles;
          progress = runningCompactionInfo.progress;
        }

        switch (format.toLowerCase()) {
          case "plain":
            System.out.format("%s %s %s %s TableId: %s\n", ecid, addr, kind, queueName, tableId);
            if (details) {
              System.out.format(
                  "  %s Last Update: %dms Duration: %dms Files: %d Progress: %.2f%%\n", status,
                  lastUpdate, duration, numFiles, progress);
            }
            break;
          case "csv":
            System.out.printf("%s,%s,%s,%s,%s,%s,%d,%d,%d,%.2f\n", ecid, addr, kind, queueName,
                tableId, status, lastUpdate, duration, numFiles, progress);
            break;
          case "json":
            Map<String,Object> jsonEntry = new LinkedHashMap<>();
            jsonEntry.put("ecid", ecid);
            jsonEntry.put("compactor", addr);
            jsonEntry.put("kind", kind);
            jsonEntry.put("queue", queueName);
            jsonEntry.put("tableId", tableId);
            if (details) {
              jsonEntry.put("status", status);
              jsonEntry.put("lastUpdate", lastUpdate);
              jsonEntry.put("duration", duration);
              jsonEntry.put("numFiles", numFiles);
              jsonEntry.put("progress", progress);
            }
            jsonOutput.add(jsonEntry);
            break;
        }
      }

      if ("json".equalsIgnoreCase(format)) {
        try {
          Gson gson = new GsonBuilder().setPrettyPrinting().create();
          System.out.println(gson.toJson(jsonOutput));
        } catch (Exception e) {
          log.error("Error generating JSON output", e);
        }
      }

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
