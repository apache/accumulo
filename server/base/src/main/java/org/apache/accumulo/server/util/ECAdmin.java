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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionMap;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import com.google.common.net.HostAndPort;

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
        description = "output format: json, csv (default: human-readable)")
    String format = "human"; // Default format
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
      throw new IllegalStateException("Exception calling cancel compaction for " + ecid, e);
    } finally {
      ThriftUtil.returnClient(coordinatorClient, context);
    }
  }

  private void listCompactorsByQueue(ServerContext context) {
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

  private void runningCompactions(ServerContext context, boolean details, String format) {
    CompactionCoordinatorService.Client coordinatorClient = null;
    TExternalCompactionMap running;

    try {
      coordinatorClient = getCoordinatorClient(context);
      running = coordinatorClient.getRunningCompactions(TraceUtil.traceInfo(), context.rpcCreds());

    if (running == null || running.getCompactionsSize() == 0) {
      System.out.println("No running compactions found.");
      return;
    }

      var ecidMap = running.getCompactions();
      List<Map<String,Object>> compactionData = new ArrayList<>();

      ecidMap.forEach((ecid, ec) -> {
        if (ec != null) {
          var runningCompaction = new RunningCompaction(ec);
          var addr = runningCompaction.getCompactorAddress();
          var kind = runningCompaction.getJob().kind;
          var group = runningCompaction.getGroupName();
          var ke = KeyExtent.fromThrift(runningCompaction.getJob().extent);

          Map<String,Object> entry = new LinkedHashMap<>();
          entry.put("ecid", ecid);
          entry.put("address", addr);
          entry.put("kind", kind);
          entry.put("group", group);
          entry.put("tableId", ke.tableId());

          if (details) {
            var runningCompactionInfo = new RunningCompactionInfo(ec);
            entry.put("status", runningCompactionInfo.status);
            entry.put("lastUpdateMs", runningCompactionInfo.lastUpdate);
            entry.put("durationMs", runningCompactionInfo.duration);
            entry.put("numFiles", runningCompactionInfo.numFiles);
            entry.put("progress", runningCompactionInfo.progress);
          }

          compactionData.add(entry);
        }
      });

      // Handle output format
      switch (format.toLowerCase()) {
        case "json":
          ObjectMapper objectMapper = new ObjectMapper();
          System.out.println(
              objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(compactionData));
          break;
        case "csv":
          if (compactionData.isEmpty()) {
            System.out.println("No running compactions found.");
            return;
          }
          // Convert to CSV format
          List<String> csvLines = new ArrayList<>();
          var headers = String.join(",", compactionData.get(0).keySet());
          csvLines.add(headers);
          csvLines.addAll(compactionData.stream().map(
              row -> row.values().stream().map(Object::toString).collect(Collectors.joining(",")))
              .collect(Collectors.toList()));
          System.out.println(String.join("\n", csvLines));
          break;
        default:
          // Default human-readable output
          compactionData.forEach(entry -> {
            System.out.printf("%s %s %s %s TableId: %s\n", entry.get("ecid"), entry.get("address"),
                entry.get("kind"), entry.get("group"), entry.get("tableId"));
            if (details) {
              System.out.printf(
                  "  %s Last Update: %dms Duration: %dms Files: %d Progress: %.2f%%\n",
                  entry.get("status"), entry.get("lastUpdateMs"), entry.get("durationMs"),
                  entry.get("numFiles"), entry.get("progress"));
            }
          });
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
