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

import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
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
        runningCompactions(context, runningOpts.details);
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

  private void runningCompactions(ServerContext context, boolean details) {
    CompactionCoordinatorService.Client coordinatorClient = null;
    TExternalCompactionList running;
    try {
      coordinatorClient = getCoordinatorClient(context);
      running = coordinatorClient.getRunningCompactions(TraceUtil.traceInfo(), context.rpcCreds());
      if (running == null) {
        System.out.println("No running compactions found.");
        return;
      }
      var ecidMap = running.getCompactions();
      if (ecidMap == null) {
        System.out.println("No running compactions found.");
        return;
      }
      ecidMap.forEach((ecid, ec) -> {
        if (ec != null) {
          var runningCompaction = new RunningCompaction(ec);
          var addr = runningCompaction.getCompactorAddress();
          var kind = runningCompaction.getJob().kind;
          var queue = runningCompaction.getQueueName();
          var ke = KeyExtent.fromThrift(runningCompaction.getJob().extent);
          System.out.format("%s %s %s %s TableId: %s\n", ecid, addr, kind, queue, ke.tableId());
          if (details) {
            var runningCompactionInfo = new RunningCompactionInfo(ec);
            var status = runningCompactionInfo.status;
            var last = runningCompactionInfo.lastUpdate;
            var duration = runningCompactionInfo.duration;
            var numFiles = runningCompactionInfo.numFiles;
            var progress = runningCompactionInfo.progress;
            System.out.format("  %s Last Update: %dms Duration: %dms Files: %d Progress: %.2f%%\n",
                status, last, duration, numFiles, progress);
          }
        }
      });
    } catch (Exception e) {
      throw new RuntimeException("Unable to get running compactions.", e);
    } finally {
      ThriftUtil.returnClient(coordinatorClient, context);
    }
  }

  private CompactionCoordinatorService.Client getCoordinatorClient(ServerContext context) {
    var coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(context);
    if (coordinatorHost.isEmpty()) {
      throw new IllegalStateException("Unable to find coordinator. Check that it is running.");
    }
    HostAndPort address = coordinatorHost.get();
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
