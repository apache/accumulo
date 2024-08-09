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

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.rpc.grpc.GrpcUtil;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.core.util.compaction.RunningCompactionInfo;
import org.apache.accumulo.grpc.compaction.protobuf.CancelRequest;
import org.apache.accumulo.grpc.compaction.protobuf.CompactionCoordinatorServiceGrpc;
import org.apache.accumulo.grpc.compaction.protobuf.CompactionCoordinatorServiceGrpc.CompactionCoordinatorServiceBlockingStub;
import org.apache.accumulo.grpc.compaction.protobuf.GetRunningCompactionsRequest;
import org.apache.accumulo.grpc.compaction.protobuf.PExternalCompactionList;
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
    CompactionCoordinatorServiceBlockingStub coordinatorClient = null;
    ecid = ExternalCompactionId.from(ecid).canonical();
    try {
      coordinatorClient = getCoordinatorClient(context);
      var ignored =
          coordinatorClient.cancel(CancelRequest.newBuilder().setPtinfo(TraceUtil.protoTraceInfo())
              .setCredentials(context.gRpcCreds()).setExternalCompactionId(ecid).build());
      System.out.println("Cancel sent to coordinator for " + ecid);
    } catch (Exception e) {
      throw new IllegalStateException("Exception calling cancel compaction for " + ecid, e);
    } finally {
      // TODO: cleanup if using grpc pooling in future
      // ThriftUtil.returnClient(coordinatorClient, context);
    }
  }

  private void listCompactorsByQueue(ServerContext context) {
    var groupToCompactorsMap = ExternalCompactionUtil.getCompactorAddrs(context);
    if (groupToCompactorsMap.isEmpty()) {
      System.out.println("No Compactors found.");
    } else {
      groupToCompactorsMap.forEach((q, compactors) -> System.out.println(q + ": " + compactors));
    }
  }

  private void runningCompactions(ServerContext context, boolean details) {
    CompactionCoordinatorServiceBlockingStub coordinatorClient = null;
    PExternalCompactionList running;
    try {
      coordinatorClient = getCoordinatorClient(context);
      running = coordinatorClient.getRunningCompactions(GetRunningCompactionsRequest.newBuilder()
          .setPtinfo(TraceUtil.protoTraceInfo()).setCredentials(context.gRpcCreds()).build());
      if (running == null) {
        System.out.println("No running compactions found.");
        return;
      }
      var ecidMap = running.getCompactionsMap();
      if (ecidMap == null) {
        System.out.println("No running compactions found.");
        return;
      }
      ecidMap.forEach((ecid, ec) -> {
        if (ec != null) {
          var runningCompaction = new RunningCompaction(ec);
          var addr = runningCompaction.getCompactorAddress();
          var kind = runningCompaction.getJob().getKind();
          var group = runningCompaction.getGroupName();
          var ke = KeyExtent.fromProtobuf(runningCompaction.getJob().getExtent());
          System.out.format("%s %s %s %s TableId: %s\n", ecid, addr, kind, group, ke.tableId());
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
      throw new IllegalStateException("Unable to get running compactions.", e);
    } finally {
      // TODO: clean up if we use pooling with grpc
      // ThriftUtil.returnClient(coordinatorClient, context);
    }
  }

  private CompactionCoordinatorServiceBlockingStub getCoordinatorClient(ServerContext context) {
    var coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(context);
    if (coordinatorHost.isEmpty()) {
      throw new IllegalStateException("Unable to find coordinator. Check that it is running.");
    }
    HostAndPort address = coordinatorHost.orElseThrow();
    CompactionCoordinatorServiceBlockingStub coordinatorClient;
    try {
      // TODO: coordinatorHost contains the Thrift port so right now only host is used.
      // we eventually need the gRPC port and will need to store than in Zk.
      // GrpcUtil for now just uses the property in the context for the port
      coordinatorClient = CompactionCoordinatorServiceGrpc
          .newBlockingStub(GrpcUtil.getChannel(coordinatorHost.orElseThrow(), context));
    } catch (Exception e) {
      throw new IllegalStateException("Unable to get Compaction coordinator at " + address, e);
    }
    System.out.println("Connected to coordinator at " + address);
    return coordinatorClient;
  }
}
