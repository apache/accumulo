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

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.CancelCompaction.CancelCommandOpts;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class CancelCompaction extends ServerKeywordExecutable<CancelCommandOpts> {

  static class CancelCommandOpts extends ServerOpts {
    @Parameter(names = "-ecid", description = "<ecid>", required = true)
    String ecid;
  }

  public CancelCompaction() {
    super(new CancelCommandOpts());
  }

  @Override
  public String keyword() {
    return "cancel";
  }

  @Override
  public String description() {
    return "Cancels a compaction";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.COMPACTION;
  }

  protected void cancelCompaction(ServerContext context, String ecid) {
    CompactionCoordinatorService.Client coordinatorClient = null;
    ecid = ExternalCompactionId.from(ecid).canonical();
    try {
      coordinatorClient = ExternalCompactionUtil.getCoordinatorClient(context);
      coordinatorClient.cancel(TraceUtil.traceInfo(), context.rpcCreds(), ecid);
      System.out.println("Cancel sent to coordinator for " + ecid);
    } catch (Exception e) {
      throw new IllegalStateException("Exception calling cancel compaction for " + ecid, e);
    } finally {
      ThriftUtil.returnClient(coordinatorClient, context);
    }
  }

  @Override
  public void execute(JCommander cl, CancelCommandOpts options) throws Exception {
    cancelCompaction(getServerContext(), options.ecid);
  }

}
