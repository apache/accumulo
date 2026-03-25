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

import java.util.Optional;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.CancelCompaction.CancelCommandOpts;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;
import com.google.common.net.HostAndPort;

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
    System.out.println("Looking for " + ecid + " in metadata table");

    Optional<HostAndPort> compactor = ExternalCompactionUtil.findCompactorRunningCompaction(context,
        ExternalCompactionId.of(ecid));

    if (compactor.isPresent()) {
      var addr = compactor.orElseThrow();
      System.out.println("Asking compactor " + addr + " to cancel " + ecid);
      ExternalCompactionUtil.cancelCompaction(context, addr, ecid);
      System.out.println("Asked compactor " + addr + " to cancel " + ecid);
    } else {
      System.out.println("No compaction found for " + ecid);
    }
  }

  @Override
  public void execute(JCommander cl, CancelCommandOpts options) throws Exception {
    cancelCompaction(getServerContext(), options.ecid);
  }

}
