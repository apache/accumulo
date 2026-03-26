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
package org.apache.accumulo.server.util.adminCommand;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.server.util.adminCommand.PingServer.PingCommandOpts;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class PingServer extends ServerKeywordExecutable<PingCommandOpts> {

  static class PingCommandOpts extends ServerOpts {
    @Parameter(description = "{<host> ... }")
    List<String> args = new ArrayList<>();
  }

  public PingServer() {
    super(new PingCommandOpts());
  }

  @Override
  public String keyword() {
    return "ping";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.PROCESS;
  }

  @Override
  public String description() {
    return "Ping tablet servers.  If no arguments, pings all.";
  }

  @Override
  public void execute(JCommander cl, PingCommandOpts options) throws Exception {

    ServerContext context = getServerContext();
    InstanceOperations io = context.instanceOperations();

    if (options.args.isEmpty()) {
      io.getServers(ServerId.Type.TABLET_SERVER)
          .forEach(t -> options.args.add(t.toHostPortString()));
    }

    int unreachable = 0;

    for (String tserver : options.args) {
      try {
        io.ping(tserver);
        System.out.println(tserver + " OK");
      } catch (AccumuloException ae) {
        System.out.println(tserver + " FAILED (" + ae.getMessage() + ")");
        unreachable++;
      }
    }

    System.out.printf("\n%d of %d tablet servers unreachable\n\n", unreachable,
        options.args.size());
    if (unreachable > 0) {
      throw new IllegalStateException("Failed to ping some servers.");
    }
  }
}
