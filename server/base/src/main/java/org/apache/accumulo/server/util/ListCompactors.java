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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.JCommander;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class ListCompactors extends ServerKeywordExecutable<ServerUtilOpts> {

  public ListCompactors() {
    super(new ServerUtilOpts());
  }

  @Override
  public String keyword() {
    return "list-compactors";
  }

  @Override
  public String description() {
    return "Displays compactors grouped by resource group";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.PROCESS;
  }

  protected Map<ResourceGroupId,List<ServerId>> listCompactorsByQueue(ServerContext context) {
    Set<ServerId> compactors = context.instanceOperations().getServers(ServerId.Type.COMPACTOR);
    if (compactors.isEmpty()) {
      return Map.of();
    } else {
      Map<ResourceGroupId,List<ServerId>> m = new TreeMap<>();
      compactors.forEach(csi -> {
        m.computeIfAbsent(csi.getResourceGroup(), (r) -> new ArrayList<>()).add(csi);
      });
      return m;
    }
  }

  @Override
  public void execute(JCommander cl, ServerUtilOpts options) throws Exception {
    var map = listCompactorsByQueue(options.getServerContext());
    if (map.isEmpty()) {
      System.out.println("No Compactors found.");
    } else {
      map.forEach((q, c) -> {
        System.out.println(q);
        System.out.println("-".repeat(q.toString().length()));
        c.forEach(s -> System.out.println("\t" + s.toHostPortString()));
      });
    }
  }

}
