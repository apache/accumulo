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

import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.util.ExpectedProcessCounts;
import org.apache.accumulo.core.zookeeper.ZcStat;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class ProcessStatus extends ServerKeywordExecutable<ProcessStatus.ProcessStatusOpts> {
  public ProcessStatus() {
    super(new ProcessStatusOpts());
  }

  static class ProcessStatusOpts extends ServerOpts {
    @Parameter(names = "--include-groups",
        description = "Comma-separated list of resource groups to check (default: all declared groups)")
    String includeGroups;
  }

  @Override
  public String keyword() {
    return "process-status";
  }

  @Override
  public String description() {
    return "Compares declared expected process counts against running compactors and scan servers,"
        + " reporting groups that are below capacity. Requires "
        + Property.GENERAL_EXPECTED_PROCESS_COUNTS.getKey() + " to be configured.";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.PROCESS;
  }

  @Override
  public void execute(JCommander cl, ProcessStatusOpts opts) throws Exception {
    ServerContext context = getServerContext();

    String rawProperty = context.getConfiguration().get(Property.GENERAL_EXPECTED_PROCESS_COUNTS);
    ExpectedProcessCounts expected = ExpectedProcessCounts.parse(rawProperty);

    if (expected.isEmpty()) {
      System.out.println("No expected process counts declared in '"
          + Property.GENERAL_EXPECTED_PROCESS_COUNTS.getKey() + "'.");
      System.out.println("Set this property to enable process health checking.");
      System.out.println("Example value: compactor.default=2,compactor.CTEST=3,sserver.default=1");
      return;
    }

    final ResourceGroupPredicate rgp;
    if (opts.includeGroups != null) {
      Set<ResourceGroupId> groups = Arrays.stream(opts.includeGroups.split(",")).map(String::trim)
          .map(ResourceGroupId::of).collect(Collectors.toSet());
      rgp = groups::contains;
    } else {
      rgp = ResourceGroupPredicate.ANY;
    }

    System.out.printf("%-12s %-20s %-10s %-10s %-6s%n", "Type", "Group", "Expected", "Running",
        "Down");
    System.out.println("-".repeat(63));

    boolean anyDegraded = false;

    for (var typeEntry : expected.all().entrySet()) {
      ServerId.Type serverType = typeEntry.getKey();
      for (var groupEntry : typeEntry.getValue().entrySet()) {
        ResourceGroupId group = groupEntry.getKey();
        OptionalInt maybeExpected = expected.getExpectedCount(serverType, group);
        if (maybeExpected.isEmpty()) {
          continue; // defensive — should not happen since we're iterating expected.all()
        }
        int expectedCount = maybeExpected.getAsInt();
        if (!rgp.test(group)) {
          continue;
        }

        int runningCount = countRunning(context, serverType, group);
        int downCount = Math.max(0, expectedCount - runningCount);
        boolean degraded = downCount > 0;
        if (degraded) {
          anyDegraded = true;
        }

        System.out.printf("%-12s %-20s %-10d %-10d %-6d%s%n", typeName(serverType),
            group.canonical(), expectedCount, runningCount, downCount,
            degraded ? "  ** DEGRADED **" : "");
      }
    }

    System.out.println();
    if (anyDegraded) {
      System.out.println("WARNING: One or more server groups are running below expected capacity.");
    } else {
      System.out.println("All declared server groups are running at expected capacity.");
    }
  }

  // Counts servers of the given type and resource group that currently hold a ZooKeeper lock.
  private int countRunning(ServerContext context, ServerId.Type serverType, ResourceGroupId group) {
    ServiceLockPaths.ResourceGroupPredicate exactGroup =
        ServiceLockPaths.ResourceGroupPredicate.exact(group);
    Set<ServiceLockPath> paths;

    switch (serverType) {
      case COMPACTOR:
        paths = context.getServerPaths().getCompactor(exactGroup, AddressSelector.all(), false);
        break;
      case SCAN_SERVER:
        paths = context.getServerPaths().getScanServer(exactGroup, AddressSelector.all(), false);
        break;
      default:
        return 0;
    }

    int count = 0;
    for (ServiceLockPath path : paths) {
      ZcStat stat = new ZcStat();
      Optional<ServiceLockData> lockData =
          ServiceLock.getLockData(context.getZooCache(), path, stat);
      if (lockData.isPresent()) {
        count++;
      }
    }
    return count;
  }

  private static String typeName(ServerId.Type type) {
    return switch (type) {
      case COMPACTOR -> "compactor";
      case SCAN_SERVER -> "sserver";
      default -> type.name().toLowerCase();
    };
  }
}
