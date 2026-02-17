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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.ZooZap.ZapOpts;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;
import com.google.common.net.HostAndPort;

@AutoService(KeywordExecutable.class)
public class ZooZap extends ServerKeywordExecutable<ZapOpts> {
  private static final Logger log = LoggerFactory.getLogger(ZooZap.class);

  private static void message(String msg, ZapOpts opts) {
    if (opts.verbose) {
      System.out.println(msg);
    }
  }

  public ZooZap() {
    super(new ZapOpts());
  }

  @Override
  public String keyword() {
    return "zoo-zap";
  }

  @Override
  public String description() {
    return "Utility for zapping Zookeeper locks";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.PROCESS;
  }

  static class ZapOpts extends ServerOpts {
    @Parameter(names = "-manager", description = "remove manager locks")
    boolean zapManager = false;
    @Parameter(names = "-tservers", description = "remove tablet server locks")
    boolean zapTservers = false;
    @Parameter(names = "-compactors", description = "remove compactor locks")
    boolean zapCompactors = false;
    @Parameter(names = "-sservers", description = "remove scan server locks")
    boolean zapScanServers = false;
    @Parameter(names = "--gc", description = "remove gc server locks")
    boolean zapGc = false;
    @Parameter(names = "--monitor", description = "remove monitor server locks")
    boolean zapMonitor = false;
    @Parameter(names = "-verbose", description = "print out messages about progress")
    boolean verbose = false;
    @Parameter(names = "--include-groups",
        description = "Comma seperated list of resource groups to include")
    String includeGroups;
    @Parameter(names = "--exclude-host-ports",
        description = "File with lines of <host>:<port> to exclude from removal")
    String hostPortExcludeFile;
    @Parameter(names = "--dry-run",
        description = "Only print changes that would be made w/o actually making any change")
    boolean dryRun = false;
  }

  @Override
  public void execute(JCommander cl, ZapOpts opts) throws Exception {
    final AddressSelector addressSelector;

    if (opts.hostPortExcludeFile != null) {
      try {
        var hostPorts = Files.lines(java.nio.file.Path.of(opts.hostPortExcludeFile))
            .map(String::trim).map(HostAndPort::fromString).collect(Collectors.toSet());
        addressSelector =
            AddressSelector.matching(hp -> !hostPorts.contains(HostAndPort.fromString(hp)));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    } else {
      addressSelector = AddressSelector.all();
    }

    final ResourceGroupPredicate rgp;

    if (opts.includeGroups != null) {
      var groups = Arrays.stream(opts.includeGroups.split(",")).map(String::trim)
          .map(ResourceGroupId::of).collect(Collectors.toSet());
      rgp = groups::contains;
    } else {
      rgp = ResourceGroupPredicate.ANY;
    }

    if (!opts.zapManager && !opts.zapTservers && !opts.zapCompactors && !opts.zapScanServers
        && !opts.zapGc && !opts.zapMonitor) {
      new JCommander(opts).usage();
      return;
    }

    var context = getServerContext();
    var zrw = context.getZooSession().asReaderWriter();
    if (opts.zapManager) {
      try {
        ServiceLockPath managerLockPath = context.getServerPaths().createManagerPath();
        filterSingleton(context, managerLockPath, addressSelector)
            .ifPresent(slp -> removeSingletonLock(zrw, slp, opts));
      } catch (RuntimeException e) {
        log.error("Error deleting manager lock", e);
      }
    }

    if (opts.zapGc) {
      try {
        ServiceLockPath gcLockPath = context.getServerPaths().createGarbageCollectorPath();
        filterSingleton(context, gcLockPath, addressSelector)
            .ifPresent(slp -> removeSingletonLock(zrw, slp, opts));
      } catch (RuntimeException e) {
        log.error("Error deleting gc lock", e);
      }
    }

    if (opts.zapMonitor) {
      try {
        ServiceLockPath monitorPath = context.getServerPaths().createMonitorPath();
        filterSingleton(context, monitorPath, addressSelector)
            .ifPresent(slp -> removeSingletonLock(zrw, slp, opts));
      } catch (RuntimeException e) {
        log.error("Error deleting monitor lock", e);
      }
    }

    if (opts.zapTservers) {
      try {
        Set<ServiceLockPath> tserverLockPaths =
            context.getServerPaths().getTabletServer(rgp, addressSelector, false);
        for (var serverLockPath : tserverLockPaths) {
          message("Deleting tserver " + serverLockPath.getServer() + " from zookeeper", opts);
          zrw.recursiveDelete(serverLockPath.toString(), NodeMissingPolicy.SKIP);
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting tserver locks", e);
      }
    }

    if (opts.zapCompactors) {
      Set<ServiceLockPath> compactorLockPaths =
          context.getServerPaths().getCompactor(rgp, addressSelector, false);
      try {
        for (var serverLockPath : compactorLockPaths) {
          message("Deleting compactor " + serverLockPath.getServer() + " from zookeeper", opts);
          zrw.recursiveDelete(serverLockPath.toString(), NodeMissingPolicy.SKIP);
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting compactors from zookeeper", e);
      }

    }

    if (opts.zapScanServers) {
      Set<ServiceLockPath> sserverLockPaths =
          context.getServerPaths().getScanServer(rgp, addressSelector, false);
      try {
        for (var serverLockPath : sserverLockPaths) {
          message("Deleting sserver " + serverLockPath.getServer() + " from zookeeper", opts);
          zrw.recursiveDelete(serverLockPath.toString(), NodeMissingPolicy.SKIP);
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting scan server locks", e);
      }
    }
  }

  private static void zapDirectory(ZooReaderWriter zoo, ServiceLockPath path, ZapOpts opts)
      throws KeeperException, InterruptedException {
    List<String> children = zoo.getChildren(path.toString());
    for (String child : children) {
      message("Deleting " + path + "/" + child + " from zookeeper", opts);
      if (!opts.dryRun) {
        zoo.recursiveDelete(path + "/" + child, NodeMissingPolicy.SKIP);
      }
    }
  }

  private static void removeSingletonLock(ZooReaderWriter zoo, ServiceLockPath path, ZapOpts ops) {
    try {
      zapDirectory(zoo, path, ops);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  public static Optional<ServiceLockPath> filterSingleton(ServerContext context,
      ServiceLockPath path, AddressSelector addressSelector) {
    Optional<ServiceLockData> sld = context.getZooCache().getLockData(path);
    return sld.filter(lockData -> {
      for (var service : ServiceLockData.ThriftService.values()) {
        var address = lockData.getAddress(service);
        if (address != null) {
          return addressSelector.getPredicate().test(address.toString());
        }
      }

      return false;
    }).map(lockData -> path);
  }
}
