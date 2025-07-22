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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;
import com.google.common.net.HostAndPort;

@AutoService(KeywordExecutable.class)
public class ZooZap implements KeywordExecutable {
  private static final Logger log = LoggerFactory.getLogger(ZooZap.class);

  private static void message(String msg, Opts opts) {
    if (opts.verbose) {
      System.out.println(msg);
    }
  }

  @Override
  public String keyword() {
    return "zoo-zap";
  }

  @Override
  public String description() {
    return "Utility for zapping Zookeeper locks";
  }

  static class Opts extends Help {
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

  public static void main(String[] args) throws Exception {
    new ZooZap().execute(args);
  }

  @Override
  public void execute(String[] args) throws Exception {
    var siteConf = SiteConfiguration.auto();
    try (var context = new ServerContext(siteConf)) {
      // Login as the server on secure HDFS
      if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        SecurityUtil.serverLogin(siteConf);
      }
      zap(context, args);
    }
  }

  public void zap(ServerContext context, String... args) {
    Opts opts = new Opts();
    opts.parseArgs(keyword(), args);

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
      rgp = g -> true;
    }

    if (!opts.zapManager && !opts.zapTservers && !opts.zapCompactors && !opts.zapScanServers
        && !opts.zapGc && !opts.zapMonitor) {
      new JCommander(opts).usage();
      return;
    }

    var zrw = context.getZooSession().asReaderWriter();
    if (opts.zapManager) {
      ServiceLockPath managerLockPath = context.getServerPaths().createManagerPath();
      try {
        removeSingletonLock(zrw, managerLockPath, addressSelector, opts);
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting manager lock", e);
      }
    }

    if (opts.zapGc) {
      ServiceLockPath gcLockPath = context.getServerPaths().createGarbageCollectorPath();
      try {
        removeSingletonLock(zrw, gcLockPath, addressSelector, opts);
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting manager lock", e);
      }
    }

    if (opts.zapMonitor) {
      ServiceLockPath monitorLockPath = context.getServerPaths().createMonitorPath();
      try {
        removeSingletonLock(zrw, monitorLockPath, addressSelector, opts);
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting monitor lock", e);
      }
    }

    if (opts.zapTservers) {
      try {
        Set<ServiceLockPath> tserverLockPaths =
            context.getServerPaths().getTabletServer(rgp, addressSelector, false);
        Set<String> tserverResourceGroupPaths = new HashSet<>();
        tserverLockPaths.forEach(p -> tserverResourceGroupPaths
            .add(p.toString().substring(0, p.toString().lastIndexOf('/'))));
        for (String group : tserverResourceGroupPaths) {
          message("Deleting tserver " + group + " from zookeeper", opts);
          zrw.recursiveDelete(group.toString(), NodeMissingPolicy.SKIP);
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting tserver locks", e);
      }
    }

    if (opts.zapCompactors) {
      Set<ServiceLockPath> compactorLockPaths =
          context.getServerPaths().getCompactor(rgp, addressSelector, false);
      Set<String> compactorResourceGroupPaths = new HashSet<>();
      compactorLockPaths.forEach(p -> compactorResourceGroupPaths
          .add(p.toString().substring(0, p.toString().lastIndexOf('/'))));
      try {
        for (String group : compactorResourceGroupPaths) {
          message("Deleting compactor " + group + " from zookeeper", opts);
          zrw.recursiveDelete(group, NodeMissingPolicy.SKIP);
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting compactors from zookeeper", e);
      }

    }

    if (opts.zapScanServers) {
      Set<ServiceLockPath> sserverLockPaths =
          context.getServerPaths().getScanServer(rgp, addressSelector, false);
      Set<String> sserverResourceGroupPaths = new HashSet<>();
      sserverLockPaths.forEach(p -> sserverResourceGroupPaths
          .add(p.toString().substring(0, p.toString().lastIndexOf('/'))));

      try {
        for (String group : sserverResourceGroupPaths) {
          message("Deleting sserver " + group + " from zookeeper", opts);
          zrw.recursiveDelete(group, NodeMissingPolicy.SKIP);
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting scan server locks", e);
      }
    }
  }

  private static void zapDirectory(ZooReaderWriter zoo, ServiceLockPath path, Opts opts)
      throws KeeperException, InterruptedException {
    List<String> children = zoo.getChildren(path.toString());
    for (String child : children) {
      message("Deleting " + path + "/" + child + " from zookeeper", opts);
      if (!opts.dryRun) {
        zoo.recursiveDelete(path + "/" + child, NodeMissingPolicy.SKIP);
      }
    }
  }

  private static void removeSingletonLock(ZooReaderWriter zoo, ServiceLockPath path,
      AddressSelector addressSelector, Opts ops) throws KeeperException, InterruptedException {
    if (addressSelector.getPredicate().test(path.getServer())) {
      zapDirectory(zoo, path, ops);
    }
  }

}
