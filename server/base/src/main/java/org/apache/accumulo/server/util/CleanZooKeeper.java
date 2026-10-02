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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.zookeeper.ZcStat;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.CleanZooKeeper.CleanOpts;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class CleanZooKeeper extends ServerKeywordExecutable<CleanOpts> {

  private static final Logger log = LoggerFactory.getLogger(CleanZooKeeper.class);

  public CleanZooKeeper() {
    super(new CleanOpts());
  }

  static class CleanOpts extends ServerOpts {
    @Parameter(names = "--compactors",
        description = "Remove ZooKeeper paths for compactors that are no longer running")
    boolean cleanCompactors = false;

    @Parameter(names = "--sservers",
        description = "Remove ZooKeeper paths for scan servers that are no longer running")
    boolean cleanScanServers = false;

    @Parameter(names = "--include-groups",
        description = "Comma-separated list of resource groups to include (default: all)")
    String includeGroups;

    @Parameter(names = "--dry-run",
        description = "Print paths that would be removed without making any changes")
    boolean dryRun = false;

    @Parameter(names = "--verbose", description = "Print progress messages")
    boolean verbose = false;
  }

  @Override
  public String keyword() {
    return "clean-zk";
  }

  @Override
  public String description() {
    return "Removes ZooKeeper paths for compactors and scan servers that are no longer running."
        + " Run only when permanently changing deployment topology, not during normal operations.";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.ZOOKEEPER;
  }

  @Override
  public void execute(JCommander cl, CleanOpts opts) throws Exception {
    if (!opts.cleanCompactors && !opts.cleanScanServers) {
      new JCommander(opts).usage();
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

    var context = getServerContext();
    var zrw = context.getZooSession().asReaderWriter();

    if (opts.cleanCompactors) {
      cleanCompactors(context, zrw, rgp, opts);
    }
    if (opts.cleanScanServers) {
      cleanScanServers(context, zrw, rgp, opts);
    }
  }

  private void cleanCompactors(ServerContext context, ZooReaderWriter zrw,
      ResourceGroupPredicate rgp, CleanOpts opts) {

    // Remove individual compactor nodes with no active lock
    Set<ServiceLockPath> compactorPaths =
        context.getServerPaths().getCompactor(rgp, AddressSelector.all(), false);

    for (ServiceLockPath path : compactorPaths) {
      ZcStat stat = new ZcStat();
      Optional<ServiceLockData> lockData =
          ServiceLock.getLockData(context.getZooCache(), path, stat);
      if (lockData.isEmpty()) {
        message("Removing empty compactor ZK path: " + path, opts);
        if (!opts.dryRun) {
          try {
            zrw.delete(path.toString());
          } catch (KeeperException.NotEmptyException e) {
            log.debug("Failed to delete compactor ZK node {} because it is not empty,"
                + " likely an expected race condition.", path);
          } catch (KeeperException | InterruptedException e) {
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
            log.warn("Failed to delete compactor ZK node {}", path, e);
          }
        }
      }
    }

    // Remove empty resource group parent nodes under ZCOMPACTORS
    try {
      var groups = zrw.getChildren(Constants.ZCOMPACTORS);
      for (String group : groups) {
        ResourceGroupId rgid = ResourceGroupId.of(group);
        if (!rgp.test(rgid)) {
          continue;
        }
        String groupPath = Constants.ZCOMPACTORS + "/" + group;
        var children = zrw.getChildren(groupPath);
        if (children.isEmpty()) {
          message("Removing empty compactor group ZK path: " + groupPath, opts);
          if (!opts.dryRun) {
            try {
              zrw.delete(groupPath);
            } catch (KeeperException.NotEmptyException e) {
              log.debug("Failed to delete compactor group ZK node {} because it is not empty.",
                  groupPath);
            }
          }
        }
      }
    } catch (KeeperException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      log.warn("Failed to clean up empty compactor group paths", e);
    }
  }

  private void cleanScanServers(org.apache.accumulo.server.ServerContext context,
      ZooReaderWriter zrw, ResourceGroupPredicate rgp, CleanOpts opts) {
    try {
      Set<ServiceLockPath> scanServerPaths =
          context.getServerPaths().getScanServer(rgp, AddressSelector.all(), false);

      for (ServiceLockPath path : scanServerPaths) {
        ZcStat stat = new ZcStat();
        Optional<ServiceLockData> lockData =
            ServiceLock.getLockData(context.getZooCache(), path, stat);
        if (lockData.isEmpty()) {
          message("Removing empty scan server ZK path: " + path, opts);
          if (!opts.dryRun) {
            try {
              zrw.delete(path.toString());
            } catch (KeeperException.NotEmptyException e) {
              log.debug("Failed to delete scan server ZK node {} because it is not empty,"
                  + " likely an expected race condition.", path);
            }
          }
        }
      }
    } catch (KeeperException e) {
      log.error("Exception trying to delete empty scan server ZK paths", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted trying to delete empty scan server ZK paths", e);
    }
  }

  private static void message(String msg, CleanOpts opts) {
    if (opts.verbose || opts.dryRun) {
      System.out.println(msg);
    }
  }
}
