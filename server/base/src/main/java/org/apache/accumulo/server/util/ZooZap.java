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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

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
    @Deprecated(since = "2.1.0")
    @Parameter(names = "-master",
        description = "remove master locks (deprecated -- user -manager instead")
    boolean zapMaster = false;
    @Parameter(names = "-manager", description = "remove manager locks")
    boolean zapManager = false;
    @Parameter(names = "-tservers", description = "remove tablet server locks")
    boolean zapTservers = false;
    @Parameter(names = "-compaction-coordinators",
        description = "remove compaction coordinator locks")
    boolean zapCoordinators = false;
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
    try {
      var siteConf = SiteConfiguration.auto();
      // Login as the server on secure HDFS
      if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        SecurityUtil.serverLogin(siteConf);
      }
      zap(siteConf, args);
    } finally {
      SingletonManager.setMode(Mode.CLOSED);
    }
  }

  public void zap(SiteConfiguration siteConf, String... args) {
    Opts opts = new Opts();
    opts.parseArgs(keyword(), args);

    final Predicate<String> groupPredicate;
    final Predicate<HostAndPort> hostPortPredicate;

    if (opts.hostPortExcludeFile != null) {
      try {
        var hostPorts = Files.lines(java.nio.file.Path.of(opts.hostPortExcludeFile))
            .map(String::trim).map(HostAndPort::fromString).collect(Collectors.toSet());
        hostPortPredicate = hp -> !hostPorts.contains(hp);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    } else {
      hostPortPredicate = hp -> true;
    }

    if (opts.includeGroups != null) {
      var groups = Arrays.stream(opts.includeGroups.split(",")).map(String::trim)
          .collect(Collectors.toSet());
      groupPredicate = groups::contains;
    } else {
      groupPredicate = g -> true;
    }

    if (!opts.zapMaster && !opts.zapManager && !opts.zapTservers && !opts.zapCompactors
        && !opts.zapCoordinators && !opts.zapScanServers && !opts.zapGc && !opts.zapMonitor) {
      new JCommander(opts).usage();
      return;
    }

    String volDir = VolumeConfiguration.getVolumeUris(siteConf).iterator().next();
    Path instanceDir = new Path(volDir, "instance_id");
    InstanceId iid = VolumeManager.getInstanceIDFromHdfs(instanceDir, new Configuration());
    ZooReaderWriter zoo = new ZooReaderWriter(siteConf);

    if (opts.zapMaster) {
      log.warn("The -master option is deprecated. Please use -manager instead.");
    }
    if (opts.zapManager || opts.zapMaster) {
      String managerLockPath = Constants.ZROOT + "/" + iid + Constants.ZMANAGER_LOCK;

      try {
        removeSingletonLock(zoo, managerLockPath, hostPortPredicate, opts);
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting manager lock", e);
      }
    }

    if (opts.zapGc) {
      String gcLockPath = Constants.ZROOT + "/" + iid + Constants.ZGC_LOCK;
      try {
        removeSingletonLock(zoo, gcLockPath, hostPortPredicate, opts);
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting manager lock", e);
      }
    }

    if (opts.zapMonitor) {
      String monitorLockPath = Constants.ZROOT + "/" + iid + Constants.ZMONITOR_LOCK;
      try {
        removeSingletonLock(zoo, monitorLockPath, hostPortPredicate, opts);
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting monitor lock", e);
      }
    }

    if (opts.zapTservers) {
      String tserversPath = Constants.ZROOT + "/" + iid + Constants.ZTSERVERS;
      try {
        if ((opts.zapManager || opts.zapMaster) && opts.hostPortExcludeFile == null
            && !opts.dryRun) {
          // When shutting down all tablet servers and the manager, then completely clean up all
          // tservers entries in zookeeper
          List<String> children = zoo.getChildren(tserversPath);
          for (String child : children) {
            message("Deleting " + tserversPath + "/" + child + " from zookeeper", opts);
            zoo.recursiveDelete(tserversPath + "/" + child, NodeMissingPolicy.SKIP);
          }
        } else {
          ServiceLock.deleteLocks(zoo, tserversPath, hostPortPredicate, m -> message(m, opts),
              opts.dryRun);
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting tserver locks", e);
      }
    }

    // Remove the tracers, we don't use them anymore.
    @SuppressWarnings("deprecation")
    String path = siteConf.get(Property.TRACE_ZK_PATH);
    try {
      zapDirectory(zoo, path, opts);
    } catch (Exception e) {
      // do nothing if the /tracers node does not exist.
    }

    if (opts.zapCoordinators) {
      final String coordinatorPath = Constants.ZROOT + "/" + iid + Constants.ZCOORDINATOR_LOCK;
      try {
        removeSingletonLock(zoo, coordinatorPath, hostPortPredicate, opts);
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting coordinator from zookeeper", e);
      }
    }

    if (opts.zapCompactors) {
      String compactorsBasepath = Constants.ZROOT + "/" + iid + Constants.ZCOMPACTORS;
      try {
        removeCompactorGroupedLocks(zoo, compactorsBasepath, groupPredicate, hostPortPredicate,
            opts);
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting compactors from zookeeper", e);
      }

    }

    if (opts.zapScanServers) {
      String sserversPath = Constants.ZROOT + "/" + iid + Constants.ZSSERVERS;
      try {
        if (opts.includeGroups == null) {
          removeLocks(zoo, sserversPath, hostPortPredicate, opts);
        } else {
          removeScanServerGroupLocks(zoo, sserversPath, hostPortPredicate, groupPredicate, opts);
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting scan server locks", e);
      }
    }
  }

  private static void zapDirectory(ZooReaderWriter zoo, String path, Opts opts)
      throws KeeperException, InterruptedException {
    List<String> children = zoo.getChildren(path);
    for (String child : children) {
      message("Deleting " + path + "/" + child + " from zookeeper", opts);
      if (!opts.dryRun) {
        zoo.recursiveDelete(path + "/" + child, NodeMissingPolicy.SKIP);
      }
    }
  }

  static void removeCompactorGroupedLocks(ZooReaderWriter zoo, String path,
      Predicate<String> groupPredicate, Predicate<HostAndPort> hostPortPredicate, Opts opts)
      throws KeeperException, InterruptedException {
    if (zoo.exists(path)) {
      List<String> groups = zoo.getChildren(path);
      for (String group : groups) {
        if (groupPredicate.test(group)) {
          ServiceLock.deleteLocks(zoo, path + "/" + group, hostPortPredicate, m -> message(m, opts),
              opts.dryRun);
        }
      }
    }
  }

  static void removeLocks(ZooReaderWriter zoo, String path,
      Predicate<HostAndPort> hostPortPredicate, Opts opts)
      throws KeeperException, InterruptedException {
    ServiceLock.deleteLocks(zoo, path, hostPortPredicate, m -> message(m, opts), opts.dryRun);
  }

  @Deprecated(since = "2.1.5")
  static void removeScanServerGroupLocks(ZooReaderWriter zoo, String path,
      Predicate<HostAndPort> hostPortPredicate, Predicate<String> groupPredicate, Opts opts)
      throws KeeperException, InterruptedException {
    ServiceLock.deleteScanServerLocks(zoo, path, hostPortPredicate, groupPredicate,
        m -> message(m, opts), opts.dryRun);
  }

  static void removeSingletonLock(ZooReaderWriter zoo, String path,
      Predicate<HostAndPort> hostPortPredicate, Opts ops)
      throws KeeperException, InterruptedException {
    var lockData = ServiceLock.getLockData(zoo.getZooKeeper(), ServiceLock.path(path));
    if (lockData != null
        && hostPortPredicate.test(HostAndPort.fromString(new String(lockData, UTF_8)))) {
      zapDirectory(zoo, path, ops);
    }
  }

}
