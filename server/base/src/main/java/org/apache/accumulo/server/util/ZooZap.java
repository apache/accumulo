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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
<<<<<<< HEAD
import org.apache.accumulo.core.zookeeper.ZooSession;
=======
>>>>>>> zkchroot-postzkclean
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
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
    @Parameter(names = "-manager", description = "remove manager locks")
    boolean zapManager = false;
    @Parameter(names = "-tservers", description = "remove tablet server locks")
    boolean zapTservers = false;
    @Parameter(names = "-group", description = "limit the zap to a specific resource group",
        arity = 1)
    String resourceGroup = "";
    @Parameter(names = "-compactors", description = "remove compactor locks")
    boolean zapCompactors = false;
    @Parameter(names = "-sservers", description = "remove scan server locks")
    boolean zapScanServers = false;
    @Parameter(names = "-verbose", description = "print out messages about progress")
    boolean verbose = false;
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
    } finally {
      SingletonManager.setMode(Mode.CLOSED);
    }
  }

  public void zap(ServerContext context, String... args) {
    Opts opts = new Opts();
    opts.parseArgs(keyword(), args);

    if (!opts.zapManager && !opts.zapTservers && !opts.zapCompactors && !opts.zapScanServers) {
      new JCommander(opts).usage();
      return;
    }

    var zrw = context.getZooSession().asReaderWriter();

    if (opts.zapManager) {
      String managerLockPath = Constants.ZMANAGER_LOCK;

      try {
        zapDirectory(zrw, managerLockPath, opts);
      } catch (KeeperException | InterruptedException e) {
        e.printStackTrace();
      }
    }

<<<<<<< HEAD
      try (var context = new ServerContext(siteConf)) {
        final ZooReaderWriter zrw = context.getZooSession().asReaderWriter();
        if (opts.zapManager) {
          ServiceLockPath managerLockPath = context.getServerPaths().createManagerPath();
          try {
            zapDirectory(zrw, managerLockPath, opts);
          } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
          }
        }

        ResourceGroupPredicate rgp;
        if (!opts.resourceGroup.isEmpty()) {
          rgp = rg -> rg.equals(opts.resourceGroup);
        } else {
          rgp = rg -> true;
        }

        if (opts.zapTservers) {
          try {
            Set<ServiceLockPath> tserverLockPaths =
                context.getServerPaths().getTabletServer(rgp, AddressSelector.all(), false);
            Set<String> tserverResourceGroupPaths = new HashSet<>();
            tserverLockPaths.forEach(p -> tserverResourceGroupPaths
                .add(p.toString().substring(0, p.toString().lastIndexOf('/'))));
            for (String group : tserverResourceGroupPaths) {
              message("Deleting tserver " + group + " from zookeeper", opts);
              zrw.recursiveDelete(group.toString(), NodeMissingPolicy.SKIP);
            }
          } catch (KeeperException | InterruptedException e) {
            log.error("{}", e.getMessage(), e);
          }
        }

        if (opts.zapCompactors) {
          Set<ServiceLockPath> compactorLockPaths =
              context.getServerPaths().getCompactor(rgp, AddressSelector.all(), false);
          Set<String> compactorResourceGroupPaths = new HashSet<>();
          compactorLockPaths.forEach(p -> compactorResourceGroupPaths
              .add(p.toString().substring(0, p.toString().lastIndexOf('/'))));
          try {
            for (String group : compactorResourceGroupPaths) {
              message("Deleting compactor " + group + " from zookeeper", opts);
              zrw.recursiveDelete(group, NodeMissingPolicy.SKIP);
            }
          } catch (KeeperException | InterruptedException e) {
            log.error("Error deleting compactors from zookeeper, {}", e.getMessage(), e);
          }

        }

        if (opts.zapScanServers) {
          Set<ServiceLockPath> sserverLockPaths =
              context.getServerPaths().getScanServer(rgp, AddressSelector.all(), false);
          Set<String> sserverResourceGroupPaths = new HashSet<>();
          sserverLockPaths.forEach(p -> sserverResourceGroupPaths
              .add(p.toString().substring(0, p.toString().lastIndexOf('/'))));

          try {
            for (String group : sserverResourceGroupPaths) {
              message("Deleting sserver " + group + " from zookeeper", opts);
              zrw.recursiveDelete(group, NodeMissingPolicy.SKIP);
=======
    if (opts.zapTservers) {
      String tserversPath = Constants.ZTSERVERS;
      try {
        List<String> children = zrw.getChildren(tserversPath);
        for (String child : children) {
          message("Deleting " + tserversPath + "/" + child + " from zookeeper", opts);

          if (opts.zapManager) {
            zrw.recursiveDelete(tserversPath + "/" + child, NodeMissingPolicy.SKIP);
          } else {
            var zLockPath = ServiceLock.path(tserversPath + "/" + child);
            if (!zrw.getChildren(zLockPath.toString()).isEmpty()) {
              try {
                ServiceLock.deleteLock(zrw, zLockPath);
              } catch (RuntimeException e) {
                message("Did not delete " + tserversPath + "/" + child, opts);
              }
>>>>>>> zkchroot-postzkclean
            }
          } catch (KeeperException | InterruptedException e) {
            log.error("{}", e.getMessage(), e);
          }
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("{}", e.getMessage(), e);
      }
    }

    if (opts.zapCoordinators) {
      final String coordinatorPath = Constants.ZCOORDINATOR_LOCK;
      try {
        if (zrw.exists(coordinatorPath)) {
          zapDirectory(zrw, coordinatorPath, opts);
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting coordinator from zookeeper, {}", e.getMessage(), e);
      }
    }

    if (opts.zapCompactors) {
      String compactorsBasepath = Constants.ZCOMPACTORS;
      try {
        if (zrw.exists(compactorsBasepath)) {
          List<String> queues = zrw.getChildren(compactorsBasepath);
          for (String queue : queues) {
            message("Deleting " + compactorsBasepath + "/" + queue + " from zookeeper", opts);
            zrw.recursiveDelete(compactorsBasepath + "/" + queue, NodeMissingPolicy.SKIP);
          }
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting compactors from zookeeper, {}", e.getMessage(), e);
      }

    }

    if (opts.zapScanServers) {
      String sserversPath = Constants.ZSSERVERS;
      try {
        if (zrw.exists(sserversPath)) {
          List<String> children = zrw.getChildren(sserversPath);
          for (String child : children) {
            message("Deleting " + sserversPath + "/" + child + " from zookeeper", opts);

            var zLockPath = ServiceLock.path(sserversPath + "/" + child);
            if (!zrw.getChildren(zLockPath.toString()).isEmpty()) {
              ServiceLock.deleteLock(zrw, zLockPath);
            }
          }
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("{}", e.getMessage(), e);
      }
    }

  }

  private static void zapDirectory(ZooReaderWriter zoo, ServiceLockPath path, Opts opts)
      throws KeeperException, InterruptedException {
    List<String> children = zoo.getChildren(path.toString());
    for (String child : children) {
      message("Deleting " + path + "/" + child + " from zookeeper", opts);
      zoo.recursiveDelete(path + "/" + child, NodeMissingPolicy.SKIP);
    }
  }
}
