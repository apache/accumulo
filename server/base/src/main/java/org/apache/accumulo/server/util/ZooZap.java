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
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
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
    Opts opts = new Opts();
    opts.parseArgs(keyword(), args);

    if (!opts.zapManager && !opts.zapTservers) {
      new JCommander(opts).usage();
      return;
    }

    try {
      var siteConf = SiteConfiguration.auto();
      // Login as the server on secure HDFS
      if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        SecurityUtil.serverLogin(siteConf);
      }

      try (var context = new ServerContext(siteConf)) {
        final ZooReaderWriter zoo = context.getZooReaderWriter();
        if (opts.zapManager) {
          ServiceLockPath managerLockPath = context.getServerPaths().createManagerPath();
          try {
            zapDirectory(zoo, managerLockPath, opts);
          } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
          }
        }

        if (opts.zapTservers) {
          try {
            Set<ServiceLockPath> tserverLockPaths =
                context.getServerPaths().getTabletServer(Optional.empty(), Optional.empty(), false);
            for (ServiceLockPath tserverPath : tserverLockPaths) {

              message("Deleting " + tserverPath + " from zookeeper", opts);

              if (opts.zapManager) {
                zoo.recursiveDelete(tserverPath.toString(), NodeMissingPolicy.SKIP);
              } else {
                if (!zoo.getChildren(tserverPath.toString()).isEmpty()) {
                  if (!ServiceLock.deleteLock(zoo, tserverPath, "tserver")) {
                    message("Did not delete " + tserverPath, opts);
                  }
                }
              }
            }
          } catch (KeeperException | InterruptedException e) {
            log.error("{}", e.getMessage(), e);
          }
        }

        if (opts.zapCompactors) {
          Set<ServiceLockPath> compactorLockPaths =
              context.getServerPaths().getCompactor(Optional.empty(), Optional.empty(), false);
          Set<String> compactorResourceGroupPaths = new HashSet<>();
          compactorLockPaths.forEach(p -> compactorResourceGroupPaths
              .add(p.toString().substring(0, p.toString().lastIndexOf('/'))));
          try {
            for (String group : compactorResourceGroupPaths) {
              message("Deleting " + group + " from zookeeper", opts);
              zoo.recursiveDelete(group, NodeMissingPolicy.SKIP);
            }
          } catch (KeeperException | InterruptedException e) {
            log.error("Error deleting compactors from zookeeper, {}", e.getMessage(), e);
          }

        }

        if (opts.zapScanServers) {
          try {
            Set<ServiceLockPath> sserverLockPaths =
                context.getServerPaths().getScanServer(Optional.empty(), Optional.empty(), false);
            for (ServiceLockPath sserverPath : sserverLockPaths) {
              message("Deleting " + sserverPath + " from zookeeper", opts);
              if (!zoo.getChildren(sserverPath.toString()).isEmpty()) {
                ServiceLock.deleteLock(zoo, sserverPath);
              }
            }
          } catch (KeeperException | InterruptedException e) {
            log.error("{}", e.getMessage(), e);
          }
        }
      }

    } finally {
      SingletonManager.setMode(Mode.CLOSED);
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
