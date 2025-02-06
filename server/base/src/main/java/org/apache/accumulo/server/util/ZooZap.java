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

import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.core.zookeeper.ZooSession;
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
    @Parameter(names = "-verbose", description = "print out messages about progress")
    boolean verbose = false;
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

    if (!opts.zapManager && !opts.zapTservers) {
      new JCommander(opts).usage();
      return;
    }

    try (var zk = new ZooSession(getClass().getSimpleName(), siteConf)) {
      // Login as the server on secure HDFS
      if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        SecurityUtil.serverLogin(siteConf);
      }

      String volDir = VolumeConfiguration.getVolumeUris(siteConf).iterator().next();
      Path instanceDir = new Path(volDir, "instance_id");
      InstanceId iid = VolumeManager.getInstanceIDFromHdfs(instanceDir, new Configuration());
      var zrw = zk.asReaderWriter();

      if (opts.zapManager) {
        String managerLockPath = Constants.ZMANAGER_LOCK;

        try {
          zapDirectory(zrw, managerLockPath, opts);
        } catch (KeeperException | InterruptedException e) {
          e.printStackTrace();
        }
      }

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
              }
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
  }

  private static void zapDirectory(ZooReaderWriter zoo, String path, Opts opts)
      throws KeeperException, InterruptedException {
    List<String> children = zoo.getChildren(path);
    for (String child : children) {
      message("Deleting " + path + "/" + child + " from zookeeper", opts);
      zoo.recursiveDelete(path + "/" + child, NodeMissingPolicy.SKIP);
    }
  }
}
