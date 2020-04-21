/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ZooZap {
  private static final Logger log = LoggerFactory.getLogger(ZooZap.class);

  private static void message(String msg, Opts opts) {
    if (opts.verbose) {
      System.out.println(msg);
    }
  }

  static class Opts extends Help {
    @Parameter(names = "-master", description = "remove master locks")
    boolean zapMaster = false;
    @Parameter(names = "-tservers", description = "remove tablet server locks")
    boolean zapTservers = false;
    @Parameter(names = "-tracers", description = "remove tracer locks")
    boolean zapTracers = false;
    @Parameter(names = "-verbose", description = "print out messages about progress")
    boolean verbose = false;
  }

  public static void main(String[] args) {
    Opts opts = new Opts();
    opts.parseArgs(ZooZap.class.getName(), args);

    if (!opts.zapMaster && !opts.zapTservers && !opts.zapTracers) {
      new JCommander(opts).usage();
      return;
    }

    try {
      var siteConf = SiteConfiguration.auto();
      Configuration hadoopConf = new Configuration();
      // Login as the server on secure HDFS
      if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        SecurityUtil.serverLogin(siteConf);
      }

      String volDir = VolumeConfiguration.getVolumeUris(siteConf, hadoopConf).iterator().next();
      Path instanceDir = new Path(volDir, "instance_id");
      String iid = VolumeManager.getInstanceIDFromHdfs(instanceDir, siteConf, hadoopConf);
      ZooReaderWriter zoo = new ZooReaderWriter(siteConf);

      if (opts.zapMaster) {
        String masterLockPath = Constants.ZROOT + "/" + iid + Constants.ZMASTER_LOCK;

        try {
          zapDirectory(zoo, masterLockPath, opts);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      if (opts.zapTservers) {
        String tserversPath = Constants.ZROOT + "/" + iid + Constants.ZTSERVERS;
        try {
          List<String> children = zoo.getChildren(tserversPath);
          for (String child : children) {
            message("Deleting " + tserversPath + "/" + child + " from zookeeper", opts);

            if (opts.zapMaster) {
              zoo.recursiveDelete(tserversPath + "/" + child, NodeMissingPolicy.SKIP);
            } else {
              String path = tserversPath + "/" + child;
              if (!zoo.getChildren(path).isEmpty()) {
                if (!ZooLock.deleteLock(zoo, path, "tserver")) {
                  message("Did not delete " + tserversPath + "/" + child, opts);
                }
              }
            }
          }
        } catch (Exception e) {
          log.error("{}", e.getMessage(), e);
        }
      }

      if (opts.zapTracers) {
        String path = siteConf.get(Property.TRACE_ZK_PATH);
        try {
          zapDirectory(zoo, path, opts);
        } catch (Exception e) {
          // do nothing if the /tracers node does not exist.
        }
      }
    } finally {
      SingletonManager.setMode(Mode.CLOSED);
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
