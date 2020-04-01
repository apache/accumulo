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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

public class CleanZookeeper {

  private static final Logger log = LoggerFactory.getLogger(CleanZookeeper.class);

  static class Opts extends Help {
    @Parameter(names = {"--password"},
        description = "The system secret, if different than instance.secret in accumulo.properties",
        password = true)
    String auth;
  }

  /**
   * @param args
   *          must contain one element: the address of a zookeeper node a second parameter provides
   *          an additional authentication value
   */
  public static void main(String[] args) {
    Opts opts = new Opts();
    opts.parseArgs(CleanZookeeper.class.getName(), args);

    try (var context = new ServerContext(SiteConfiguration.auto())) {

      String root = Constants.ZROOT;
      ZooReaderWriter zk = context.getZooReaderWriter();
      if (opts.auth != null) {
        zk.getZooKeeper().addAuthInfo("digest", ("accumulo:" + opts.auth).getBytes(UTF_8));
      }

      for (String child : zk.getChildren(root)) {
        if (Constants.ZINSTANCES.equals("/" + child)) {
          for (String instanceName : zk.getChildren(root + Constants.ZINSTANCES)) {
            String instanceNamePath = root + Constants.ZINSTANCES + "/" + instanceName;
            byte[] id = zk.getData(instanceNamePath, null);
            if (id != null && !new String(id, UTF_8).equals(context.getInstanceID())) {
              try {
                zk.recursiveDelete(instanceNamePath, NodeMissingPolicy.SKIP);
              } catch (KeeperException.NoAuthException ex) {
                log.warn("Unable to delete {}", instanceNamePath);
              }
            }
          }
        } else if (!child.equals(context.getInstanceID())) {
          String path = root + "/" + child;
          try {
            zk.recursiveDelete(path, NodeMissingPolicy.SKIP);
          } catch (KeeperException.NoAuthException ex) {
            log.warn("Unable to delete {}", path);
          }
        }
      }
    } catch (Exception ex) {
      System.out.println("Error Occurred: " + ex);
    }
  }

}
