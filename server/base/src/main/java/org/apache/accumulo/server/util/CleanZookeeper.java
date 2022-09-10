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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanZookeeper {

  private static final Logger log = LoggerFactory.getLogger(CleanZookeeper.class);

  public static void execute(ServerContext context, String auth) throws Exception {

    try {
      final String root = Constants.ZROOT;
      final ZooReaderWriter zk = context.getZooReaderWriter();
      if (auth != null) {
        ZooUtil.digestAuth(zk.getZooKeeper(), auth);
      }

      for (String child : zk.getChildren(root)) {
        if (Constants.ZINSTANCES.equals("/" + child)) {
          for (String instanceName : zk.getChildren(root + Constants.ZINSTANCES)) {
            String instanceNamePath = root + Constants.ZINSTANCES + "/" + instanceName;
            byte[] id = zk.getData(instanceNamePath);
            if (id != null && !new String(id, UTF_8).equals(context.getInstanceID().canonical())) {
              delete(zk, instanceNamePath);
              System.out.println("Deleted instance " + instanceName);
            }
          }
        } else if (!child.equals(context.getInstanceID().canonical())) {
          delete(zk, root + "/" + child);
        }
      }
    } catch (Exception ex) {
      System.out.println("Error Occurred: " + ex);
    }
  }

  private static void delete(final ZooReaderWriter zk, final String path)
      throws InterruptedException, KeeperException {
    try {
      zk.recursiveDelete(path, NodeMissingPolicy.SKIP);
    } catch (KeeperException.NoAuthException ex) {
      log.warn("Unable to delete {}", path);
    }
  }

}
