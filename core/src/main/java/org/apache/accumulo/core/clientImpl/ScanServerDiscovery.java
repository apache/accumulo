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
package org.apache.accumulo.core.clientImpl;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanServerDiscovery {

  private static final Logger LOG = LoggerFactory.getLogger(ScanServerDiscovery.class);

  private static String getDiscoveryRoot(String zooRoot) {
    return zooRoot + Constants.ZSSERVERS_DISCOVERY;
  }

  public static String reserve(String zooRoot, ZooReaderWriter zrw)
      throws KeeperException, InterruptedException {
    String root = getDiscoveryRoot(zooRoot);
    ZooKeeper zk = zrw.getZooKeeper();
    LOG.debug("Looking for scan servers in {}", root);
    for (String child : zrw.getChildren(root)) {
      LOG.debug("Found scan server: {}", child);
      try {
        LOG.debug("Attempting to delete available scan server node: {}", root + "/" + child);
        zk.delete(root + "/" + child, -1);
      } catch (KeeperException e) {
        // ignore the case where the node doesn't exist
        if (e.code() != Code.NONODE) {
          LOG.debug(
              "Node {} could not be deleted, scan server possibly taken by another scan, looking for another",
              child);
          LOG.error("", e);
          continue;
        } else {
          throw e;
        }
      }
      LOG.debug("Using scan server: {}", child);
      return child;
    }
    return null;
  }

  public static void unreserve(String zooRoot, ZooReaderWriter zrw, String hostPort)
      throws KeeperException, InterruptedException {
    String root = getDiscoveryRoot(zooRoot);
    LOG.debug("Adding scan server {} in {}", hostPort, root);
    zrw.putUnsafeEphemeralData(root + "/" + hostPort, new byte[0]);
  }

}
