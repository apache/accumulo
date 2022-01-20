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

public class ScanServerDiscovery {

  private static String getDiscoveryRoot(String zooRoot) {
    return zooRoot + Constants.ZSSERVERS + "/discovery/";
  }

  public static String reserve(String zooRoot, ZooReaderWriter zrw)
      throws KeeperException, InterruptedException {
    String root = getDiscoveryRoot(zooRoot);
    for (String child : zrw.getChildren(root, null)) {
      try {
        zrw.getZooKeeper().delete(root + child, -1);
      } catch (KeeperException e) {
        // ignore the case where the node doesn't exist
        if (e.code() != Code.NONODE) {
          continue;
        } else {
          throw e;
        }
      }
      return child;
    }
    return null;
  }

  public static void unreserve(String zooRoot, ZooReaderWriter zrw, String hostPort)
      throws KeeperException, InterruptedException {
    String root = getDiscoveryRoot(zooRoot);
    zrw.mkdirs(root);
    zrw.putEphemeralData(root + hostPort, new byte[0]);
  }

}
