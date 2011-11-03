/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.util;

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class CleanZookeeper {
  
  static void recursivelyDelete(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
    for (String child : zk.getChildren(path, false)) {
      recursivelyDelete(zk, path + "/" + child);
    }
    zk.delete(path, -1);
  }
  
  /**
   * @param args
   *          should contain one element: the address of a zookeeper node
   * @throws IOException
   *           error connecting to accumulo or zookeeper
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: " + CleanZookeeper.class.getName() + " hostname[:port]");
      System.exit(1);
    }
    String root = Constants.ZROOT;
    ZooKeeper zk = new ZooKeeper(args[0], 10000, new Watcher() {
      public void process(WatchedEvent event) {}
    });
    try {
      for (String child : zk.getChildren(root, false)) {
        if (Constants.ZINSTANCES.equals("/" + child)) {
          for (String instanceName : zk.getChildren(root + Constants.ZINSTANCES, false)) {
            String instanceNamePath = root + Constants.ZINSTANCES + "/" + instanceName;
            byte[] id = zk.getData(instanceNamePath, false, null);
            if (id != null && !new String(id).equals(HdfsZooInstance.getInstance().getInstanceID()))
              recursivelyDelete(zk, instanceNamePath);
          }
        } else if (!child.equals(HdfsZooInstance.getInstance().getInstanceID())) {
          recursivelyDelete(zk, root + "/" + child);
        }
      }
    } catch (Exception ex) {
      System.out.println("Error Occurred: " + ex);
    }
  }
  
}
