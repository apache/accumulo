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
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class CleanZookeeper {
  
  private static final Logger log = Logger.getLogger(CleanZookeeper.class);
  
  /**
   * @param args
   *          must contain one element: the address of a zookeeper node a second parameter provides an additional authentication value
   * @throws IOException
   *           error connecting to accumulo or zookeeper
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: " + CleanZookeeper.class.getName() + " hostname[:port] [auth]");
      System.exit(1);
    }
    String root = Constants.ZROOT;
    IZooReaderWriter zk = ZooReaderWriter.getInstance();
    if (args.length == 2) {
      zk.getZooKeeper().addAuthInfo("digest", args[1].getBytes());
    }
    
    try {
      for (String child : zk.getChildren(root)) {
        if (Constants.ZINSTANCES.equals("/" + child)) {
          for (String instanceName : zk.getChildren(root + Constants.ZINSTANCES)) {
            String instanceNamePath = root + Constants.ZINSTANCES + "/" + instanceName;
            byte[] id = zk.getData(instanceNamePath, null);
            if (id != null && !new String(id).equals(HdfsZooInstance.getInstance().getInstanceID())) {
              try {
                zk.recursiveDelete(instanceNamePath, NodeMissingPolicy.SKIP);
              } catch (KeeperException.NoAuthException ex) {
                log.warn("Unable to delete " + instanceNamePath);
              }
            }
          }
        } else if (!child.equals(HdfsZooInstance.getInstance().getInstanceID())) {
          String path = root + "/" + child;
          try {
            zk.recursiveDelete(path, NodeMissingPolicy.SKIP);
          } catch (KeeperException.NoAuthException ex) {
            log.warn("Unable to delete " + path);
          }
        }
      }
    } catch (Exception ex) {
      System.out.println("Error Occurred: " + ex);
    }
  }
  
}
