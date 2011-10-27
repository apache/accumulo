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
package org.apache.accumulo.core.zookeeper;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

public class ZooUtil {
  public enum NodeExistsPolicy {
    SKIP, OVERWRITE, FAIL
  }
  
  public enum NodeMissingPolicy {
    SKIP, CREATE, FAIL
  }
  
  /**
   * This method will delete a node and all its children from zookeeper
   * 
   * @param zPath
   *          the path to delete
   */
  public static void recursiveDelete(String zPath, NodeMissingPolicy policy) throws KeeperException, InterruptedException {
    recursiveDelete(ZooSession.getSession(), zPath, policy);
  }
  
  public static void recursiveDelete(ZooKeeper zk, String zPath, NodeMissingPolicy policy) throws KeeperException, InterruptedException {
    if (policy.equals(NodeMissingPolicy.CREATE))
      throw new IllegalArgumentException(policy.name() + " is invalid for this operation");
    try {
      for (String child : zk.getChildren(zPath, false))
        recursiveDelete(zk, zPath + "/" + child, NodeMissingPolicy.SKIP);
      
      Stat stat;
      if ((stat = zk.exists(zPath, null)) != null)
        zk.delete(zPath, stat.getVersion());
    } catch (KeeperException e) {
      if (policy.equals(NodeMissingPolicy.SKIP) && e.code().equals(KeeperException.Code.NONODE))
        return;
      throw e;
    }
  }
  
  /**
   * Create a persistent node with the default ACL
   * 
   * @return true if the node was created or altered; false if it was skipped
   */
  public static boolean putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return putData(ZooSession.getSession(), zPath, data, CreateMode.PERSISTENT, -1, policy);
  }
  
  public static boolean putPersistentData(String zPath, byte[] data, int version, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return putData(ZooSession.getSession(), zPath, data, CreateMode.PERSISTENT, version, policy);
  }
  
  /**
   * Create a persistent node with the default ACL
   * 
   * @return true if the node was created or altered; false if it was skipped
   */
  public static boolean putPersistentData(ZooKeeper zk, String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return putData(zk, zPath, data, CreateMode.PERSISTENT, -1, policy);
  }
  
  public static boolean putPersistentData(ZooKeeper zk, String zPath, byte[] data, int version, NodeExistsPolicy policy) throws KeeperException,
      InterruptedException {
    return putData(zk, zPath, data, CreateMode.PERSISTENT, version, policy);
  }
  
  private static boolean putData(ZooKeeper zk, String zPath, byte[] data, CreateMode mode, int version, NodeExistsPolicy policy) throws KeeperException,
      InterruptedException {
    if (policy == null)
      policy = NodeExistsPolicy.FAIL;
    
    if (!policy.equals(NodeExistsPolicy.FAIL) && zk.exists(zPath, null) != null) {
      // check for overwrite or skip and node exists
      switch (policy) {
        case SKIP:
          return false;
        case OVERWRITE:
          zk.setData(zPath, data, version);
          return true;
      }
    }
    
    zk.create(zPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    return true;
  }
  
  public static byte[] getData(ZooKeeper zk, String zPath, Stat stat) throws KeeperException, InterruptedException {
    return zk.getData(zPath, false, stat);
  }
  
  public static byte[] getData(String zPath, Stat stat) throws KeeperException, InterruptedException {
    return getData(ZooSession.getSession(), zPath, stat);
  }
  
  public static Stat getStatus(String zPath) throws KeeperException, InterruptedException {
    return getStatus(ZooSession.getSession(), zPath);
  }
  
  public static Stat getStatus(ZooKeeper zk, String zPath) throws KeeperException, InterruptedException {
    return zk.exists(zPath, false);
  }
  
  public static boolean exists(String zPath) throws KeeperException, InterruptedException {
    return getStatus(zPath) != null;
  }
  
  public static boolean exists(ZooKeeper zk, String zPath) throws KeeperException, InterruptedException {
    return getStatus(zk, zPath) != null;
  }
  
  public static void recursiveCopyPersistent(String source, String destination, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    recursiveCopyPersistent(ZooSession.getSession(), source, destination, policy);
  }
  
  public static void recursiveCopyPersistent(ZooKeeper zk, String source, String destination, NodeExistsPolicy policy) throws KeeperException,
      InterruptedException {
    Stat stat = null;
    if (!exists(source))
      throw KeeperException.create(Code.NONODE, source);
    if (exists(destination)) {
      switch (policy) {
        case OVERWRITE:
          break;
        case SKIP:
          return;
        case FAIL:
        default:
          throw KeeperException.create(Code.NODEEXISTS, source);
      }
    }
    
    stat = new Stat();
    byte[] data = zk.getData(source, false, stat);
    if (stat.getEphemeralOwner() == 0) {
      if (data == null)
        throw KeeperException.create(Code.NONODE, source);
      putPersistentData(destination, data, policy);
      if (stat.getNumChildren() > 0)
        for (String child : zk.getChildren(source, false))
          recursiveCopyPersistent(zk, source + "/" + child, destination + "/" + child, policy);
    }
  }
  
  public static String getRoot(Instance instance) {
    return getRoot(instance.getInstanceID());
  }
  
  public static String getRoot(String instanceId) {
    return Constants.ZROOT + "/" + instanceId;
  }
  
}
