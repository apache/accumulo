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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZooUtil {
  public enum NodeExistsPolicy {
    SKIP, OVERWRITE, FAIL
  }
  
  public enum NodeMissingPolicy {
    SKIP, CREATE, FAIL
  }
  
  public static class LockID {
    public long eid;
    public String path;
    public String node;
    
    public LockID(String root, String serializedLID) {
      String sa[] = serializedLID.split("\\$");
      int lastSlash = sa[0].lastIndexOf('/');
      
      if (sa.length != 2 || lastSlash < 0) {
        throw new IllegalArgumentException("Malformed serialized lock id " + serializedLID);
      }
      
      if (lastSlash == 0)
        path = root;
      else
        path = root + "/" + sa[0].substring(0, lastSlash);
      node = sa[0].substring(lastSlash + 1);
      eid = Long.parseLong(sa[1], 16);
    }
    
    public LockID(String path, String node, long eid) {
      this.path = path;
      this.node = node;
      this.eid = eid;
    }
    
    public String serialize(String root) {
      
      return path.substring(root.length()) + "/" + node + "$" + Long.toHexString(eid);
    }
    
    @Override
    public String toString() {
      return " path = " + path + " node = " + node + " eid = " + Long.toHexString(eid);
    }
  }
  
  public static final List<ACL> PRIVATE;
  public static final List<ACL> PUBLIC;
  static {
    PRIVATE = new ArrayList<ACL>();
    PRIVATE.addAll(Ids.CREATOR_ALL_ACL);
    PUBLIC = new ArrayList<ACL>();
    PUBLIC.addAll(PRIVATE);
    PUBLIC.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE));
  }
  
  /**
   * This method will delete a node and all its children from zookeeper
   * 
   * @param zPath
   *          the path to delete
   */
  public static void recursiveDelete(ZooKeeper zk, String zPath, int version, NodeMissingPolicy policy) throws KeeperException, InterruptedException {
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
  
  public static void recursiveDelete(ZooKeeper zk, String zPath, NodeMissingPolicy policy) throws KeeperException, InterruptedException {
    recursiveDelete(zk, zPath, -1, policy);
  }
  
  /**
   * Create a persistent node with the default ACL
   * 
   * @return true if the node was created or altered; false if it was skipped
   */
  public static boolean putPersistentData(ZooKeeper zk, String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return putData(zk, zPath, data, CreateMode.PERSISTENT, -1, policy, PUBLIC);
  }
  
  public static boolean putPersistentData(ZooKeeper zk, String zPath, byte[] data, int version, NodeExistsPolicy policy) throws KeeperException,
      InterruptedException {
    return putData(zk, zPath, data, CreateMode.PERSISTENT, version, policy, PUBLIC);
  }
  
  public static boolean putPersistentData(ZooKeeper zk, String zPath, byte[] data, int version, NodeExistsPolicy policy, List<ACL> acls)
      throws KeeperException, InterruptedException {
    return putData(zk, zPath, data, CreateMode.PERSISTENT, version, policy, acls);
  }
  
  private static boolean putData(ZooKeeper zk, String zPath, byte[] data, CreateMode mode, int version, NodeExistsPolicy policy, List<ACL> acls)
      throws KeeperException, InterruptedException {
    if (policy == null)
      policy = NodeExistsPolicy.FAIL;
    
    while (true) {
      try {
        zk.create(zPath, data, acls, mode);
        return true;
      } catch (NodeExistsException nee) {
        switch (policy) {
          case SKIP:
            return false;
          case OVERWRITE:
            try {
              zk.setData(zPath, data, version);
              return true;
            } catch (NoNodeException nne) {
              // node delete between create call and set data, so try create call again
              continue;
            }
          default:
            throw nee;
        }
      }
    }
  }
  
  public static byte[] getData(ZooKeeper zk, String zPath, Stat stat) throws KeeperException, InterruptedException {
    return zk.getData(zPath, false, stat);
  }
  
  public static Stat getStatus(ZooKeeper zk, String zPath) throws KeeperException, InterruptedException {
    return zk.exists(zPath, false);
  }
  
  public static boolean exists(ZooKeeper zk, String zPath) throws KeeperException, InterruptedException {
    return getStatus(zk, zPath) != null;
  }
  
  public static void recursiveCopyPersistent(ZooKeeper zk, String source, String destination, NodeExistsPolicy policy) throws KeeperException,
      InterruptedException {
    Stat stat = null;
    if (!exists(zk, source))
      throw KeeperException.create(Code.NONODE, source);
    if (exists(zk, destination)) {
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
      putPersistentData(zk, destination, data, policy);
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
  
  public static boolean putPrivatePersistentData(ZooKeeper zk, String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return putData(zk, zPath, data, CreateMode.PERSISTENT, -1, policy, PRIVATE);
  }
  
  public static String putPersistentSequential(ZooKeeper zk, String zPath, byte[] data) throws KeeperException, InterruptedException {
    return zk.create(zPath, data, ZooUtil.PUBLIC, CreateMode.PERSISTENT_SEQUENTIAL);
  }
  
  public static String putEphemeralSequential(ZooKeeper zk, String zPath, byte[] data) throws KeeperException, InterruptedException {
    return zk.create(zPath, data, ZooUtil.PUBLIC, CreateMode.EPHEMERAL_SEQUENTIAL);
  }
  
  public static byte[] getLockData(ZooCache zc, String path) {
    
    List<String> children = zc.getChildren(path);
    
    if (children.size() == 0) {
      return null;
    }
    
    children = new ArrayList<String>(children);
    Collections.sort(children);
    
    String lockNode = children.get(0);
    
    return zc.get(path + "/" + lockNode);
  }
  
  public static boolean isLockHeld(ZooKeeper zk, LockID lid) throws KeeperException, InterruptedException {
    
    List<String> children = zk.getChildren(lid.path, false);
    
    if (children.size() == 0) {
      return false;
    }
    
    Collections.sort(children);
    
    String lockNode = children.get(0);
    if (!lid.node.equals(lockNode))
      return false;
    
    Stat stat = zk.exists(lid.path + "/" + lid.node, false);
    return stat != null && stat.getEphemeralOwner() == lid.eid;
  }
  
}
