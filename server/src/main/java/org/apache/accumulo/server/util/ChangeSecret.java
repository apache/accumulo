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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.zookeeper.ZooReader;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ChangeSecret {
  
  public static void main(String[] args) throws Exception {
    String oldPass = null;
    String newPass = null;
    if (args.length < 2) {
      System.err.println("Usage: accumulo " + ChangeSecret.class.getName() + " oldPass newPass");
      System.exit(-1);
    }
    oldPass = args[0];
    newPass = args[1];
    
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    Instance inst = HdfsZooInstance.getInstance();
    if (!verifyAccumuloIsDown(inst, oldPass))
      System.exit(-1);
    String instanceId = rewriteZooKeeperInstance(inst, oldPass, newPass);
    updateHdfs(fs, inst, instanceId);
    if (oldPass != null) {
      deleteInstance(inst, oldPass);
    }
    System.out.println("New instance id is " + instanceId);
    System.out.println("Be sure to put your new secret in accumulo-site.xml");
  }
  
  interface Visitor {
    void visit(ZooReader zoo, String path) throws Exception;
  }
  
  private static void recurse(ZooReader zoo, String root, Visitor v) {
    try {
      v.visit(zoo, root);
      for (String child : zoo.getChildren(root)) {
        recurse(zoo, root + "/" + child, v);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  private static boolean verifyAccumuloIsDown(Instance inst, String oldPassword) {
    ZooReader zooReader = new ZooReaderWriter(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut(), oldPassword);
    String root = ZooUtil.getRoot(inst);
    final List<String> ephemerals = new ArrayList<String>();
    recurse(zooReader, root, new Visitor() {
      public void visit(ZooReader zoo, String path) throws Exception {
        Stat stat = zoo.getStatus(path);
        if (stat.getEphemeralOwner() != 0)
          ephemerals.add(path);
      }
    });
    if (ephemerals.size() == 0) {
      return true;
    }
    
    System.err.println("The following ephemeral nodes exist, something is still running:");
    for (String path : ephemerals) {
      System.err.println(path);
    }
    return false;
  }
  
  private static String rewriteZooKeeperInstance(final Instance inst, String oldPass, String newPass) throws Exception {
    final ZooReaderWriter orig = new ZooReaderWriter(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut(), oldPass);
    final IZooReaderWriter new_ = new ZooReaderWriter(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut(), newPass);
    final String newInstanceId = UUID.randomUUID().toString();
    String root = ZooUtil.getRoot(inst);
    recurse(orig, root, new Visitor() {
      public void visit(ZooReader zoo, String path) throws Exception {
        String newPath = path.replace(inst.getInstanceID(), newInstanceId);
        byte[] data = zoo.getData(path, null);
        List<ACL> acls = orig.getZooKeeper().getACL(path, new Stat());
        if (acls.containsAll(Ids.READ_ACL_UNSAFE)) {
          new_.putPersistentData(newPath, data, NodeExistsPolicy.FAIL);
        } else {
          // upgrade
          if (acls.contains(Ids.OPEN_ACL_UNSAFE)) {
            // make user nodes private, they contain the user's password
            String parts[] = path.split("/");
            if (parts[parts.length - 2].equals("users")) {
              new_.putPrivatePersistentData(newPath, data, NodeExistsPolicy.FAIL);
            } else {
              // everything else can have the readable acl
              new_.putPersistentData(newPath, data, NodeExistsPolicy.FAIL);
            }
          } else {
            new_.putPrivatePersistentData(newPath, data, NodeExistsPolicy.FAIL);
          }
        }
      }
    });
    String path = "/accumulo/instances/" + inst.getInstanceName();
    orig.recursiveDelete(path, NodeMissingPolicy.SKIP);
    new_.putPersistentData(path, newInstanceId.getBytes(), NodeExistsPolicy.OVERWRITE);
    return newInstanceId;
  }
  
  private static void updateHdfs(FileSystem fs, Instance inst, String newInstanceId) throws IOException {
    fs.delete(ServerConstants.getInstanceIdLocation(), true);
    fs.mkdirs(ServerConstants.getInstanceIdLocation());
    fs.create(new Path(ServerConstants.getInstanceIdLocation(), newInstanceId)).close();
  }
  
  private static void deleteInstance(Instance origInstance, String oldPass) throws Exception {
    IZooReaderWriter orig = new ZooReaderWriter(origInstance.getZooKeepers(), origInstance.getZooKeepersSessionTimeOut(), oldPass);
    orig.recursiveDelete("/accumulo/" + origInstance.getInstanceID(), NodeMissingPolicy.SKIP);
  }
}
