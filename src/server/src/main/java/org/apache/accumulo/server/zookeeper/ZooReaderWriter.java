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
package org.apache.accumulo.server.zookeeper;

import java.security.SecurityPermission;
import java.util.List;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooReader;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZooReaderWriter extends ZooReader {
  
  private static SecurityPermission ZOOWRITER_PERMISSION = new SecurityPermission("zookeeperWriterPermission");
  
  private static ZooReaderWriter instance = null;
  private final String auth;
  
  @Override
  public ZooKeeper getZooKeeper() {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(ZOOWRITER_PERMISSION);
    }
    return getSession(keepers, timeout, auth);
  }
  
  public ZooReaderWriter(String string, int timeInMillis, String auth) {
    super(string, timeInMillis);
    this.auth = "acu:" + auth;
  }
  
  public void recursiveDelete(String zPath, NodeMissingPolicy policy) throws KeeperException, InterruptedException {
    ZooUtil.recursiveDelete(getZooKeeper(), zPath, policy);
  }
  
  public void recursiveDelete(String zPath, int version, NodeMissingPolicy policy) throws KeeperException, InterruptedException {
    ZooUtil.recursiveDelete(getZooKeeper(), zPath, version, policy);
  }
  
  /**
   * Create a persistent node with the default ACL
   * 
   * @return true if the node was created or altered; false if it was skipped
   */
  public boolean putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return ZooUtil.putPersistentData(getZooKeeper(), zPath, data, policy);
  }
  
  public boolean putPrivatePersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return ZooUtil.putPrivatePersistentData(getZooKeeper(), zPath, data, policy);
  }
  
  public void putPersistentData(String zPath, byte[] data, int version, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    ZooUtil.putPersistentData(getZooKeeper(), zPath, data, version, policy);
  }
  
  public String putPersistentSequential(String zPath, byte[] data) throws KeeperException, InterruptedException {
    return ZooUtil.putPersistentSequential(getZooKeeper(), zPath, data);
  }
  
  public String putEphemeralSequential(String zPath, byte[] data) throws KeeperException, InterruptedException {
    return ZooUtil.putEphemeralSequential(getZooKeeper(), zPath, data);
  }
  
  public void recursiveCopyPersistent(String source, String destination, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    ZooUtil.recursiveCopyPersistent(getZooKeeper(), source, destination, policy);
  }
  
  public void delete(String path, int version) throws InterruptedException, KeeperException {
    getZooKeeper().delete(path, version);
  }
  
  public interface Mutator {
    byte[] mutate(byte[] currentValue) throws Exception;
  }
  
  public byte[] mutate(String zPath, byte[] createValue, List<ACL> acl, Mutator mutator) throws Exception {
    if (createValue != null) {
      try {
        getZooKeeper().create(zPath, createValue, acl, CreateMode.PERSISTENT);
        return createValue;
      } catch (NodeExistsException ex) {
        // expected
      }
    }
    do {
      Stat stat = new Stat();
      byte[] data = getZooKeeper().getData(zPath, false, stat);
      data = mutator.mutate(data);
      if (data == null) return data;
      try {
        getZooKeeper().setData(zPath, data, stat.getVersion());
        return data;
      } catch (BadVersionException ex) {
        //
      }
    } while (true);
  }
  
  public static synchronized ZooReaderWriter getInstance() {
    if (instance == null) {
      AccumuloConfiguration conf = ServerConfiguration.getSiteConfiguration();
      instance = new ZooReaderWriter(conf.get(Property.INSTANCE_ZK_HOST), (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT),
          conf.get(Property.INSTANCE_SECRET));
    }
    return instance;
  }
  
  public boolean isLockHeld(ZooUtil.LockID lockID) throws KeeperException, InterruptedException {
    return ZooUtil.isLockHeld(getZooKeeper(), lockID);
  }
  
  public void mkdirs(String path) throws KeeperException, InterruptedException {
    if (path.equals("")) return;
    if (!path.startsWith("/")) throw new IllegalArgumentException(path + "does not start with /");
    if (getZooKeeper().exists(path, false) != null) return;
    String parent = path.substring(0, path.lastIndexOf("/"));
    mkdirs(parent);
    putPersistentData(path, new byte[] {}, NodeExistsPolicy.SKIP);
  }
  
}
