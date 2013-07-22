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
package org.apache.accumulo.fate.zookeeper;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.SecurityPermission;
import java.util.List;

import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZooReaderWriter extends ZooReader implements IZooReaderWriter {
  
  private static SecurityPermission ZOOWRITER_PERMISSION = new SecurityPermission("zookeeperWriterPermission");
  
  private static ZooReaderWriter instance = null;
  private static IZooReaderWriter retryingInstance = null;
  private final String scheme;
  private final byte[] auth;
  
  @Override
  public ZooKeeper getZooKeeper() {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(ZOOWRITER_PERMISSION);
    }
    return getSession(keepers, timeout, scheme, auth);
  }
  
  public ZooReaderWriter(String string, int timeInMillis, String scheme, byte[] auth) {
    super(string, timeInMillis);
    this.scheme = scheme;
    this.auth = auth;
  }
  
  @Override
  public void recursiveDelete(String zPath, NodeMissingPolicy policy) throws KeeperException, InterruptedException {
    ZooUtil.recursiveDelete(getZooKeeper(), zPath, policy);
  }
  
  @Override
  public void recursiveDelete(String zPath, int version, NodeMissingPolicy policy) throws KeeperException, InterruptedException {
    ZooUtil.recursiveDelete(getZooKeeper(), zPath, version, policy);
  }
  
  /**
   * Create a persistent node with the default ACL
   * 
   * @return true if the node was created or altered; false if it was skipped
   */
  @Override
  public boolean putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return ZooUtil.putPersistentData(getZooKeeper(), zPath, data, policy);
  }
  
  @Override
  public boolean putPrivatePersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return ZooUtil.putPrivatePersistentData(getZooKeeper(), zPath, data, policy);
  }
  
  @Override
  public void putPersistentData(String zPath, byte[] data, int version, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    ZooUtil.putPersistentData(getZooKeeper(), zPath, data, version, policy);
  }
  
  @Override
  public String putPersistentSequential(String zPath, byte[] data) throws KeeperException, InterruptedException {
    return ZooUtil.putPersistentSequential(getZooKeeper(), zPath, data);
  }
  
  @Override
  public String putEphemeralData(String zPath, byte[] data) throws KeeperException, InterruptedException {
    return ZooUtil.putEphemeralData(getZooKeeper(), zPath, data);
  }
  
  @Override
  public String putEphemeralSequential(String zPath, byte[] data) throws KeeperException, InterruptedException {
    return ZooUtil.putEphemeralSequential(getZooKeeper(), zPath, data);
  }
  
  @Override
  public void recursiveCopyPersistent(String source, String destination, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    ZooUtil.recursiveCopyPersistent(getZooKeeper(), source, destination, policy);
  }
  
  @Override
  public void delete(String path, int version) throws InterruptedException, KeeperException {
    getZooKeeper().delete(path, version);
  }
  
  @Override
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
      if (data == null)
        return data;
      try {
        getZooKeeper().setData(zPath, data, stat.getVersion());
        return data;
      } catch (BadVersionException ex) {
        //
      }
    } while (true);
  }
  
  public static synchronized ZooReaderWriter getInstance(String zookeepers, int timeInMillis, String scheme, byte[] auth) {
    if (instance == null)
      instance = new ZooReaderWriter(zookeepers, timeInMillis, scheme, auth);
    return instance;
  }
  
  /**
   * get an instance that retries when zookeeper connection errors occur
   * 
   * @return an instance that retries when Zookeeper connection errors occur.
   */
  public static synchronized IZooReaderWriter getRetryingInstance(String zookeepers, int timeInMillis, String scheme, byte[] auth) {
    
    if (retryingInstance == null) {
      final IZooReaderWriter inst = getInstance(zookeepers, timeInMillis, scheme, auth);
      
      InvocationHandler ih = new InvocationHandler() {
        @Override
        public Object invoke(Object obj, Method method, Object[] args) throws Throwable {
          long retryTime = 250;
          while (true) {
            try {
              return method.invoke(inst, args);
            } catch (InvocationTargetException e) {
              if (e.getCause() instanceof KeeperException.ConnectionLossException) {
                Logger.getLogger(ZooReaderWriter.class).warn("Error connecting to zookeeper, will retry in " + retryTime, e.getCause());
                UtilWaitThread.sleep(retryTime);
                retryTime = Math.min(5000, retryTime + 250);
              } else {
                throw e.getCause();
              }
            }
          }
        }
      };
      
      retryingInstance = (IZooReaderWriter) Proxy.newProxyInstance(ZooReaderWriter.class.getClassLoader(), new Class[] {IZooReaderWriter.class}, ih);
    }
    
    return retryingInstance;
  }
  
  @Override
  public boolean isLockHeld(ZooUtil.LockID lockID) throws KeeperException, InterruptedException {
    return ZooUtil.isLockHeld(getZooKeeper(), lockID);
  }
  
  @Override
  public void mkdirs(String path) throws KeeperException, InterruptedException {
    if (path.equals(""))
      return;
    if (!path.startsWith("/"))
      throw new IllegalArgumentException(path + "does not start with /");
    if (getZooKeeper().exists(path, false) != null)
      return;
    String parent = path.substring(0, path.lastIndexOf("/"));
    mkdirs(parent);
    putPersistentData(path, new byte[] {}, NodeExistsPolicy.SKIP);
  }


}
