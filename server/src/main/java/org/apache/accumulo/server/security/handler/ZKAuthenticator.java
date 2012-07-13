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
package org.apache.accumulo.server.security.handler;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

// Utility class for adding all authentication info into ZK
public final class ZKAuthenticator implements Authenticator {
  static final Logger log = Logger.getLogger(ZKAuthenticator.class);
  private static Authenticator zkAuthenticatorInstance = null;

  private String ZKUserPath;
  private final ZooCache zooCache;
  
  public static synchronized Authenticator getInstance() {
    if (zkAuthenticatorInstance == null)
      zkAuthenticatorInstance = new ZKAuthenticator();
    return zkAuthenticatorInstance;
  }
  
  public ZKAuthenticator() {
    zooCache = new ZooCache();
  }
  
  public void initialize(String instanceId) {
    ZKUserPath = Constants.ZROOT + "/" + instanceId + "/users";
  }

  @Override
  public void initializeSecurity(AuthInfo credentials, String rootuser, byte[] rootpass) throws AccumuloSecurityException {
    try {
      // remove old settings from zookeeper first, if any
      IZooReaderWriter zoo = ZooReaderWriter.getRetryingInstance();
      synchronized (zooCache) {
        zooCache.clear();
        if (zoo.exists(ZKUserPath)) {
          zoo.recursiveDelete(ZKUserPath, NodeMissingPolicy.SKIP);
          log.info("Removed " + ZKUserPath + "/" + " from zookeeper");
        }
        
        // prep parent node of users with root username
        zoo.putPersistentData(ZKUserPath, rootuser.getBytes(), NodeExistsPolicy.FAIL);
        
        constructUser(rootuser, ZKSecurityTool.createPass(rootpass));
      }
    } catch (KeeperException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    } catch (AccumuloException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Sets up the user in ZK for the provided user. No checking for existence is done here, it should be done before calling.
   */
  private void constructUser(String user, byte[] pass)
      throws KeeperException, InterruptedException {
    synchronized (zooCache) {
      zooCache.clear();
      IZooReaderWriter zoo = ZooReaderWriter.getRetryingInstance();
      zoo.putPrivatePersistentData(ZKUserPath + "/" + user, pass, NodeExistsPolicy.FAIL);
    }
  }
  
  @Override
  public Set<String> listUsers() {
    return new TreeSet<String>(zooCache.getChildren(ZKUserPath));
  }
  
  /**
   * Creates a user with no permissions whatsoever
   */
  @Override
  public void createUser(String user, byte[] pass) throws AccumuloSecurityException {
    try {
      constructUser(user, ZKSecurityTool.createPass(pass));
    } catch (KeeperException e) {
      log.error(e, e);
      if (e.code().equals(KeeperException.Code.NODEEXISTS))
        throw new AccumuloSecurityException(user, SecurityErrorCode.USER_EXISTS, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    } catch (AccumuloException e) {
      log.error(e, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.DEFAULT_SECURITY_ERROR, e);
    }
  }
  
  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        zooCache.clear();
        ZooReaderWriter.getRetryingInstance().recursiveDelete(ZKUserPath + "/" + user, NodeMissingPolicy.FAIL);
      }
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      log.error(e, e);
      if (e.code().equals(KeeperException.Code.NONODE))
        throw new AccumuloSecurityException(user, SecurityErrorCode.USER_DOESNT_EXIST, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    }
  }
  
  @Override
  public void changePassword(String user, byte[] pass) throws AccumuloSecurityException {
    if (userExists(user)) {
      try {
        synchronized (zooCache) {
          zooCache.clear(ZKUserPath + "/" + user);
          ZooReaderWriter.getRetryingInstance().putPrivatePersistentData(ZKUserPath + "/" + user, ZKSecurityTool.createPass(pass), NodeExistsPolicy.OVERWRITE);
        }
      } catch (KeeperException e) {
        log.error(e, e);
        throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
      } catch (InterruptedException e) {
        log.error(e, e);
        throw new RuntimeException(e);
      } catch (AccumuloException e) {
        log.error(e, e);
        throw new AccumuloSecurityException(user, SecurityErrorCode.DEFAULT_SECURITY_ERROR, e);
      }
    } else
      throw new AccumuloSecurityException(user, SecurityErrorCode.USER_DOESNT_EXIST); // user doesn't exist
  }
  
  /**
   * Checks if a user exists
   */
  @Override
  public boolean userExists(String user) {
    return zooCache.get(ZKUserPath + "/" + user) != null;
  }
  
  @Override
  public void clearCache(String user) {
    zooCache.clear(ZKUserPath + "/" + user);
  }

  @Override
  public boolean validSecurityHandlers(Authorizor auth, PermissionHandler pm) {
    return true;
  }
  
  @Override
  public boolean authenticateUser(String user, ByteBuffer password, String instanceId) {
    byte[] pass;
    String zpath = ZKUserPath + "/" + user;
    pass = zooCache.get(zpath);
    boolean result = ZKSecurityTool.checkPass(ByteBufferUtil.toBytes(password), pass);
    if (!result) {
      zooCache.clear(zpath);
      pass = zooCache.get(zpath);
      result = ZKSecurityTool.checkPass(ByteBufferUtil.toBytes(password), pass);
    }
    return result;
  }
  
  @Override
  public boolean cachesToClear() {
    return true;
  }
}
