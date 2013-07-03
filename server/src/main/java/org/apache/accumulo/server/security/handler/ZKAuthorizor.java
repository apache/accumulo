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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class ZKAuthorizor implements Authorizor {
  private static final Logger log = Logger.getLogger(ZKAuthorizor.class);
  private static Authorizor zkAuthorizorInstance = null;
  
  private final String ZKUserAuths = "/Authorizations";
  
  private String ZKUserPath;
  private final ZooCache zooCache;
  
  public static synchronized Authorizor getInstance() {
    if (zkAuthorizorInstance == null)
      zkAuthorizorInstance = new ZKAuthorizor();
    return zkAuthorizorInstance;
  }
  
  public ZKAuthorizor() {
    zooCache = new ZooCache();
  }
  
  public void initialize(String instanceId, boolean initialize) {
    ZKUserPath = ZKSecurityTool.getInstancePath(instanceId) + "/users";
  }
  
  public Authorizations getCachedUserAuthorizations(String user) {
    byte[] authsBytes = zooCache.get(ZKUserPath + "/" + user + ZKUserAuths);
    if (authsBytes != null)
      return ZKSecurityTool.convertAuthorizations(authsBytes);
    return Authorizations.EMPTY;
  }
  
  @Override
  public boolean validSecurityHandlers(Authenticator auth, PermissionHandler pm) {
    return true;
  }
  
  @Override
  public void initializeSecurity(TCredentials itw, String rootuser) throws AccumuloSecurityException {
    IZooReaderWriter zoo = ZooReaderWriter.getRetryingInstance();
    
    // create the root user with all system privileges, no table privileges, and no record-level authorizations
    Set<SystemPermission> rootPerms = new TreeSet<SystemPermission>();
    for (SystemPermission p : SystemPermission.values())
      rootPerms.add(p);
    Map<String,Set<TablePermission>> tablePerms = new HashMap<String,Set<TablePermission>>();
    // Allow the root user to flush the !METADATA table
    tablePerms.put(MetadataTable.ID, Collections.singleton(TablePermission.ALTER_TABLE));
    
    try {
      // prep parent node of users with root username
      if (!zoo.exists(ZKUserPath))
        zoo.putPersistentData(ZKUserPath, rootuser.getBytes(), NodeExistsPolicy.FAIL);
      
      initUser(rootuser);
      zoo.putPersistentData(ZKUserPath + "/" + rootuser + ZKUserAuths, ZKSecurityTool.convertAuthorizations(Authorizations.EMPTY), NodeExistsPolicy.FAIL);
    } catch (KeeperException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  /**
   * @param user
   * @throws AccumuloSecurityException
   */
  public void initUser(String user) throws AccumuloSecurityException {
    IZooReaderWriter zoo = ZooReaderWriter.getRetryingInstance();
    try {
      zoo.putPersistentData(ZKUserPath + "/" + user, new byte[0], NodeExistsPolicy.SKIP);
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        IZooReaderWriter zoo = ZooReaderWriter.getRetryingInstance();
        zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserAuths, NodeMissingPolicy.SKIP);
        zooCache.clear(ZKUserPath + "/" + user);
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
  public void changeAuthorizations(String user, Authorizations authorizations) throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        zooCache.clear();
        ZooReaderWriter.getRetryingInstance().putPersistentData(ZKUserPath + "/" + user + ZKUserAuths, ZKSecurityTool.convertAuthorizations(authorizations),
            NodeExistsPolicy.OVERWRITE);
      }
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
}
