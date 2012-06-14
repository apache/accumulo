/**
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
package org.apache.accumulo.server.security;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class ZKAuthorizor implements Authorizor {
  private static final Logger log = Logger.getLogger(ZKAuthorizor.class);
  private static Authorizor zkAuthorizorInstance = null;

  private final String ZKUserAuths = "/Authorizations";
  private final String ZKUserSysPerms = "/System";
  private final String ZKUserTablePerms = "/Tables";

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
  
  public void initialize(String instanceId) {
    ZKUserPath = Constants.ZROOT + "/" + instanceId + "/users";
  }

  public Authorizations getUserAuthorizations(String user) {
    byte[] authsBytes = zooCache.get(ZKUserPath + "/" + user + ZKUserAuths);
    if (authsBytes != null)
      return ZKSecurityTool.convertAuthorizations(authsBytes);
    return Constants.NO_AUTHS;
  }

  @Override
  public boolean hasTablePermission(String user, String table, TablePermission permission) {
    byte[] serializedPerms = zooCache.get(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table);
    if (serializedPerms != null) {
      return ZKSecurityTool.convertTablePermissions(serializedPerms).contains(permission);
    }
    return false;
  }
  
  @Override
  public void grantSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    try {
      byte[] permBytes = zooCache.get(ZKUserPath + "/" + user + ZKUserSysPerms);
      Set<SystemPermission> perms;
      if (permBytes == null) {
        perms = new TreeSet<SystemPermission>();
      } else {
        perms = ZKSecurityTool.convertSystemPermissions(permBytes);
      }
      
      if (perms.add(permission)) {
        synchronized (zooCache) {
          zooCache.clear();
          ZooReaderWriter.getRetryingInstance().putPersistentData(ZKUserPath + "/" + user + ZKUserSysPerms, ZKSecurityTool.convertSystemPermissions(perms),
              NodeExistsPolicy.OVERWRITE);
        }
      }
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void grantTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException {
    Set<TablePermission> tablePerms;
    byte[] serializedPerms = zooCache.get(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table);
    if (serializedPerms != null)
      tablePerms = ZKSecurityTool.convertTablePermissions(serializedPerms);
    else
      tablePerms = new TreeSet<TablePermission>();
    
    try {
      if (tablePerms.add(permission)) {
        synchronized (zooCache) {
          zooCache.clear(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table);
          IZooReaderWriter zoo = ZooReaderWriter.getRetryingInstance();
          zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table,
              ZKSecurityTool.convertTablePermissions(tablePerms), NodeExistsPolicy.OVERWRITE);
        }
      }
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void revokeSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    byte[] sysPermBytes = zooCache.get(ZKUserPath + "/" + user + ZKUserSysPerms);
    
    // User had no system permission, nothing to revoke.
    if (sysPermBytes == null)
      return;
    
    Set<SystemPermission> sysPerms = ZKSecurityTool.convertSystemPermissions(sysPermBytes);
    
    try {
      if (sysPerms.remove(permission)) {
        synchronized (zooCache) {
          zooCache.clear();
          ZooReaderWriter.getRetryingInstance().putPersistentData(ZKUserPath + "/" + user + ZKUserSysPerms, ZKSecurityTool.convertSystemPermissions(sysPerms),
              NodeExistsPolicy.OVERWRITE);
        }
      }
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException {
    byte[] serializedPerms = zooCache.get(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table);
    
    // User had no table permission, nothing to revoke.
    if (serializedPerms == null)
      return;

    Set<TablePermission> tablePerms = ZKSecurityTool.convertTablePermissions(serializedPerms);
    try {
      if (tablePerms.remove(permission)) {
        zooCache.clear();
        IZooReaderWriter zoo = ZooReaderWriter.getRetryingInstance();
        if (tablePerms.size() == 0)
          zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table, NodeMissingPolicy.SKIP);
        else
          zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table, ZKSecurityTool.convertTablePermissions(tablePerms),
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
  
  @Override
  public void cleanTablePermissions(String table) throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        zooCache.clear();
        IZooReaderWriter zoo = ZooReaderWriter.getRetryingInstance();
        for (String user : zooCache.getChildren(ZKUserPath))
          zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table, NodeMissingPolicy.SKIP);
      }
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException("unknownUser", SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }

  }
  
  @Override
  public boolean validAuthenticator(Authenticator auth) {
    return true;
  }
  
  @Override
  public void initializeSecurity(String rootuser) throws AccumuloSecurityException {
    IZooReaderWriter zoo = ZooReaderWriter.getRetryingInstance();

    // create the root user with all system privileges, no table privileges, and no record-level authorizations
    Set<SystemPermission> rootPerms = new TreeSet<SystemPermission>();
    for (SystemPermission p : SystemPermission.values())
      rootPerms.add(p);
    Map<String,Set<TablePermission>> tablePerms = new HashMap<String,Set<TablePermission>>();
    // Allow the root user to flush the !METADATA table
    tablePerms.put(Constants.METADATA_TABLE_ID, Collections.singleton(TablePermission.ALTER_TABLE));
    
    try {
      initUser(rootuser);
      zoo.putPersistentData(ZKUserPath + "/" + rootuser + ZKUserAuths, ZKSecurityTool.convertAuthorizations(Constants.NO_AUTHS), NodeExistsPolicy.FAIL);
      zoo.putPersistentData(ZKUserPath + "/" + rootuser + ZKUserSysPerms, ZKSecurityTool.convertSystemPermissions(rootPerms), NodeExistsPolicy.FAIL);
      for (Entry<String,Set<TablePermission>> entry : tablePerms.entrySet())
        createTablePerm(rootuser, entry.getKey(), entry.getValue());
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
      zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms, new byte[0], NodeExistsPolicy.SKIP);
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Sets up a new table configuration for the provided user/table. No checking for existence is done here, it should be done before calling.
   */
  private void createTablePerm(String user, String table, Set<TablePermission> perms) throws KeeperException, InterruptedException {
    synchronized (zooCache) {
      zooCache.clear();
      ZooReaderWriter.getRetryingInstance().putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table,
          ZKSecurityTool.convertTablePermissions(perms), NodeExistsPolicy.FAIL);
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
  
  @Override
  public boolean hasSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    byte[] perms = zooCache.get(ZKUserPath + "/" + user + ZKUserSysPerms);
    if (perms == null)
      return false;
    return ZKSecurityTool.convertSystemPermissions(perms).contains(permission);
  }
  
  @Override
  public void clearUserCache(String user) {
    zooCache.clear(ZKUserPath + "/" + user);
  }
  
  @Override
  public void clearTableCache(String table) {
    for (String user : zooCache.getChildren(ZKUserPath))
      zooCache.clear(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table);
  }
  
  @Override
  public void clearCache(String user, String tableId) {
    zooCache.clear(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + tableId);
  }
  
  @Override
  public boolean cachesToClear() {
    return true;
  }
  
  @Override
  public void clearCache(String user, boolean auths, boolean system, Set<String> tables) {
    if (auths)
      zooCache.clear(ZKUserPath + "/" + user + ZKUserAuths);
    
    if (system)
      zooCache.clear(ZKUserPath + "/" + user + ZKUserSysPerms);
    
    for (String table : tables)
      clearCache(user, table);
  }

  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        IZooReaderWriter zoo = ZooReaderWriter.getRetryingInstance();
        zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserAuths, NodeMissingPolicy.SKIP);
        zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserSysPerms, NodeMissingPolicy.SKIP);
        zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserTablePerms, NodeMissingPolicy.SKIP);
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
}
