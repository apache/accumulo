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

import static org.apache.accumulo.server.security.handler.SecurityModule.ZKUserNamespacePerms;
import static org.apache.accumulo.server.security.handler.SecurityModule.ZKUserSysPerms;
import static org.apache.accumulo.server.security.handler.SecurityModule.ZKUserTablePerms;

import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * These methods all originated in {@link ZKPermHandler}
 */
public class PermImpl implements Perm {
  private static final Logger log = LoggerFactory.getLogger(PermImpl.class);
  private ZooCache zooCache;
  private ServerContext context;
  private String ZKUserPath;
  private String ZKTablePath;
  private String ZKNamespacePath;
  private ZooReaderWriter zoo;

  public PermImpl(ZooCache zooCache, ServerContext context, String ZKUserPath) {
    this.zooCache = zooCache;
    this.context = context;
    this.ZKUserPath = ZKUserPath;
    this.ZKTablePath = ZKSecurityTool.getInstancePath(context.getInstanceID()) + "/tables";
    this.ZKNamespacePath = ZKSecurityTool.getInstancePath(context.getInstanceID()) + "/namespaces";
    this.zoo = context.getZooReaderWriter();
  }

  @Override
  public boolean hasSystem(String user, SystemPermission permission, boolean useCache) {
    if (useCache)
      return hasCachedSystemPermission(user, permission);

    byte[] perms;
    try {
      String path = ZKUserPath + "/" + user + ZKUserSysPerms;
      zoo.sync(path);
      perms = zoo.getData(path, null);
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.NONODE) {
        return false;
      }
      log.warn("Unhandled KeeperException, failing closed for table permission check", e);
      return false;
    } catch (InterruptedException e) {
      log.warn("Unhandled InterruptedException, failing closed for table permission check", e);
      return false;
    }

    if (perms == null)
      return false;
    return ZKSecurityTool.convertSystemPermissions(perms).contains(permission);
  }

  private boolean hasCachedSystemPermission(String user, SystemPermission permission) {
    byte[] perms = zooCache.get(ZKUserPath + "/" + user + ZKUserSysPerms);
    if (perms == null)
      return false;
    return ZKSecurityTool.convertSystemPermissions(perms).contains(permission);
  }

  @Override
  public boolean hasTable(String user, TableId tableId, TablePermission permission,
      boolean useCache) throws TableNotFoundException {
    if (useCache)
      return hasCachedTablePermission(user, tableId, permission);

    byte[] serializedPerms;
    try {
      String path = ZKUserPath + "/" + user + ZKUserTablePerms + "/" + tableId;
      zoo.sync(path);
      serializedPerms = zoo.getData(path, null);
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.NONODE) {
        // maybe the table was just deleted?
        try {
          // check for existence:
          zoo.getData(ZKTablePath + "/" + tableId, null);
          // it's there, you don't have permission
          return false;
        } catch (InterruptedException ex) {
          log.warn("Unhandled InterruptedException, failing closed for table permission check", e);
          return false;
        } catch (KeeperException ex) {
          // not there, throw an informative exception
          if (e.code() == KeeperException.Code.NONODE) {
            throw new TableNotFoundException(null, tableId.canonical(),
                "while checking permissions");
          }
          log.warn("Unhandled InterruptedException, failing closed for table permission check", e);
        }
        return false;
      }
      log.warn("Unhandled KeeperException, failing closed for table permission check", e);
      return false;
    } catch (InterruptedException e) {
      log.warn("Unhandled InterruptedException, failing closed for table permission check", e);
      return false;
    }
    if (serializedPerms != null) {
      return ZKSecurityTool.convertTablePermissions(serializedPerms).contains(permission);
    }
    return false;
  }

  private boolean hasCachedTablePermission(String user, TableId tableId,
      TablePermission permission) {
    byte[] serializedPerms =
        zooCache.get(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + tableId);
    if (serializedPerms != null) {
      return ZKSecurityTool.convertTablePermissions(serializedPerms).contains(permission);
    }
    return false;
  }

  @Override
  public boolean hasNamespace(String user, NamespaceId namespaceId, NamespacePermission permission,
      boolean useCache) throws NamespaceNotFoundException {
    if (useCache)
      return hasCachedNamespacePermission(user, namespaceId, permission);
    byte[] serializedPerms;
    try {
      String path = ZKUserPath + "/" + user + ZKUserNamespacePerms + "/" + namespaceId;
      zoo.sync(path);
      serializedPerms = zoo.getData(path, null);
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.NONODE) {
        // maybe the namespace was just deleted?
        try {
          // check for existence:
          zoo.getData(ZKNamespacePath + "/" + namespaceId, null);
          // it's there, you don't have permission
          return false;
        } catch (InterruptedException ex) {
          log.warn("Unhandled InterruptedException, failing closed for namespace permission check",
              e);
          return false;
        } catch (KeeperException ex) {
          // not there, throw an informative exception
          if (e.code() == KeeperException.Code.NONODE) {
            throw new NamespaceNotFoundException(namespaceId.canonical(), null,
                "while checking permissions");
          }
          log.warn("Unhandled InterruptedException, failing closed for table permission check", e);
        }
        return false;
      }
      log.warn("Unhandled KeeperException, failing closed for table permission check", e);
      return false;
    } catch (InterruptedException e) {
      log.warn("Unhandled InterruptedException, failing closed for table permission check", e);
      return false;
    }
    if (serializedPerms != null) {
      return ZKSecurityTool.convertNamespacePermissions(serializedPerms).contains(permission);
    }
    return false;
  }

  private boolean hasCachedNamespacePermission(String user, NamespaceId namespaceId,
      NamespacePermission permission) {
    byte[] serializedPerms = zooCache
        .get(ZKUserPath + "/" + user + ZKUserNamespacePerms + "/" + namespaceId.canonical());
    if (serializedPerms != null) {
      return ZKSecurityTool.convertNamespacePermissions(serializedPerms).contains(permission);
    }
    return false;
  }

  @Override
  public void grantSystem(String user, SystemPermission permission)
      throws AccumuloSecurityException {
    try {
      byte[] permBytes = zooCache.get(ZKUserPath + "/" + user + ZKUserSysPerms);
      Set<SystemPermission> perms;
      if (permBytes == null) {
        perms = new TreeSet<>();
      } else {
        perms = ZKSecurityTool.convertSystemPermissions(permBytes);
      }

      if (perms.add(permission)) {
        synchronized (zooCache) {
          zooCache.clear();
          zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserSysPerms,
              ZKSecurityTool.convertSystemPermissions(perms), ZooUtil.NodeExistsPolicy.OVERWRITE);
        }
      }
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void revokeSystem(String user, SystemPermission permission)
      throws AccumuloSecurityException {
    byte[] sysPermBytes = zooCache.get(ZKUserPath + "/" + user + ZKUserSysPerms);

    // User had no system permission, nothing to revoke.
    if (sysPermBytes == null)
      return;

    Set<SystemPermission> sysPerms = ZKSecurityTool.convertSystemPermissions(sysPermBytes);

    try {
      if (sysPerms.remove(permission)) {
        synchronized (zooCache) {
          zooCache.clear();
          zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserSysPerms,
              ZKSecurityTool.convertSystemPermissions(sysPerms),
              ZooUtil.NodeExistsPolicy.OVERWRITE);
        }
      }
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void grantTable(String user, TableId tableId, TablePermission permission)
      throws AccumuloSecurityException, TableNotFoundException {
    String tId = tableId.canonical();
    Set<TablePermission> tablePerms;
    byte[] serializedPerms = zooCache.get(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + tId);
    if (serializedPerms != null)
      tablePerms = ZKSecurityTool.convertTablePermissions(serializedPerms);
    else
      tablePerms = new TreeSet<>();

    try {
      if (tablePerms.add(permission)) {
        synchronized (zooCache) {
          zooCache.clear(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + tId);
          zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + tId,
              ZKSecurityTool.convertTablePermissions(tablePerms),
              ZooUtil.NodeExistsPolicy.OVERWRITE);
        }
      }
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void revokeTable(String user, TableId tableId, TablePermission permission)
      throws AccumuloSecurityException, TableNotFoundException {
    String tId = tableId.canonical();
    byte[] serializedPerms = zooCache.get(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + tId);

    // User had no table permission, nothing to revoke.
    if (serializedPerms == null)
      return;

    Set<TablePermission> tablePerms = ZKSecurityTool.convertTablePermissions(serializedPerms);
    try {
      if (tablePerms.remove(permission)) {
        zooCache.clear();
        if (tablePerms.size() == 0)
          zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + tId,
              ZooUtil.NodeMissingPolicy.SKIP);
        else
          zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + tId,
              ZKSecurityTool.convertTablePermissions(tablePerms),
              ZooUtil.NodeExistsPolicy.OVERWRITE);
      }
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void grantNamespace(String user, NamespaceId namespaceId, NamespacePermission permission)
      throws AccumuloSecurityException {
    String nId = namespaceId.canonical();
    Set<NamespacePermission> namespacePerms;
    byte[] serializedPerms =
        zooCache.get(ZKUserPath + "/" + user + ZKUserNamespacePerms + "/" + nId);
    if (serializedPerms != null)
      namespacePerms = ZKSecurityTool.convertNamespacePermissions(serializedPerms);
    else
      namespacePerms = new TreeSet<>();

    try {
      if (namespacePerms.add(permission)) {
        synchronized (zooCache) {
          zooCache.clear(ZKUserPath + "/" + user + ZKUserNamespacePerms + "/" + nId);
          zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserNamespacePerms + "/" + nId,
              ZKSecurityTool.convertNamespacePermissions(namespacePerms),
              ZooUtil.NodeExistsPolicy.OVERWRITE);
        }
      }
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void revokeNamespace(String user, NamespaceId namespaceId, NamespacePermission permission)
      throws AccumuloSecurityException {
    String nId = namespaceId.canonical();
    byte[] serializedPerms =
        zooCache.get(ZKUserPath + "/" + user + ZKUserNamespacePerms + "/" + nId);

    // User had no namespace permission, nothing to revoke.
    if (serializedPerms == null)
      return;

    Set<NamespacePermission> namespacePerms =
        ZKSecurityTool.convertNamespacePermissions(serializedPerms);
    try {
      if (namespacePerms.remove(permission)) {
        zooCache.clear();
        if (namespacePerms.size() == 0)
          zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserNamespacePerms + "/" + nId,
              ZooUtil.NodeMissingPolicy.SKIP);
        else
          zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserNamespacePerms + "/" + nId,
              ZKSecurityTool.convertNamespacePermissions(namespacePerms),
              ZooUtil.NodeExistsPolicy.OVERWRITE);
      }
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public void cleanTableOrNamespace(AbstractId<?> tableOrNs, String zkTableOrNsPerms)
      throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        zooCache.clear();
        for (String user : zooCache.getChildren(ZKUserPath))
          zoo.recursiveDelete(
              ZKUserPath + "/" + user + zkTableOrNsPerms + "/" + tableOrNs.canonical(),
              ZooUtil.NodeMissingPolicy.SKIP);
      }
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException("unknownUser", SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }
}
