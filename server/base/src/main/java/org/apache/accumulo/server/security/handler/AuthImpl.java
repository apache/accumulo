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

import static org.apache.accumulo.server.security.handler.SecurityModule.ZKUserAuths;
import static org.apache.accumulo.server.security.handler.SecurityModule.ZKUserNamespacePerms;
import static org.apache.accumulo.server.security.handler.SecurityModule.ZKUserSysPerms;
import static org.apache.accumulo.server.security.handler.SecurityModule.ZKUserTablePerms;

import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthImpl implements Auth {
  private static final Logger log = LoggerFactory.getLogger(AuthImpl.class);
  private ZooCache zooCache;
  private ServerContext context;
  private String ZKUserPath;

  public AuthImpl(ZooCache zooCache, ServerContext context, String ZKUserPath) {
    this.zooCache = zooCache;
    this.context = context;
    this.ZKUserPath = ZKUserPath;
  }

  @Override
  public boolean authenticate(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    if (!(token instanceof PasswordToken))
      throw new AccumuloSecurityException(principal, SecurityErrorCode.INVALID_TOKEN);
    PasswordToken pt = (PasswordToken) token;
    byte[] pass;
    String zpath = ZKUserPath + "/" + principal;
    pass = zooCache.get(zpath);
    boolean result = ZKSecurityTool.checkPass(pt.getPassword(), pass);
    if (!result) {
      zooCache.clear(zpath);
      pass = zooCache.get(zpath);
      result = ZKSecurityTool.checkPass(pt.getPassword(), pass);
    }
    return result;
  }

  @Override
  public Authorizations getAuthorizations(String user) {
    byte[] authsBytes = zooCache.get(ZKUserPath + "/" + user + ZKUserAuths);
    if (authsBytes != null)
      return ZKSecurityTool.convertAuthorizations(authsBytes);
    return Authorizations.EMPTY;
  }

  @Override
  public boolean hasAuths(String user, Authorizations auths) {
    if (auths.isEmpty()) {
      // avoid deserializing auths from ZK cache
      return true;
    }

    Authorizations userauths = getAuthorizations(user);

    for (byte[] auth : auths.getAuthorizations()) {
      if (!userauths.contains(auth)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public void changeAuthorizations(String user, Authorizations authorizations)
      throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        zooCache.clear();
        context.getZooReaderWriter().putPersistentData(ZKUserPath + "/" + user + ZKUserAuths,
            ZKSecurityTool.convertAuthorizations(authorizations),
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
  public void changePassword(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    if (!(token instanceof PasswordToken))
      throw new AccumuloSecurityException(principal, SecurityErrorCode.INVALID_TOKEN);
    PasswordToken pt = (PasswordToken) token;
    if (userExists(principal)) {
      try {
        synchronized (zooCache) {
          zooCache.clear(ZKUserPath + "/" + principal);
          context.getZooReaderWriter().putPrivatePersistentData(ZKUserPath + "/" + principal,
              ZKSecurityTool.createPass(pt.getPassword()), ZooUtil.NodeExistsPolicy.OVERWRITE);
        }
      } catch (KeeperException e) {
        log.error("{}", e.getMessage(), e);
        throw new AccumuloSecurityException(principal, SecurityErrorCode.CONNECTION_ERROR, e);
      } catch (InterruptedException e) {
        log.error("{}", e.getMessage(), e);
        throw new RuntimeException(e);
      } catch (AccumuloException e) {
        log.error("{}", e.getMessage(), e);
        throw new AccumuloSecurityException(principal, SecurityErrorCode.DEFAULT_SECURITY_ERROR, e);
      }
    } else
      // user doesn't exist
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
  }

  /**
   *
   */
  private void initUser(String user) throws AccumuloSecurityException {
    // Copied from Authorizer.initUser(user)
    IZooReaderWriter zoo = context.getZooReaderWriter();
    try {
      zoo.putPersistentData(ZKUserPath + "/" + user, new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }

    // copied from PermissionHandler.initUser()
    try {
      zoo.putPersistentData(ZKUserPath + "/" + user, new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
      zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms, new byte[0],
          ZooUtil.NodeExistsPolicy.SKIP);
      zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserNamespacePerms, new byte[0],
          ZooUtil.NodeExistsPolicy.SKIP);
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createUser(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    initUser(principal);

    try {
      if (!(token instanceof PasswordToken))
        throw new AccumuloSecurityException(principal, SecurityErrorCode.INVALID_TOKEN);
      PasswordToken pt = (PasswordToken) token;
      constructUser(principal, ZKSecurityTool.createPass(pt.getPassword()));
    } catch (KeeperException e) {
      if (e.code().equals(KeeperException.Code.NODEEXISTS))
        throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_EXISTS, e);
      throw new AccumuloSecurityException(principal, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    } catch (AccumuloException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(principal, SecurityErrorCode.DEFAULT_SECURITY_ERROR, e);
    }
  }

  /**
   * Sets up the user in ZK for the provided user. No checking for existence is done here, it should
   * be done before calling.
   */
  public void constructUser(String user, byte[] pass) throws KeeperException, InterruptedException {
    synchronized (zooCache) {
      zooCache.clear();
      IZooReaderWriter zoo = context.getZooReaderWriter();
      zoo.putPrivatePersistentData(ZKUserPath + "/" + user, pass, ZooUtil.NodeExistsPolicy.FAIL);
    }
  }

  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        zooCache.clear();
        context.getZooReaderWriter().recursiveDelete(ZKUserPath + "/" + user,
            ZooUtil.NodeMissingPolicy.FAIL);
      }
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      if (e.code().equals(KeeperException.Code.NONODE)) {
        throw new AccumuloSecurityException(user, SecurityErrorCode.USER_DOESNT_EXIST, e);
      }
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    }

    cleanUser(user);
  }

  /**
   * Copied from ZKPermHandler.cleanUser()
   */
  private void cleanUser(String user) throws AccumuloSecurityException {
    IZooReaderWriter zoo = context.getZooReaderWriter();

    try {
      synchronized (zooCache) {
        zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserSysPerms,
            ZooUtil.NodeMissingPolicy.SKIP);
        zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserTablePerms,
            ZooUtil.NodeMissingPolicy.SKIP);
        zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserNamespacePerms,
            ZooUtil.NodeMissingPolicy.SKIP);
        zooCache.clear(ZKUserPath + "/" + user);
      }
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      if (e.code().equals(KeeperException.Code.NONODE))
        throw new AccumuloSecurityException(user, SecurityErrorCode.USER_DOESNT_EXIST, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);

    }
  }

  @Override
  public boolean userExists(String user) {
    return zooCache.get(ZKUserPath + "/" + user) != null;
  }

  @Override
  public Set<String> listUsers() {
    return new TreeSet<>(zooCache.getChildren(ZKUserPath));
  }
}
