/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.security.handler;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Utility class for adding all authentication info into ZK
public final class ZKAuthenticator implements Authenticator {
  private static final Logger log = LoggerFactory.getLogger(ZKAuthenticator.class);

  private ServerContext context;
  private String zkUserPath;
  private ZooCache zooCache;

  @Override
  public void initialize(ServerContext context) {
    this.context = context;
    zooCache = new ZooCache(context.getZooReader(), null);
    zkUserPath = context.zkUserPath();
  }

  @Override
  public void initializeSecurity(String principal, byte[] token) {
    try {
      // remove old settings from zookeeper first, if any
      ZooReaderWriter zoo = context.getZooReaderWriter();
      synchronized (zooCache) {
        zooCache.clear();
        if (zoo.exists(zkUserPath)) {
          zoo.recursiveDelete(zkUserPath, NodeMissingPolicy.SKIP);
          log.info("Removed {}/ from zookeeper", zkUserPath);
        }

        // prep parent node of users with root username
        zoo.putPersistentData(zkUserPath, principal.getBytes(UTF_8), NodeExistsPolicy.FAIL);

        constructUser(principal, ZKSecurityTool.createPass(token));
      }
    } catch (KeeperException | AccumuloException | InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new IllegalStateException(e);
    }
  }

  /**
   * Sets up the user in ZK for the provided user. No checking for existence is done here, it should
   * be done before calling.
   */
  private void constructUser(String user, byte[] pass)
      throws KeeperException, InterruptedException {
    synchronized (zooCache) {
      zooCache.clear();
      ZooReaderWriter zoo = context.getZooReaderWriter();
      zoo.putPrivatePersistentData(zkUserPath + "/" + user, pass, NodeExistsPolicy.FAIL);
    }
  }

  @Override
  public Set<String> listUsers() {
    return new TreeSet<>(zooCache.getChildren(zkUserPath));
  }

  @Override
  public void createUser(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    try {
      if (!(token instanceof PasswordToken)) {
        throw new AccumuloSecurityException(principal, SecurityErrorCode.INVALID_TOKEN);
      }
      PasswordToken pt = (PasswordToken) token;
      constructUser(principal, ZKSecurityTool.createPass(pt.getPassword()));
    } catch (KeeperException e) {
      if (e.code().equals(KeeperException.Code.NODEEXISTS)) {
        throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_EXISTS, e);
      }
      throw new AccumuloSecurityException(principal, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new IllegalStateException(e);
    } catch (AccumuloException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(principal, SecurityErrorCode.DEFAULT_SECURITY_ERROR, e);
    }
  }

  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        zooCache.clear();
        context.getZooReaderWriter().recursiveDelete(zkUserPath + "/" + user,
            NodeMissingPolicy.FAIL);
      }
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new IllegalStateException(e);
    } catch (KeeperException e) {
      if (e.code().equals(KeeperException.Code.NONODE)) {
        throw new AccumuloSecurityException(user, SecurityErrorCode.USER_DOESNT_EXIST, e);
      }
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    }
  }

  @Override
  public void changePassword(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    if (!(token instanceof PasswordToken)) {
      throw new AccumuloSecurityException(principal, SecurityErrorCode.INVALID_TOKEN);
    }
    PasswordToken pt = (PasswordToken) token;
    if (userExists(principal)) {
      try {
        synchronized (zooCache) {
          zooCache.clear(zkUserPath + "/" + principal);
          context.getZooReaderWriter().putPrivatePersistentData(zkUserPath + "/" + principal,
              ZKSecurityTool.createPass(pt.getPassword()), NodeExistsPolicy.OVERWRITE);
        }
      } catch (KeeperException e) {
        log.error("{}", e.getMessage(), e);
        throw new AccumuloSecurityException(principal, SecurityErrorCode.CONNECTION_ERROR, e);
      } catch (InterruptedException e) {
        log.error("{}", e.getMessage(), e);
        throw new IllegalStateException(e);
      } catch (AccumuloException e) {
        log.error("{}", e.getMessage(), e);
        throw new AccumuloSecurityException(principal, SecurityErrorCode.DEFAULT_SECURITY_ERROR, e);
      }
    } else {
      // user doesn't exist
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
    }
  }

  @Override
  public boolean userExists(String user) {
    return zooCache.get(zkUserPath + "/" + user) != null;
  }

  @Override
  public boolean validSecurityHandlers() {
    return true;
  }

  @Override
  public boolean authenticateUser(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    if (!(token instanceof PasswordToken)) {
      throw new AccumuloSecurityException(principal, SecurityErrorCode.INVALID_TOKEN);
    }
    PasswordToken pt = (PasswordToken) token;
    byte[] zkData;
    String zpath = zkUserPath + "/" + principal;
    zkData = zooCache.get(zpath);
    boolean result = authenticateUser(principal, pt, zkData);
    if (!result) {
      zooCache.clear(zpath);
      zkData = zooCache.get(zpath);
      result = authenticateUser(principal, pt, zkData);
    }
    return result;
  }

  private boolean authenticateUser(String principal, PasswordToken pt, byte[] zkData) {
    if (zkData == null) {
      return false;
    }
    return ZKSecurityTool.checkCryptPass(pt.getPassword(), zkData);
  }

  @Override
  public Set<Class<? extends AuthenticationToken>> getSupportedTokenTypes() {
    Set<Class<? extends AuthenticationToken>> cs = new HashSet<>();
    cs.add(PasswordToken.class);
    return cs;
  }

  @Override
  public boolean validTokenClass(String tokenClass) {
    return tokenClass.equals(PasswordToken.class.getName());
  }
}
