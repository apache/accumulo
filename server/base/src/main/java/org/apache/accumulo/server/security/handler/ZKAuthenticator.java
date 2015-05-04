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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Utility class for adding all authentication info into ZK
public final class ZKAuthenticator implements Authenticator {
  private static final Logger log = LoggerFactory.getLogger(ZKAuthenticator.class);
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

  @Override
  public void initialize(String instanceId, boolean initialize) {
    ZKUserPath = Constants.ZROOT + "/" + instanceId + "/users";
  }

  @Override
  public void initializeSecurity(TCredentials credentials, String principal, byte[] token) throws AccumuloSecurityException {
    try {
      // remove old settings from zookeeper first, if any
      IZooReaderWriter zoo = ZooReaderWriter.getInstance();
      synchronized (zooCache) {
        zooCache.clear();
        if (zoo.exists(ZKUserPath)) {
          zoo.recursiveDelete(ZKUserPath, NodeMissingPolicy.SKIP);
          log.info("Removed {}/ from zookeeper", ZKUserPath);
        }

        // prep parent node of users with root username
        zoo.putPersistentData(ZKUserPath, principal.getBytes(UTF_8), NodeExistsPolicy.FAIL);

        constructUser(principal, ZKSecurityTool.createPass(token));
      }
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    } catch (AccumuloException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Sets up the user in ZK for the provided user. No checking for existence is done here, it should be done before calling.
   */
  private void constructUser(String user, byte[] pass) throws KeeperException, InterruptedException {
    synchronized (zooCache) {
      zooCache.clear();
      IZooReaderWriter zoo = ZooReaderWriter.getInstance();
      zoo.putPrivatePersistentData(ZKUserPath + "/" + user, pass, NodeExistsPolicy.FAIL);
    }
  }

  @Override
  public Set<String> listUsers() {
    return new TreeSet<>(zooCache.getChildren(ZKUserPath));
  }

  @Override
  public void createUser(String principal, AuthenticationToken token) throws AccumuloSecurityException {
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

  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        zooCache.clear();
        ZooReaderWriter.getInstance().recursiveDelete(ZKUserPath + "/" + user, NodeMissingPolicy.FAIL);
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
  }

  @Override
  public void changePassword(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    if (!(token instanceof PasswordToken))
      throw new AccumuloSecurityException(principal, SecurityErrorCode.INVALID_TOKEN);
    PasswordToken pt = (PasswordToken) token;
    if (userExists(principal)) {
      try {
        synchronized (zooCache) {
          zooCache.clear(ZKUserPath + "/" + principal);
          ZooReaderWriter.getInstance().putPrivatePersistentData(ZKUserPath + "/" + principal, ZKSecurityTool.createPass(pt.getPassword()),
              NodeExistsPolicy.OVERWRITE);
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
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST); // user doesn't exist
  }

  @Override
  public boolean userExists(String user) {
    return zooCache.get(ZKUserPath + "/" + user) != null;
  }

  @Override
  public boolean validSecurityHandlers(Authorizor auth, PermissionHandler pm) {
    return true;
  }

  @Override
  public boolean authenticateUser(String principal, AuthenticationToken token) throws AccumuloSecurityException {
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
