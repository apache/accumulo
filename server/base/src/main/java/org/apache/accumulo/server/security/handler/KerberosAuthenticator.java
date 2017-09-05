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

import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.DelegationTokenImpl;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.rpc.UGIAssumingProcessor;
import org.apache.accumulo.server.security.SystemCredentials.SystemToken;
import org.apache.accumulo.server.security.UserImpersonation;
import org.apache.accumulo.server.security.UserImpersonation.UsersWithHosts;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class KerberosAuthenticator implements Authenticator {
  private static final Logger log = LoggerFactory.getLogger(KerberosAuthenticator.class);

  private static final Set<Class<? extends AuthenticationToken>> SUPPORTED_TOKENS = Sets.newHashSet(Arrays.<Class<? extends AuthenticationToken>> asList(
      KerberosToken.class, SystemToken.class));
  private static final Set<String> SUPPORTED_TOKEN_NAMES = Sets.newHashSet(KerberosToken.class.getName(), SystemToken.class.getName());

  private final ZKAuthenticator zkAuthenticator = new ZKAuthenticator();
  private String zkUserPath;
  private final ZooCache zooCache;
  private final UserImpersonation impersonation;

  public KerberosAuthenticator() {
    this(new ZooCache(), SiteConfiguration.getInstance());
  }

  public KerberosAuthenticator(ZooCache cache, AccumuloConfiguration conf) {
    this.zooCache = cache;
    this.impersonation = new UserImpersonation(conf);
  }

  @Override
  public void initialize(String instanceId, boolean initialize) {
    zkAuthenticator.initialize(instanceId, initialize);
    zkUserPath = Constants.ZROOT + "/" + instanceId + "/users";
  }

  @Override
  public boolean validSecurityHandlers(Authorizor auth, PermissionHandler pm) {
    return true;
  }

  private void createUserNodeInZk(String principal) throws KeeperException, InterruptedException {
    synchronized (zooCache) {
      zooCache.clear();
      IZooReaderWriter zoo = ZooReaderWriter.getInstance();
      zoo.putPrivatePersistentData(zkUserPath + "/" + principal, new byte[0], NodeExistsPolicy.FAIL);
    }
  }

  @Override
  public void initializeSecurity(TCredentials credentials, String principal, byte[] token) throws AccumuloSecurityException, ThriftSecurityException {
    try {
      // remove old settings from zookeeper first, if any
      IZooReaderWriter zoo = ZooReaderWriter.getInstance();
      synchronized (zooCache) {
        zooCache.clear();
        if (zoo.exists(zkUserPath)) {
          zoo.recursiveDelete(zkUserPath, NodeMissingPolicy.SKIP);
          log.info("Removed {}/ from zookeeper", zkUserPath);
        }

        // prep parent node of users with root username
        // ACCUMULO-4140 The root user needs to be stored un-base64 encoded in the znode's value
        byte[] principalData = principal.getBytes(UTF_8);
        zoo.putPersistentData(zkUserPath, principalData, NodeExistsPolicy.FAIL);

        // Create the root user in ZK using base64 encoded name (since the name is included in the znode)
        createUserNodeInZk(Base64.getEncoder().encodeToString(principalData));
      }
    } catch (KeeperException | InterruptedException e) {
      log.error("Failed to initialize security", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean authenticateUser(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    final String rpcPrincipal = UGIAssumingProcessor.rpcPrincipal();

    if (!rpcPrincipal.equals(principal)) {
      // KerberosAuthenticator can't do perform this because KerberosToken is just a shim and doesn't contain the actual credentials
      // Double check that the rpc user can impersonate as the requested user.
      UsersWithHosts usersWithHosts = impersonation.get(rpcPrincipal);
      if (null == usersWithHosts) {
        throw new AccumuloSecurityException(principal, SecurityErrorCode.AUTHENTICATOR_FAILED);
      }
      if (!usersWithHosts.getUsers().contains(principal)) {
        throw new AccumuloSecurityException(principal, SecurityErrorCode.AUTHENTICATOR_FAILED);
      }

      log.debug("Allowing impersonation of {} by {}", principal, rpcPrincipal);
    }

    // User is authenticated at the transport layer -- nothing extra is necessary
    if (token instanceof KerberosToken || token instanceof DelegationTokenImpl) {
      return true;
    }
    return false;
  }

  @Override
  public Set<String> listUsers() throws AccumuloSecurityException {
    Set<String> base64Users = zkAuthenticator.listUsers();
    Set<String> readableUsers = new HashSet<>();
    for (String base64User : base64Users) {
      readableUsers.add(new String(Base64.getDecoder().decode(base64User), UTF_8));
    }
    return readableUsers;
  }

  @Override
  public synchronized void createUser(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    if (!(token instanceof KerberosToken)) {
      throw new UnsupportedOperationException("Expected a KerberosToken but got a " + token.getClass().getSimpleName());
    }

    try {
      createUserNodeInZk(Base64.getEncoder().encodeToString(principal.getBytes(UTF_8)));
    } catch (KeeperException e) {
      if (e.code().equals(KeeperException.Code.NODEEXISTS)) {
        throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_EXISTS, e);
      }
      log.error("Failed to create user in ZooKeeper", e);
      throw new AccumuloSecurityException(principal, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("Interrupted trying to create node for user", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void dropUser(String user) throws AccumuloSecurityException {
    final String encodedUser = Base64.getEncoder().encodeToString(user.getBytes(UTF_8));
    try {
      zkAuthenticator.dropUser(encodedUser);
    } catch (AccumuloSecurityException e) {
      throw new AccumuloSecurityException(user, e.asThriftException().getCode(), e.getCause());
    }
  }

  @Override
  public void changePassword(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    throw new UnsupportedOperationException("Cannot change password with Kerberos authenticaton");
  }

  @Override
  public synchronized boolean userExists(String user) throws AccumuloSecurityException {
    user = Base64.getEncoder().encodeToString(user.getBytes(UTF_8));
    return zkAuthenticator.userExists(user);
  }

  @Override
  public Set<Class<? extends AuthenticationToken>> getSupportedTokenTypes() {
    return SUPPORTED_TOKENS;
  }

  @Override
  public boolean validTokenClass(String tokenClass) {
    return SUPPORTED_TOKEN_NAMES.contains(tokenClass);
  }

}
