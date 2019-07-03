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

import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.clientImpl.DelegationTokenImpl;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.rpc.UGIAssumingProcessor;
import org.apache.accumulo.server.security.UserImpersonation;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosAuthImpl extends AuthImpl {
  private static final Logger log = LoggerFactory.getLogger(KerberosAuthImpl.class);
  private UserImpersonation impersonation;

  public KerberosAuthImpl(ZooCache zooCache, ServerContext context, String ZKUserPath) {
    super(zooCache, context, ZKUserPath);
    impersonation = new UserImpersonation(context.getConfiguration());
  }

  @Override
  public Set<String> listUsers() {
    Set<String> base64Users = super.listUsers();
    // decode kerberos byte encoded users
    Set<String> readableUsers = new HashSet<>();
    for (String base64User : base64Users) {
      readableUsers.add(new String(Base64.getDecoder().decode(base64User), UTF_8));
    }
    return readableUsers;
  }

  @Override
  public boolean authenticate(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    final String rpcPrincipal = UGIAssumingProcessor.rpcPrincipal();

    if (!rpcPrincipal.equals(principal)) {
      // KerberosAuthenticator can't do perform this because KerberosToken is just a shim and
      // doesn't contain the actual credentials
      // Double check that the rpc user can impersonate as the requested user.
      UserImpersonation.UsersWithHosts usersWithHosts = impersonation.get(rpcPrincipal);
      if (usersWithHosts == null) {
        throw new AccumuloSecurityException(principal, SecurityErrorCode.AUTHENTICATOR_FAILED);
      }
      if (!usersWithHosts.getUsers().contains(principal)) {
        throw new AccumuloSecurityException(principal, SecurityErrorCode.AUTHENTICATOR_FAILED);
      }

      log.debug("Allowing impersonation of {} by {}", principal, rpcPrincipal);
    }

    // User is authenticated at the transport layer -- nothing extra is necessary
    return token instanceof KerberosToken || token instanceof DelegationTokenImpl;
  }

  @Override
  public Authorizations getAuthorizations(String principal) {
    return super.getAuthorizations(Base64.getEncoder().encodeToString(principal.getBytes(UTF_8)));
  }

  @Override
  public boolean hasAuths(String principal, Authorizations authorizations) {
    return super.hasAuths(Base64.getEncoder().encodeToString(principal.getBytes(UTF_8)),
        authorizations);
  }

  @Override
  public void changeAuthorizations(String principal, Authorizations authorizations)
      throws AccumuloSecurityException {
    super.changeAuthorizations(Base64.getEncoder().encodeToString(principal.getBytes(UTF_8)),
        authorizations);
  }

  @Override
  public void changePassword(String principal, AuthenticationToken token) {
    throw new UnsupportedOperationException("Cannot change password with Kerberos authenticaton");
  }

  @Override
  public void createUser(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    super.createUser(Base64.getEncoder().encodeToString(principal.getBytes(UTF_8)), token);
  }

  @Override
  public void constructUser(String principal, byte[] pass)
      throws KeeperException, InterruptedException {
    super.constructUser(Base64.getEncoder().encodeToString(principal.getBytes(UTF_8)), pass);
  }

  @Override
  public void dropUser(String principal) throws AccumuloSecurityException {
    super.dropUser(Base64.getEncoder().encodeToString(principal.getBytes(UTF_8)));
  }
}
