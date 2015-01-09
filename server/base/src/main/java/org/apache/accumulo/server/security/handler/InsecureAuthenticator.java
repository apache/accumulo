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
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.NullToken;
import org.apache.accumulo.core.security.thrift.TCredentials;

/**
 * This is an Authenticator implementation that doesn't actually do any security. Any principal will authenticate if a NullToken is provided. It's existence is
 * primarily for testing, but can also be used for any system where user space management is not a concern.
 */
public class InsecureAuthenticator implements Authenticator {

  @Override
  public void initialize(String instanceId, boolean initialize) {}

  @Override
  public boolean validSecurityHandlers(Authorizor auth, PermissionHandler pm) {
    return true;
  }

  @Override
  public void initializeSecurity(TCredentials credentials, String principal, byte[] token) throws AccumuloSecurityException {}

  @Override
  public boolean authenticateUser(String principal, AuthenticationToken token) {
    return token instanceof NullToken;
  }

  @Override
  public Set<String> listUsers() throws AccumuloSecurityException {
    return Collections.emptySet();
  }

  @Override
  public void createUser(String principal, AuthenticationToken token) throws AccumuloSecurityException {}

  @Override
  public void dropUser(String user) throws AccumuloSecurityException {}

  @Override
  public void changePassword(String user, AuthenticationToken token) throws AccumuloSecurityException {}

  @Override
  public boolean userExists(String user) {
    return true;
  }

  @Override
  public boolean validTokenClass(String tokenClass) {
    return tokenClass.equals(NullToken.class.getName());
  }

  @Override
  public Set<Class<? extends AuthenticationToken>> getSupportedTokenTypes() {
    Set<Class<? extends AuthenticationToken>> cs = new HashSet<Class<? extends AuthenticationToken>>();
    cs.add(NullToken.class);
    return cs;
  }

}
