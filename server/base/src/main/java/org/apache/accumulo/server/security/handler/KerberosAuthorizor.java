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

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.TCredentials;

/**
 * Kerberos principals might contains identifiers that are not valid ZNodes ('/'). Base64-encodes the principals before interacting with ZooKeeper.
 */
public class KerberosAuthorizor implements Authorizor {

  private final ZKAuthorizor zkAuthorizor;

  public KerberosAuthorizor() {
    zkAuthorizor = new ZKAuthorizor();
  }

  @Override
  public void initialize(String instanceId, boolean initialize) {
    zkAuthorizor.initialize(instanceId, initialize);
  }

  @Override
  public boolean validSecurityHandlers(Authenticator auth, PermissionHandler pm) {
    return auth instanceof KerberosAuthenticator && pm instanceof KerberosPermissionHandler;
  }

  @Override
  public void initializeSecurity(TCredentials credentials, String rootuser) throws AccumuloSecurityException, ThriftSecurityException {
    zkAuthorizor.initializeSecurity(credentials, Base64.getEncoder().encodeToString(rootuser.getBytes(UTF_8)));
  }

  @Override
  public void changeAuthorizations(String user, Authorizations authorizations) throws AccumuloSecurityException {
    zkAuthorizor.changeAuthorizations(Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), authorizations);
  }

  @Override
  public Authorizations getCachedUserAuthorizations(String user) throws AccumuloSecurityException {
    return zkAuthorizor.getCachedUserAuthorizations(Base64.getEncoder().encodeToString(user.getBytes(UTF_8)));
  }

  @Override
  public boolean isValidAuthorizations(String user, List<ByteBuffer> list) throws AccumuloSecurityException {
    return zkAuthorizor.isValidAuthorizations(Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), list);
  }

  @Override
  public void initUser(String user) throws AccumuloSecurityException {
    zkAuthorizor.initUser(Base64.getEncoder().encodeToString(user.getBytes(UTF_8)));
  }

  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    user = Base64.getEncoder().encodeToString(user.getBytes(UTF_8));
    zkAuthorizor.dropUser(user);
  }

}
