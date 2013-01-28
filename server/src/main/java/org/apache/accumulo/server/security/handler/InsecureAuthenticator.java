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
package org.apache.accumulo.server.security.handler;

import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.tokens.SecurityToken;
import org.apache.accumulo.core.security.tokens.InstanceTokenWrapper;
import org.apache.accumulo.core.security.tokens.UserPassToken;

/**
 * This is an Authenticator implementation that doesn't actually do any security. Use at your own risk.
 */
public class InsecureAuthenticator implements Authenticator {
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.Authenticator#initialize(java.lang.String)
   */
  @Override
  public void initialize(String instanceId, boolean initialize) {
    return;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.Authenticator#validSecurityHandlers(org.apache.accumulo.server.security.handler.Authorizor, org.apache.accumulo.server.security.handler.PermissionHandler)
   */
  @Override
  public boolean validSecurityHandlers(Authorizor auth, PermissionHandler pm) {
    return true;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.Authenticator#initializeSecurity(org.apache.accumulo.core.security.thrift.InstanceTokenWrapper, java.lang.String, byte[])
   */
  @Override
  public void initializeSecurity(InstanceTokenWrapper credentials, SecurityToken token) throws AccumuloSecurityException {
    return;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.Authenticator#authenticateUser(java.lang.String, java.nio.ByteBuffer, java.lang.String)
   */
  @Override
  public boolean authenticateUser(SecurityToken token) {
    return true;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.Authenticator#listUsers()
   */
  @Override
  public Set<String> listUsers() throws AccumuloSecurityException {
    return Collections.emptySet();
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.Authenticator#createUser(java.lang.String, byte[])
   */
  @Override
  public void createUser(SecurityToken token) throws AccumuloSecurityException {
    return;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.Authenticator#dropUser(java.lang.String)
   */
  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    return;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.Authenticator#changePassword(java.lang.String, byte[])
   */
  @Override
  public void changePassword(SecurityToken token) throws AccumuloSecurityException {
    return;
  }

  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.Authenticator#userExists(java.lang.String)
   */
  @Override
  public boolean userExists(String user) {
    return true;
  }

  @Override
  public String getTokenClassName() {
    return UserPassToken.class.getName();
  }
  
}
