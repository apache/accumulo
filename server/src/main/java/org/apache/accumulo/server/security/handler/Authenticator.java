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

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.thrift.TCredentials;

/**
 * This interface is used for the system which will be used for authenticating a user. If the implementation does not support configuration through Accumulo, it
 * should throw an AccumuloSecurityException with the error code UNSUPPORTED_OPERATION
 */

public interface Authenticator {
  
  public void initialize(String instanceId, boolean initialize);
  
  public boolean validSecurityHandlers(Authorizor auth, PermissionHandler pm);
  
  public void initializeSecurity(TCredentials credentials, String principal, byte[] token) throws AccumuloSecurityException, ThriftSecurityException;
  
  public boolean authenticateUser(String principal, AuthenticationToken token) throws AccumuloSecurityException;
  
  public Set<String> listUsers() throws AccumuloSecurityException;
  
  public void createUser(String principal, AuthenticationToken token) throws AccumuloSecurityException;
  
  public void dropUser(String user) throws AccumuloSecurityException;
  
  public void changePassword(String principal, AuthenticationToken token) throws AccumuloSecurityException;
  
  public boolean userExists(String user) throws AccumuloSecurityException;
  
  public Set<Class<? extends AuthenticationToken>> getSupportedTokenTypes();
  
  /**
   * Returns true if the given token is appropriate for this Authenticator
   */
  public boolean validTokenClass(String tokenClass);
}
