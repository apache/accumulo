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

  void initialize(String instanceId, boolean initialize);

  boolean validSecurityHandlers(Authorizor auth, PermissionHandler pm);

  void initializeSecurity(TCredentials credentials, String principal, byte[] token) throws AccumuloSecurityException, ThriftSecurityException;

  boolean authenticateUser(String principal, AuthenticationToken token) throws AccumuloSecurityException;

  Set<String> listUsers() throws AccumuloSecurityException;

  /**
   * Creates a user with no initial permissions whatsoever
   */
  void createUser(String principal, AuthenticationToken token) throws AccumuloSecurityException;

  void dropUser(String user) throws AccumuloSecurityException;

  void changePassword(String principal, AuthenticationToken token) throws AccumuloSecurityException;

  /**
   * Checks if a user exists
   */
  boolean userExists(String user) throws AccumuloSecurityException;

  Set<Class<? extends AuthenticationToken>> getSupportedTokenTypes();

  /**
   * Returns true if the given token is appropriate for this Authenticator
   */
  boolean validTokenClass(String tokenClass);
}
