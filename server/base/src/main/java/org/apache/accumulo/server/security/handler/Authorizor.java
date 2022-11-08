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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.server.ServerContext;

/**
 * This interface is used for the system which will be used for getting a users Authorizations. If
 * the implementation does not support configuration through Accumulo, it should throw an
 * AccumuloSecurityException with the error code UNSUPPORTED_OPERATION
 */
public interface Authorizor {

  /**
   * Sets up the authorizor for a new instance of Accumulo
   */
  void initialize(ServerContext context);

  /**
   * Used to validate that the Authorizor, Authenticator, and permission handler can coexist
   */
  boolean validSecurityHandlers(Authenticator auth, PermissionHandler pm);

  /**
   * Used to initialize security for the root user
   */
  void initializeSecurity(TCredentials credentials, String rootuser)
      throws AccumuloSecurityException;

  /**
   * Used to change the authorizations for the user
   */
  void changeAuthorizations(String user, Authorizations authorizations)
      throws AccumuloSecurityException;

  /**
   * Used to get the authorizations for the user
   */
  Authorizations getCachedUserAuthorizations(String user);

  /**
   * Used to check if a user has valid auths.
   */
  boolean isValidAuthorizations(String user, List<ByteBuffer> list);

  /**
   * Initializes a new user
   */
  void initUser(String user) throws AccumuloSecurityException;

  /**
   * Deletes a user
   */
  void dropUser(String user) throws AccumuloSecurityException;
}
