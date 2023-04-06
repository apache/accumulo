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

import java.util.Base64;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.server.ServerContext;

/**
 * Kerberos principals might contains identifiers that are not valid ZNodes ('/'). Base64-encodes
 * the principals before interacting with ZooKeeper.
 */
public class KerberosPermissionHandler implements PermissionHandler {

  private final ZKPermHandler zkPermissionHandler;

  public KerberosPermissionHandler() {
    zkPermissionHandler = new ZKPermHandler();
  }

  @Override
  public void initialize(ServerContext context) {
    zkPermissionHandler.initialize(context);
  }

  @Override
  public boolean validSecurityHandlers(Authenticator authent, Authorizor author) {
    return authent instanceof KerberosAuthenticator && author instanceof KerberosAuthorizor;
  }

  @Override
  public void initializeSecurity(TCredentials credentials, String rootuser)
      throws AccumuloSecurityException {
    zkPermissionHandler.initializeSecurity(credentials,
        Base64.getEncoder().encodeToString(rootuser.getBytes(UTF_8)));
  }

  @Override
  public boolean hasSystemPermission(String user, SystemPermission permission) {
    return zkPermissionHandler
        .hasSystemPermission(Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), permission);
  }

  @Override
  public boolean hasCachedSystemPermission(String user, SystemPermission permission) {
    return zkPermissionHandler.hasCachedSystemPermission(
        Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), permission);
  }

  @Override
  public boolean hasTablePermission(String user, String table, TablePermission permission)
      throws TableNotFoundException {
    return zkPermissionHandler.hasTablePermission(
        Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), table, permission);
  }

  @Override
  public boolean hasCachedTablePermission(String user, String table, TablePermission permission) {
    return zkPermissionHandler.hasCachedTablePermission(
        Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), table, permission);
  }

  @Override
  public boolean hasNamespacePermission(String user, String namespace,
      NamespacePermission permission) throws NamespaceNotFoundException {
    return zkPermissionHandler.hasNamespacePermission(
        Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), namespace, permission);
  }

  @Override
  public boolean hasCachedNamespacePermission(String user, String namespace,
      NamespacePermission permission) {
    return zkPermissionHandler.hasCachedNamespacePermission(
        Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), namespace, permission);
  }

  @Override
  public void grantSystemPermission(String user, SystemPermission permission)
      throws AccumuloSecurityException {
    zkPermissionHandler.grantSystemPermission(
        Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), permission);
  }

  @Override
  public void revokeSystemPermission(String user, SystemPermission permission)
      throws AccumuloSecurityException {
    zkPermissionHandler.revokeSystemPermission(
        Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), permission);
  }

  @Override
  public void grantTablePermission(String user, String table, TablePermission permission)
      throws AccumuloSecurityException {
    zkPermissionHandler.grantTablePermission(
        Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), table, permission);
  }

  @Override
  public void revokeTablePermission(String user, String table, TablePermission permission)
      throws AccumuloSecurityException {
    zkPermissionHandler.revokeTablePermission(
        Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), table, permission);
  }

  @Override
  public void grantNamespacePermission(String user, String namespace,
      NamespacePermission permission) throws AccumuloSecurityException {
    zkPermissionHandler.grantNamespacePermission(
        Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), namespace, permission);
  }

  @Override
  public void revokeNamespacePermission(String user, String namespace,
      NamespacePermission permission) throws AccumuloSecurityException {
    zkPermissionHandler.revokeNamespacePermission(
        Base64.getEncoder().encodeToString(user.getBytes(UTF_8)), namespace, permission);
  }

  @Override
  public void cleanTablePermissions(String table) throws AccumuloSecurityException {
    zkPermissionHandler.cleanTablePermissions(table);
  }

  @Override
  public void cleanNamespacePermissions(String namespace) throws AccumuloSecurityException {
    zkPermissionHandler.cleanNamespacePermissions(namespace);
  }

  @Override
  public void initUser(String user) throws AccumuloSecurityException {
    zkPermissionHandler.initUser(Base64.getEncoder().encodeToString(user.getBytes(UTF_8)));
  }

  @Override
  public void cleanUser(String user) throws AccumuloSecurityException {
    zkPermissionHandler.cleanUser(Base64.getEncoder().encodeToString(user.getBytes(UTF_8)));
  }

}
