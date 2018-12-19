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

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.server.ServerContext;

/**
 * This is a Permission Handler implementation that doesn't actually do any security. Use at your
 * own risk.
 */
public class InsecurePermHandler implements PermissionHandler {

  @Override
  public void initialize(ServerContext context, boolean initialize) {}

  @Override
  public boolean validSecurityHandlers(Authenticator authent, Authorizor author) {
    return true;
  }

  @Override
  public void initializeSecurity(TCredentials token, String rootuser) {}

  @Override
  public boolean hasSystemPermission(String user, SystemPermission permission) {
    return true;
  }

  @Override
  public boolean hasCachedSystemPermission(String user, SystemPermission permission) {
    return true;
  }

  @Override
  public boolean hasTablePermission(String user, String table, TablePermission permission) {
    return true;
  }

  @Override
  public boolean hasCachedTablePermission(String user, String table, TablePermission permission) {
    return true;
  }

  @Override
  public void grantSystemPermission(String user, SystemPermission permission) {}

  @Override
  public void revokeSystemPermission(String user, SystemPermission permission) {}

  @Override
  public void grantTablePermission(String user, String table, TablePermission permission) {}

  @Override
  public void revokeTablePermission(String user, String table, TablePermission permission) {}

  @Override
  public void cleanTablePermissions(String table) {}

  @Override
  public void initUser(String user) {}

  @Override
  public void cleanUser(String user) {}

  @Override
  public void initTable(String table) {}

  @Override
  public boolean hasNamespacePermission(String user, Namespace.ID namespace,
      NamespacePermission permission) {
    return true;
  }

  @Override
  public boolean hasCachedNamespacePermission(String user, Namespace.ID namespace,
      NamespacePermission permission) {
    return true;
  }

  @Override
  public void grantNamespacePermission(String user, Namespace.ID namespace,
      NamespacePermission permission) {}

  @Override
  public void revokeNamespacePermission(String user, Namespace.ID namespace,
      NamespacePermission permission) {}

  @Override
  public void cleanNamespacePermissions(Namespace.ID namespace) {}

}
