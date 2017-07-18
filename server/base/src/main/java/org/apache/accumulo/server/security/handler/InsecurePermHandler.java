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

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;

/**
 * This is a Permission Handler implementation that doesn't actually do any security. Use at your own risk.
 */
public class InsecurePermHandler implements PermissionHandler {

  @Override
  public void initialize(String instanceId, boolean initialize) {}

  @Override
  public boolean validSecurityHandlers(Authenticator authent, Authorizor author) {
    return true;
  }

  @Override
  public void initializeSecurity(TCredentials token, String rootuser) throws AccumuloSecurityException {}

  @Override
  public boolean hasSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    return true;
  }

  @Override
  public boolean hasCachedSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    return true;
  }

  @Override
  public boolean hasTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    return true;
  }

  @Override
  public boolean hasCachedTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    return true;
  }

  @Override
  public void grantSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {}

  @Override
  public void revokeSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {}

  @Override
  public void grantTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {}

  @Override
  public void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {}

  @Override
  public void cleanTablePermissions(String table) throws AccumuloSecurityException, TableNotFoundException {}

  @Override
  public void initUser(String user) throws AccumuloSecurityException {}

  @Override
  public void cleanUser(String user) throws AccumuloSecurityException {}

  @Override
  public void initTable(String table) throws AccumuloSecurityException {}

  @Override
  public boolean hasNamespacePermission(String user, Namespace.ID namespace, NamespacePermission permission) throws AccumuloSecurityException,
      NamespaceNotFoundException {
    return true;
  }

  @Override
  public boolean hasCachedNamespacePermission(String user, Namespace.ID namespace, NamespacePermission permission) throws AccumuloSecurityException,
      NamespaceNotFoundException {
    return true;
  }

  @Override
  public void grantNamespacePermission(String user, Namespace.ID namespace, NamespacePermission permission) throws AccumuloSecurityException,
      NamespaceNotFoundException {}

  @Override
  public void revokeNamespacePermission(String user, Namespace.ID namespace, NamespacePermission permission) throws AccumuloSecurityException,
      NamespaceNotFoundException {}

  @Override
  public void cleanNamespacePermissions(Namespace.ID namespace) throws AccumuloSecurityException, NamespaceNotFoundException {}

}
