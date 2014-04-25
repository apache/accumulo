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
package org.apache.accumulo.core.client.mock;

import java.util.EnumSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

class MockSecurityOperationsImpl implements SecurityOperations {

  final private MockAccumulo acu;

  MockSecurityOperationsImpl(MockAccumulo acu) {
    this.acu = acu;
  }

  @Deprecated
  @Override
  public void createUser(String user, byte[] password, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    createLocalUser(user, new PasswordToken(password));
    changeUserAuthorizations(user, authorizations);
  }

  @Override
  public void createLocalUser(String principal, PasswordToken password) throws AccumuloException, AccumuloSecurityException {
    this.acu.users.put(principal, new MockUser(principal, password, new Authorizations()));
  }

  @Deprecated
  @Override
  public void dropUser(String user) throws AccumuloException, AccumuloSecurityException {
    dropLocalUser(user);
  }

  @Override
  public void dropLocalUser(String principal) throws AccumuloException, AccumuloSecurityException {
    this.acu.users.remove(principal);
  }

  @Deprecated
  @Override
  public boolean authenticateUser(String user, byte[] password) throws AccumuloException, AccumuloSecurityException {
    return authenticateUser(user, new PasswordToken(password));
  }

  @Override
  public boolean authenticateUser(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(principal);
    if (user == null)
      return false;
    return user.token.equals(token);
  }

  @Deprecated
  @Override
  public void changeUserPassword(String user, byte[] password) throws AccumuloException, AccumuloSecurityException {
    changeLocalUserPassword(user, new PasswordToken(password));
  }

  @Override
  public void changeLocalUserPassword(String principal, PasswordToken token) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(principal);
    if (user != null)
      user.token = token.clone();
    else
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
  }

  @Override
  public void changeUserAuthorizations(String principal, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(principal);
    if (user != null)
      user.authorizations = authorizations;
    else
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
  }

  @Override
  public Authorizations getUserAuthorizations(String principal) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(principal);
    if (user != null)
      return user.authorizations;
    else
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
  }

  @Override
  public boolean hasSystemPermission(String principal, SystemPermission perm) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(principal);
    if (user != null)
      return user.permissions.contains(perm);
    else
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
  }

  @Override
  public boolean hasTablePermission(String principal, String tableName, TablePermission perm) throws AccumuloException, AccumuloSecurityException {
    MockTable table = acu.tables.get(tableName);
    if (table == null)
      throw new AccumuloSecurityException(tableName, SecurityErrorCode.TABLE_DOESNT_EXIST);
    EnumSet<TablePermission> perms = table.userPermissions.get(principal);
    if (perms == null)
      return false;
    return perms.contains(perm);
  }

  @Override
  public boolean hasNamespacePermission(String principal, String namespace, NamespacePermission permission) throws AccumuloException, AccumuloSecurityException {
    MockNamespace mockNamespace = acu.namespaces.get(namespace);
    if (mockNamespace == null)
      throw new AccumuloSecurityException(namespace, SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
    EnumSet<NamespacePermission> perms = mockNamespace.userPermissions.get(principal);
    if (perms == null)
      return false;
    return perms.contains(permission);
  }

  @Override
  public void grantSystemPermission(String principal, SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(principal);
    if (user != null)
      user.permissions.add(permission);
    else
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
  }

  @Override
  public void grantTablePermission(String principal, String tableName, TablePermission permission) throws AccumuloException, AccumuloSecurityException {
    if (acu.users.get(principal) == null)
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
    MockTable table = acu.tables.get(tableName);
    if (table == null)
      throw new AccumuloSecurityException(tableName, SecurityErrorCode.TABLE_DOESNT_EXIST);
    EnumSet<TablePermission> perms = table.userPermissions.get(principal);
    if (perms == null)
      table.userPermissions.put(principal, EnumSet.of(permission));
    else
      perms.add(permission);
  }

  @Override
  public void grantNamespacePermission(String principal, String namespace, NamespacePermission permission) throws AccumuloException, AccumuloSecurityException {
    if (acu.users.get(principal) == null)
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
    MockNamespace mockNamespace = acu.namespaces.get(namespace);
    if (mockNamespace == null)
      throw new AccumuloSecurityException(namespace, SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
    EnumSet<NamespacePermission> perms = mockNamespace.userPermissions.get(principal);
    if (perms == null)
      mockNamespace.userPermissions.put(principal, EnumSet.of(permission));
    else
      perms.add(permission);
  }

  @Override
  public void revokeSystemPermission(String principal, SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(principal);
    if (user != null)
      user.permissions.remove(permission);
    else
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
  }

  @Override
  public void revokeTablePermission(String principal, String tableName, TablePermission permission) throws AccumuloException, AccumuloSecurityException {
    if (acu.users.get(principal) == null)
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
    MockTable table = acu.tables.get(tableName);
    if (table == null)
      throw new AccumuloSecurityException(tableName, SecurityErrorCode.TABLE_DOESNT_EXIST);
    EnumSet<TablePermission> perms = table.userPermissions.get(principal);
    if (perms != null)
      perms.remove(permission);

  }

  @Override
  public void revokeNamespacePermission(String principal, String namespace, NamespacePermission permission) throws AccumuloException, AccumuloSecurityException {
    if (acu.users.get(principal) == null)
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
    MockNamespace mockNamespace = acu.namespaces.get(namespace);
    if (mockNamespace == null)
      throw new AccumuloSecurityException(namespace, SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
    EnumSet<NamespacePermission> perms = mockNamespace.userPermissions.get(principal);
    if (perms != null)
      perms.remove(permission);

  }

  @Deprecated
  @Override
  public Set<String> listUsers() throws AccumuloException, AccumuloSecurityException {
    return listLocalUsers();
  }

  @Override
  public Set<String> listLocalUsers() throws AccumuloException, AccumuloSecurityException {
    return acu.users.keySet();
  }

}
