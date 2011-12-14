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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;

public class MockSecurityOperations implements SecurityOperations {
  
  final private MockAccumulo acu;
  
  MockSecurityOperations(MockAccumulo acu) {
    this.acu = acu;
  }
  
  @Override
  public void createUser(String user, byte[] password, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    this.acu.users.put(user, new MockUser(user, password, authorizations));
  }
  
  @Override
  public void dropUser(String user) throws AccumuloException, AccumuloSecurityException {
    this.acu.users.remove(user);
  }
  
  @Override
  public boolean authenticateUser(String name, byte[] password) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(name);
    if (user == null)
      return false;
    return Arrays.equals(user.password, password);
  }
  
  @Override
  public void changeUserPassword(String name, byte[] password) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(name);
    if (user != null)
      user.password = Arrays.copyOf(password, password.length);
    else
      throw new AccumuloSecurityException(name, SecurityErrorCode.USER_DOESNT_EXIST);
  }
  
  @Override
  public void changeUserAuthorizations(String name, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(name);
    if (user != null)
      user.authorizations = authorizations;
    else
      throw new AccumuloSecurityException(name, SecurityErrorCode.USER_DOESNT_EXIST);
  }
  
  @Override
  public Authorizations getUserAuthorizations(String name) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(name);
    if (user != null)
      return user.authorizations;
    else
      throw new AccumuloSecurityException(name, SecurityErrorCode.USER_DOESNT_EXIST);
  }
  
  @Override
  public boolean hasSystemPermission(String name, SystemPermission perm) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(name);
    if (user != null)
      return user.permissions.contains(perm);
    else
      throw new AccumuloSecurityException(name, SecurityErrorCode.USER_DOESNT_EXIST);
  }
  
  @Override
  public boolean hasTablePermission(String name, String tableName, TablePermission perm) throws AccumuloException, AccumuloSecurityException {
    MockTable table = acu.tables.get(tableName);
    if (table == null)
      throw new AccumuloSecurityException(tableName, SecurityErrorCode.TABLE_DOESNT_EXIST);
    EnumSet<TablePermission> perms = table.userPermissions.get(name);
    if (perms == null)
      return false;
    return perms.contains(perm);
  }
  
  @Override
  public void grantSystemPermission(String name, SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(name);
    if (user != null)
      user.permissions.add(permission);
    else
      throw new AccumuloSecurityException(name, SecurityErrorCode.USER_DOESNT_EXIST);
  }
  
  @Override
  public void grantTablePermission(String name, String tableName, TablePermission permission) throws AccumuloException, AccumuloSecurityException {
    if (acu.users.get(name) == null)
      throw new AccumuloSecurityException(name, SecurityErrorCode.USER_DOESNT_EXIST);
    MockTable table = acu.tables.get(tableName);
    if (table == null)
      throw new AccumuloSecurityException(tableName, SecurityErrorCode.TABLE_DOESNT_EXIST);
    EnumSet<TablePermission> perms = table.userPermissions.get(name);
    if (perms == null)
      table.userPermissions.put(name, EnumSet.of(permission));
    else
      perms.add(permission);
  }
  
  @Override
  public void revokeSystemPermission(String name, SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    MockUser user = acu.users.get(name);
    if (user != null)
      user.permissions.remove(permission);
    else
      throw new AccumuloSecurityException(name, SecurityErrorCode.USER_DOESNT_EXIST);
  }
  
  @Override
  public void revokeTablePermission(String name, String tableName, TablePermission permission) throws AccumuloException, AccumuloSecurityException {
    if (acu.users.get(name) == null)
      throw new AccumuloSecurityException(name, SecurityErrorCode.USER_DOESNT_EXIST);
    MockTable table = acu.tables.get(tableName);
    if (table == null)
      throw new AccumuloSecurityException(tableName, SecurityErrorCode.TABLE_DOESNT_EXIST);
    EnumSet<TablePermission> perms = table.userPermissions.get(name);
    if (perms != null)
      perms.remove(permission);
    
  }
  
  @Override
  public Set<String> listUsers() throws AccumuloException, AccumuloSecurityException {
    return acu.users.keySet();
  }
  
}
