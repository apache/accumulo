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
package org.apache.accumulo.server.security;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;

public interface Authenticator {
  public void initializeSecurity(AuthInfo credentials, String rootuser, byte[] rootpass) throws AccumuloSecurityException;
  
  public String getRootUsername();
  
  public boolean authenticateUser(AuthInfo credentials, String user, ByteBuffer pass) throws AccumuloSecurityException;
  
  public Set<String> listUsers(AuthInfo credentials) throws AccumuloSecurityException;
  
  public void createUser(AuthInfo credentials, String user, byte[] pass, Authorizations authorizations) throws AccumuloSecurityException;
  
  public void dropUser(AuthInfo credentials, String user) throws AccumuloSecurityException;
  
  public void changePassword(AuthInfo credentials, String user, byte[] pass) throws AccumuloSecurityException;
  
  public void changeAuthorizations(AuthInfo credentials, String user, Authorizations authorizations) throws AccumuloSecurityException;
  
  public Authorizations getUserAuthorizations(AuthInfo credentials, String user) throws AccumuloSecurityException;
  
  public boolean hasSystemPermission(AuthInfo credentials, String user, SystemPermission permission) throws AccumuloSecurityException;
  
  public boolean hasTablePermission(AuthInfo credentials, String user, String table, TablePermission permission) throws AccumuloSecurityException;
  
  public void grantSystemPermission(AuthInfo credentials, String user, SystemPermission permission) throws AccumuloSecurityException;
  
  public void revokeSystemPermission(AuthInfo credentials, String user, SystemPermission permission) throws AccumuloSecurityException;
  
  public void grantTablePermission(AuthInfo credentials, String user, String table, TablePermission permission) throws AccumuloSecurityException;
  
  public void revokeTablePermission(AuthInfo credentials, String user, String table, TablePermission permission) throws AccumuloSecurityException;
  
  public void deleteTable(AuthInfo credentials, String table) throws AccumuloSecurityException;
  
  public void clearCache(String user);
  
  public void clearCache(String user, String tableId);
}
