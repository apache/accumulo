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
package org.apache.accumulo.server.security;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

/**
 * 
 */
public interface Authorizor {
  public void initialize(String instanceId);

  public boolean validAuthenticator(Authenticator auth);
  
  public void initializeSecurity(String rootuser) throws AccumuloSecurityException;
  
  public void changeAuthorizations(String user, Authorizations authorizations) throws AccumuloSecurityException;
  
  public Authorizations getUserAuthorizations(String user) throws AccumuloSecurityException;
  
  public boolean hasSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException;
  
  public boolean hasTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException;
  
  public void grantSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException;
  
  public void revokeSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException;
  
  public void grantTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException;
  
  public void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException;
  
  public void cleanTablePermissions(String table) throws AccumuloSecurityException, TableNotFoundException;
  
  public void clearUserCache(String user) throws AccumuloSecurityException;
  
  public void clearTableCache(String user) throws AccumuloSecurityException, TableNotFoundException;

  public void clearCache(String user, String tableId) throws TableNotFoundException;
  
  public void initUser(String user) throws AccumuloSecurityException;
  
  public boolean cachesToClear() throws AccumuloSecurityException;
  
  public void clearCache(String user, boolean auths, boolean system, Set<String> tables) throws AccumuloSecurityException, TableNotFoundException;
  
  public void dropUser(String user) throws AccumuloSecurityException;
}
