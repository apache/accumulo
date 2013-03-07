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
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;

/**
 * This is a Permission Handler implementation that doesn't actually do any security. Use at your own risk.
 */
public class InsecurePermHandler implements PermissionHandler {
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#initialize(java.lang.String)
   */
  @Override
  public void initialize(String instanceId, boolean initialize) {
    return;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#validSecurityHandlers(org.apache.accumulo.server.security.handler.Authenticator, org.apache.accumulo.server.security.handler.Authorizor)
   */
  @Override
  public boolean validSecurityHandlers(Authenticator authent, Authorizor author) {
    return true;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#initializeSecurity(java.lang.String)
   */
  @Override
  public void initializeSecurity(TCredentials token, String rootuser) throws AccumuloSecurityException {
    return;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#hasSystemPermission(java.lang.String, org.apache.accumulo.core.security.SystemPermission)
   */
  @Override
  public boolean hasSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    return true;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#hasCachedSystemPermission(java.lang.String, org.apache.accumulo.core.security.SystemPermission)
   */
  @Override
  public boolean hasCachedSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    return true;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#hasTablePermission(java.lang.String, java.lang.String, org.apache.accumulo.core.security.TablePermission)
   */
  @Override
  public boolean hasTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    return true;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#hasCachedTablePermission(java.lang.String, java.lang.String, org.apache.accumulo.core.security.TablePermission)
   */
  @Override
  public boolean hasCachedTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    return true;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#grantSystemPermission(java.lang.String, org.apache.accumulo.core.security.SystemPermission)
   */
  @Override
  public void grantSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    return;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#revokeSystemPermission(java.lang.String, org.apache.accumulo.core.security.SystemPermission)
   */
  @Override
  public void revokeSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    return;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#grantTablePermission(java.lang.String, java.lang.String, org.apache.accumulo.core.security.TablePermission)
   */
  @Override
  public void grantTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    return;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#revokeTablePermission(java.lang.String, java.lang.String, org.apache.accumulo.core.security.TablePermission)
   */
  @Override
  public void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    return;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#cleanTablePermissions(java.lang.String)
   */
  @Override
  public void cleanTablePermissions(String table) throws AccumuloSecurityException, TableNotFoundException {
    return;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#initUser(java.lang.String)
   */
  @Override
  public void initUser(String user) throws AccumuloSecurityException {
    return;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.security.handler.PermissionHandler#dropUser(java.lang.String)
   */
  @Override
  public void cleanUser(String user) throws AccumuloSecurityException {
    return;
  }

  @Override
  public void initTable(String table) throws AccumuloSecurityException {
  }
  
}
