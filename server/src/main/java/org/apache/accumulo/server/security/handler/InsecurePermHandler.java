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
  
  @Override
  public void initialize(String instanceId, boolean initialize) {
    return;
  }
  
  @Override
  public boolean validSecurityHandlers(Authenticator authent, Authorizor author) {
    return true;
  }
  
  @Override
  public void initializeSecurity(TCredentials token, String rootuser) throws AccumuloSecurityException {
    return;
  }
  
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
  public void grantSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    return;
  }
  
  @Override
  public void revokeSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    return;
  }
  
  @Override
  public void grantTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    return;
  }
  
  @Override
  public void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    return;
  }
  
  @Override
  public void cleanTablePermissions(String table) throws AccumuloSecurityException, TableNotFoundException {
    return;
  }
  
  @Override
  public void initUser(String user) throws AccumuloSecurityException {
    return;
  }
  
  @Override
  public void cleanUser(String user) throws AccumuloSecurityException {
    return;
  }
  
  @Override
  public void initTable(String table) throws AccumuloSecurityException {}
  
}
