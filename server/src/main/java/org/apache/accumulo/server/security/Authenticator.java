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
import org.apache.accumulo.core.security.thrift.AuthInfo;

public interface Authenticator {
  
  public void initialize(String instanceId);

  public boolean validAuthorizor(Authorizor auth);

  public void initializeSecurity(AuthInfo credentials, String rootuser, byte[] rootpass) throws AccumuloSecurityException;

  public boolean authenticateUser(String user, ByteBuffer password, String instanceId);
  
  public Set<String> listUsers() throws AccumuloSecurityException;
  
  public void createUser(String user, byte[] pass) throws AccumuloSecurityException;
  
  public void dropUser(String user) throws AccumuloSecurityException;
  
  public void changePassword(String user, byte[] pass) throws AccumuloSecurityException;
  
  public void clearCache(String user);
  
  public boolean cachesToClear();
  
  public boolean userExists(String user);
}
