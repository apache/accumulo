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
package org.apache.accumulo.core.security.handler;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.NullToken;

/**
 * 
 */
public class InsecureAuthenticator implements Authenticator {
  static NullToken nt = new NullToken();
  
  @Override
  public AuthenticationToken login(String principal, Properties properties) throws AccumuloSecurityException {
    return nt;
  }
  
  @Override
  public List<Set<AuthProperty>> getProperties() {
    Set<AuthProperty> internal = new TreeSet<AuthProperty>();
    List<Set<AuthProperty>> toRet = new LinkedList<Set<AuthProperty>>();
    toRet.add(internal);
    return toRet;
  }
}
