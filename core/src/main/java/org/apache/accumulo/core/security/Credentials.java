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
package org.apache.accumulo.core.security;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.thrift.TCredentials;

/**
 * A wrapper for internal use. This class carries the instance, principal, and authentication token for use in the public API, in a non-serialized form. This is
 * important, so that the authentication token carried in a {@link Connector} can be destroyed, invalidating future RPC operations from that {@link Connector}.
 * <p>
 * See ACCUMULO-1312
 * 
 * @since 1.6.0
 */
public class Credentials {
  
  private String principal;
  private AuthenticationToken token;
  
  public Credentials(String principal, AuthenticationToken token) {
    this.principal = principal;
    this.token = token;
  }
  
  public String getPrincipal() {
    return principal;
  }
  
  public AuthenticationToken getToken() {
    return token;
  }
  
  public TCredentials toThrift(Instance instance) {
    return CredentialHelper.createSquelchError(principal, token, instance.getInstanceID());
  }
  
}
