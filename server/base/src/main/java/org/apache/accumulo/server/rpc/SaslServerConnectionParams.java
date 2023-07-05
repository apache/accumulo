/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.rpc;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.rpc.SaslConnectionParams;
import org.apache.accumulo.server.security.SystemCredentials.SystemToken;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;

/**
 * Server-side SASL connection information
 */
public class SaslServerConnectionParams extends SaslConnectionParams {

  private final AuthenticationTokenSecretManager secretManager;

  public SaslServerConnectionParams(AccumuloConfiguration conf, AuthenticationToken token,
      AuthenticationTokenSecretManager secretManager) {
    super(conf, token);
    this.secretManager = secretManager;
  }

  @Override
  protected void updateFromToken(AuthenticationToken token) {
    // Servers should never have a delegation token -- only a strong kerberos identity
    if (token instanceof KerberosToken || token instanceof SystemToken) {
      mechanism = SaslMechanism.GSSAPI;
    } else {
      throw new IllegalArgumentException(
          "Cannot determine SASL mechanism for token class: " + token.getClass());
    }
  }

  public AuthenticationTokenSecretManager getSecretManager() {
    return secretManager;
  }
}
