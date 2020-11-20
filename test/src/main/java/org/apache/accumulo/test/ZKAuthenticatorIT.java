/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;

import java.nio.charset.Charset;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.security.handler.ZKAuthenticator;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ZKAuthenticatorIT extends AccumuloClusterHarness {

  @Test
  public void testUserAuthentication() throws Exception {
    byte[] rawPass = "myPassword".getBytes(Charset.forName("UTF-8"));
    String principal = "myTestUser";

    PasswordToken token = null;
    if (!saslEnabled()) {
      token = new PasswordToken(rawPass.clone());
    }

    ZKAuthenticator auth = new ZKAuthenticator();
    auth.initialize(cluster.getServerContext());

    auth.createUser(principal, token);
    assertTrue(auth.authenticateUser(principal, token));
  }

  @Test
  public void testUserAuthenticationUpdate() throws Exception {
    byte[] rawPass = "myPassword".getBytes(Charset.forName("UTF-8"));
    String principal = "myTestUser";

    PasswordToken token = null;
    if (!saslEnabled()) {
      token = new PasswordToken(rawPass.clone());
    }

    ZKAuthenticator auth = new ZKAuthenticator();
    auth.initialize(cluster.getServerContext());

    auth.createOutdatedUser(principal, token);
    assertTrue(auth.authenticateUser(principal, token));
  }
}
