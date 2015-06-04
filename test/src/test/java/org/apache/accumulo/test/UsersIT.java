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
package org.apache.accumulo.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.Test;

public class UsersIT extends AccumuloClusterHarness {

  @Test
  public void testCreateExistingUser() throws Exception {
    ClusterUser user0 = getUser(0);
    Connector conn = getConnector();
    Set<String> currentUsers = conn.securityOperations().listLocalUsers();

    // Ensure that the user exists
    if (!currentUsers.contains(user0.getPrincipal())) {
      PasswordToken token = null;
      if (!getCluster().getClientConfig().getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
        token = new PasswordToken(user0.getPassword());
      }
      conn.securityOperations().createLocalUser(user0.getPrincipal(), token);
    }

    try {
      conn.securityOperations().createLocalUser(user0.getPrincipal(), new PasswordToken("better_fail"));
      fail("Creating a user that already exists should throw an exception");
    } catch (AccumuloSecurityException e) {
      assertTrue("Expected USER_EXISTS error", SecurityErrorCode.USER_EXISTS == e.getSecurityErrorCode());
      String msg = e.getMessage();
      assertTrue("Error message didn't contain principal: '" + msg + "'", msg.contains(user0.getPrincipal()));
    }
  }

}
