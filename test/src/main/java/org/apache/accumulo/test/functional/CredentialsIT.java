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
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CredentialsIT extends AccumuloClusterHarness {

  private boolean saslEnabled;
  private String username;
  private String password;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @BeforeEach
  public void createLocalUser() throws AccumuloException, AccumuloSecurityException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      ClusterUser user = getUser(0);
      username = user.getPrincipal();
      saslEnabled = saslEnabled();
      // Create the user if it doesn't exist
      Set<String> users = client.securityOperations().listLocalUsers();
      if (!users.contains(username)) {
        PasswordToken passwdToken = null;
        if (!saslEnabled) {
          password = user.getPassword();
          passwdToken = new PasswordToken(password);
        }
        client.securityOperations().createLocalUser(username, passwdToken);
      }
    }
  }

  @AfterEach
  public void deleteLocalUser() throws Exception {
    if (saslEnabled) {
      ClusterUser root = getAdminUser();
      UserGroupInformation.loginUserFromKeytab(root.getPrincipal(),
          root.getKeytab().getAbsolutePath());
    }
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.securityOperations().dropLocalUser(username);
    }
  }

  @Test
  public void testConnectorWithDestroyedToken() throws Exception {
    AuthenticationToken token = getUser(0).getToken();
    assertFalse(token.isDestroyed());
    token.destroy();
    assertTrue(token.isDestroyed());
    Properties props = getClientProps();
    var e = assertThrows(IllegalArgumentException.class,
        () -> Accumulo.newClient().from(props).as("non_existent_user", token).build().close());
    assertEquals(e.getMessage(), "AuthenticationToken has been destroyed");
  }

  @Test
  public void testDestroyTokenBeforeRPC() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      AuthenticationToken token = getUser(0).getToken();
      try (
          AccumuloClient userAccumuloClient =
              Accumulo.newClient().from(client.properties()).as(username, token).build();
          Scanner scanner =
              userAccumuloClient.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        assertFalse(token.isDestroyed());
        token.destroy();
        assertTrue(token.isDestroyed());

        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        var e = assertThrows(RuntimeException.class, iter::hasNext);
        assertTrue(e.getCause() instanceof AccumuloSecurityException);
        assertEquals(((AccumuloSecurityException) e.getCause()).getSecurityErrorCode(),
            SecurityErrorCode.TOKEN_EXPIRED);
      }
    }
  }
}
