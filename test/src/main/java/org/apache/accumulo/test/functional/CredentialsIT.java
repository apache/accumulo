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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CredentialsIT extends AccumuloClusterHarness {

  private boolean saslEnabled;
  private String username;
  private String password;
  private Instance inst;

  @Override
  public int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Before
  public void createLocalUser() throws AccumuloException, AccumuloSecurityException {
    Connector conn = getConnector();
    inst = conn.getInstance();

    ClientConfiguration clientConf = cluster.getClientConfig();
    ClusterUser user = getUser(0);
    username = user.getPrincipal();
    saslEnabled = clientConf.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false);
    // Create the user if it doesn't exist
    Set<String> users = conn.securityOperations().listLocalUsers();
    if (!users.contains(username)) {
      PasswordToken passwdToken = null;
      if (!saslEnabled) {
        password = user.getPassword();
        passwdToken = new PasswordToken(password);
      }
      conn.securityOperations().createLocalUser(username, passwdToken);
    }
  }

  @After
  public void deleteLocalUser() throws Exception {
    if (saslEnabled) {
      ClusterUser root = getAdminUser();
      UserGroupInformation.loginUserFromKeytab(root.getPrincipal(), root.getKeytab().getAbsolutePath());
    }
    getConnector().securityOperations().dropLocalUser(username);
  }

  @Test
  public void testConnectorWithDestroyedToken() throws Exception {
    AuthenticationToken token = getUser(0).getToken();
    assertFalse(token.isDestroyed());
    token.destroy();
    assertTrue(token.isDestroyed());
    try {
      inst.getConnector("non_existent_user", token);
      fail();
    } catch (AccumuloSecurityException e) {
      assertTrue(e.getSecurityErrorCode().equals(SecurityErrorCode.TOKEN_EXPIRED));
    }
  }

  @Test
  public void testDestroyTokenBeforeRPC() throws Exception {
    AuthenticationToken token = getUser(0).getToken();
    Connector userConnector = inst.getConnector(username, token);
    try (Scanner scanner = userConnector.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      assertFalse(token.isDestroyed());
      token.destroy();
      assertTrue(token.isDestroyed());
      try {
        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        while (iter.hasNext())
          fail();
        fail();
      } catch (Exception e) {
        assertTrue(e instanceof RuntimeException);
        assertTrue(e.getCause() instanceof AccumuloSecurityException);
        assertTrue(AccumuloSecurityException.class.cast(e.getCause()).getSecurityErrorCode().equals(SecurityErrorCode.TOKEN_EXPIRED));
      }
    }
  }

}
