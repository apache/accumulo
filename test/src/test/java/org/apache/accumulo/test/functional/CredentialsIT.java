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

import javax.security.auth.DestroyFailedException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class CredentialsIT extends AccumuloClusterIT {

  private static final String username = CredentialsIT.class.getSimpleName();
  private static final String password = Base64.encodeBase64String(username.getBytes());

  @Override
  public int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Before
  public void createLocalUser() throws AccumuloException, AccumuloSecurityException {
    getConnector().securityOperations().createLocalUser(username, new PasswordToken(password));
  }

  @After
  public void deleteLocalUser() throws AccumuloException, AccumuloSecurityException {
    getConnector().securityOperations().dropLocalUser(username);
  }

  @Test
  public void testConnectorWithDestroyedToken() throws DestroyFailedException, AccumuloException {
    PasswordToken token = new PasswordToken(password);
    assertFalse(token.isDestroyed());
    token.destroy();
    assertTrue(token.isDestroyed());
    try {
      getConnector().getInstance().getConnector("localUser", token);
      fail();
    } catch (AccumuloSecurityException e) {
      assertTrue(e.getSecurityErrorCode().equals(SecurityErrorCode.TOKEN_EXPIRED));
    }
  }

  @Test
  public void testDestroyTokenBeforeRPC() throws AccumuloException, DestroyFailedException, AccumuloSecurityException, TableNotFoundException {
    PasswordToken token = new PasswordToken(password);
    Connector userConnector = getConnector().getInstance().getConnector(username, token);
    Scanner scanner = userConnector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
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
