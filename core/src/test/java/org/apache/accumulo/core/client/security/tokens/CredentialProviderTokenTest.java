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
package org.apache.accumulo.core.client.security.tokens;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.net.URL;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CredentialProviderTokenTest {

  // Keystore contains: {'root.password':'password', 'bob.password':'bob'}
  private static String keystorePath;

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "keystoreUrl location isn't provided by user input")
  @BeforeAll
  public static void setup() {
    URL keystoreUrl = CredentialProviderTokenTest.class.getResource("/passwords.jceks");
    assertNotNull(keystoreUrl);
    keystorePath = "jceks://file/" + new File(keystoreUrl.getFile()).getAbsolutePath();
  }

  @Test
  public void testPasswordsFromCredentialProvider() throws Exception {
    CredentialProviderToken token = new CredentialProviderToken("root.password", keystorePath);
    assertEquals("root.password", token.getName());
    assertEquals(keystorePath, token.getCredentialProviders());
    assertArrayEquals("password".getBytes(UTF_8), token.getPassword());

    token = new CredentialProviderToken("bob.password", keystorePath);
    assertArrayEquals("bob".getBytes(UTF_8), token.getPassword());
  }

  @Test
  public void testEqualityAfterInit() throws Exception {
    CredentialProviderToken token = new CredentialProviderToken("root.password", keystorePath);

    CredentialProviderToken uninitializedToken = new CredentialProviderToken();
    Properties props = new Properties();
    props.put(CredentialProviderToken.NAME_PROPERTY, "root.password");
    props.put(CredentialProviderToken.CREDENTIAL_PROVIDERS_PROPERTY, keystorePath);
    uninitializedToken.init(props);

    assertArrayEquals(token.getPassword(), uninitializedToken.getPassword());
  }

  @Test
  public void cloneReturnsCorrectObject() throws Exception {
    CredentialProviderToken token = new CredentialProviderToken("root.password", keystorePath);
    CredentialProviderToken clone = token.clone();

    assertEquals(token, clone);
    assertArrayEquals(token.getPassword(), clone.getPassword());
  }

  @Test
  public void missingProperties() {
    CredentialProviderToken token = new CredentialProviderToken();
    assertThrows(IllegalArgumentException.class, () -> token.init(new Properties()));
  }

  @Test
  public void missingNameProperty() {
    CredentialProviderToken token = new CredentialProviderToken();
    Properties props = new Properties();
    props.put(CredentialProviderToken.NAME_PROPERTY, "root.password");
    assertThrows(IllegalArgumentException.class, () -> token.init(props));
  }

  @Test
  public void missingProviderProperty() {
    CredentialProviderToken token = new CredentialProviderToken();
    Properties props = new Properties();
    props.put(CredentialProviderToken.CREDENTIAL_PROVIDERS_PROPERTY, keystorePath);
    assertThrows(IllegalArgumentException.class, () -> token.init(props));
  }
}
