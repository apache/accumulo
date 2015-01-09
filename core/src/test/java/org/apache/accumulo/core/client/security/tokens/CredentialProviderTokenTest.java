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
package org.apache.accumulo.core.client.security.tokens;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.Properties;
import org.apache.accumulo.core.conf.CredentialProviderFactoryShim;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CredentialProviderTokenTest {

  private static boolean isCredentialProviderAvailable = false;

  // Keystore contains: {'root.password':'password', 'bob.password':'bob'}
  private static String keystorePath;

  @BeforeClass
  public static void setup() {
    try {
      Class.forName(CredentialProviderFactoryShim.HADOOP_CRED_PROVIDER_CLASS_NAME);
      isCredentialProviderAvailable = true;
    } catch (Exception e) {
      isCredentialProviderAvailable = false;
    }

    URL keystoreUrl = CredentialProviderTokenTest.class.getResource("/passwords.jceks");
    Assert.assertNotNull(keystoreUrl);
    keystorePath = "jceks://file/" + new File(keystoreUrl.getFile()).getAbsolutePath();
  }

  @Test
  public void testPasswordsFromCredentialProvider() throws Exception {
    if (!isCredentialProviderAvailable) {
      return;
    }

    CredentialProviderToken token = new CredentialProviderToken("root.password", keystorePath);
    Assert.assertArrayEquals("password".getBytes(UTF_8), token.getPassword());

    token = new CredentialProviderToken("bob.password", keystorePath);
    Assert.assertArrayEquals("bob".getBytes(UTF_8), token.getPassword());
  }

  @Test
  public void testEqualityAfterInit() throws Exception {
    if (!isCredentialProviderAvailable) {
      return;
    }

    CredentialProviderToken token = new CredentialProviderToken("root.password", keystorePath);

    CredentialProviderToken uninitializedToken = new CredentialProviderToken();
    Properties props = new Properties();
    props.put(CredentialProviderToken.NAME_PROPERTY, "root.password");
    props.put(CredentialProviderToken.CREDENTIAL_PROVIDERS_PROPERTY, keystorePath);
    uninitializedToken.init(props);

    Assert.assertArrayEquals(token.getPassword(), uninitializedToken.getPassword());
  }

  @Test
  public void testMissingClassesThrowsException() throws Exception {
    if (isCredentialProviderAvailable) {
      return;
    }

    try {
      new CredentialProviderToken("root.password", keystorePath);
      Assert.fail("Should fail to create CredentialProviderToken when classes are not available");
    } catch (IOException e) {
      // pass
    }
  }

  @Test
  public void cloneReturnsCorrectObject() throws Exception {
    if (!isCredentialProviderAvailable) {
      return;
    }

    CredentialProviderToken token = new CredentialProviderToken("root.password", keystorePath);
    CredentialProviderToken clone = token.clone();

    Assert.assertEquals(token, clone);
    Assert.assertArrayEquals(token.getPassword(), clone.getPassword());
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingProperties() throws Exception {
    CredentialProviderToken token = new CredentialProviderToken();
    token.init(new Properties());
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingNameProperty() throws Exception {
    CredentialProviderToken token = new CredentialProviderToken();
    Properties props = new Properties();
    props.put(CredentialProviderToken.NAME_PROPERTY, "root.password");
    token.init(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingProviderProperty() throws Exception {
    CredentialProviderToken token = new CredentialProviderToken();
    Properties props = new Properties();
    props.put(CredentialProviderToken.CREDENTIAL_PROVIDERS_PROPERTY, keystorePath);
    token.init(props);
  }
}
