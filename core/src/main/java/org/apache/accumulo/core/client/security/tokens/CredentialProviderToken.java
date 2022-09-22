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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.accumulo.core.conf.HadoopCredentialProvider;
import org.apache.hadoop.conf.Configuration;

/**
 * An {@link AuthenticationToken} backed by a Hadoop CredentialProvider.
 */
public class CredentialProviderToken extends PasswordToken {
  public static final String NAME_PROPERTY = "name",
      CREDENTIAL_PROVIDERS_PROPERTY = "credentialProviders";

  private String name;
  private String credentialProviders;

  public CredentialProviderToken() {}

  public CredentialProviderToken(String name, String credentialProviders) throws IOException {
    requireNonNull(name);
    requireNonNull(credentialProviders);
    setWithCredentialProviders(name, credentialProviders);
  }

  protected void setWithCredentialProviders(String name, String credentialProviders)
      throws IOException {
    this.name = name;
    this.credentialProviders = credentialProviders;
    final Configuration conf = new Configuration();
    HadoopCredentialProvider.setPath(conf, credentialProviders);

    char[] password = HadoopCredentialProvider.getValue(conf, name);

    if (password == null) {
      throw new IOException(
          "No password could be extracted from CredentialProvider(s) with " + name);
    }

    setPassword(CharBuffer.wrap(password));
  }

  /**
   * @return Name used to extract Accumulo user password from CredentialProvider
   *
   * @since 2.0.0
   */
  public String getName() {
    return name;
  }

  /**
   * @return CSV list of CredentialProvider(s)
   *
   * @since 2.0.0
   */
  public String getCredentialProviders() {
    return credentialProviders;
  }

  @Override
  public void init(Properties properties) {
    char[] nameCharArray = properties.get(NAME_PROPERTY),
        credentialProvidersCharArray = properties.get(CREDENTIAL_PROVIDERS_PROPERTY);
    if (nameCharArray != null && credentialProvidersCharArray != null) {
      try {
        this.setWithCredentialProviders(new String(nameCharArray),
            new String(credentialProvidersCharArray));
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not extract password from CredentialProvider", e);
      }

      return;
    }

    throw new IllegalArgumentException(
        "Expected " + NAME_PROPERTY + " and " + CREDENTIAL_PROVIDERS_PROPERTY + " properties.");
  }

  @Override
  public Set<TokenProperty> getProperties() {
    LinkedHashSet<TokenProperty> properties = new LinkedHashSet<>();
    // Neither name or CPs are sensitive
    properties
        .add(new TokenProperty(NAME_PROPERTY, "Alias to extract from CredentialProvider", false));
    properties.add(new TokenProperty(CREDENTIAL_PROVIDERS_PROPERTY,
        "Comma separated list of URLs defining CredentialProvider(s)", false));
    return properties;
  }

  @Override
  public CredentialProviderToken clone() {
    CredentialProviderToken clone = (CredentialProviderToken) super.clone();
    clone.setPassword(this.getPassword());
    return clone;
  }

}
