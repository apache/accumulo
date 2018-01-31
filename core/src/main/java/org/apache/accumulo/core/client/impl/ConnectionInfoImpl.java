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
package org.apache.accumulo.core.client.impl;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import org.apache.accumulo.core.client.ConnectionInfo;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.CredentialProviderToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;

public class ConnectionInfoImpl implements ConnectionInfo {

  private Properties properties;

  ConnectionInfoImpl(Properties properties) {
    this.properties = properties;
  }

  @Override
  public String getInstanceName() {
    return getString(ClientProperty.INSTANCE_NAME);
  }

  @Override
  public String getZookeepers() {
    return getString(ClientProperty.INSTANCE_ZOOKEEPERS);
  }

  @Override
  public String getPrincipal() {
    String authType = getString(ClientProperty.AUTH_TYPE);
    String principal;
    switch (authType) {
      case "basic":
        principal = getString(ClientProperty.AUTH_BASIC_USERNAME);
        break;
      case "kerberos":
        principal = getString(ClientProperty.AUTH_KERBEROS_PRINCIPAL);
        break;
      case "provider":
        principal = getString(ClientProperty.AUTH_PROVIDER_USERNAME);
        break;
      default:
        throw new IllegalArgumentException("An authentication type (basic, kerberos, etc) must be set");
    }
    return principal;
  }

  @Override
  public Properties getProperties() {
    return properties;
  }

  @Override
  public AuthenticationToken getAuthenticationToken() {
    String authType = getString(ClientProperty.AUTH_TYPE).toLowerCase();
    switch (authType) {
      case "basic":
        String password = getString(ClientProperty.AUTH_BASIC_PASSWORD);
        Objects.nonNull(password);
        return new PasswordToken(password);
      case "kerberos":
        String principal = getString(ClientProperty.AUTH_KERBEROS_PRINCIPAL);
        String keytabPath = getString(ClientProperty.AUTH_KERBEROS_KEYTAB_PATH);
        Objects.nonNull(principal);
        Objects.nonNull(keytabPath);
        try {
          return new KerberosToken(principal, new File(keytabPath));
        } catch (IOException e) {
          throw new IllegalArgumentException(e);
        }
      case "provider":
        String name = getString(ClientProperty.AUTH_PROVIDER_NAME);
        String providerUrls = getString(ClientProperty.AUTH_PROVIDER_URLS);
        try {
          return new CredentialProviderToken(name, providerUrls);
        } catch (IOException e) {
          throw new IllegalArgumentException(e);
        }
      default:
        throw new IllegalArgumentException("An authentication type (basic, kerberos, etc) must be set");
    }
  }

  @Override
  public String getProperty(String key) {
    return properties.getProperty(key);
  }

  private String getString(ClientProperty property) {
    return property.getValue(properties);
  }
}
