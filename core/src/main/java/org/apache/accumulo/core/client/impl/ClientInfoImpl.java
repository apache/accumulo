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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;

public class ClientInfoImpl implements ClientInfo {

  private Properties properties;
  private AuthenticationToken token;

  public ClientInfoImpl(String configFile) {
    this(ClientInfoImpl.toProperties(configFile));
  }

  public ClientInfoImpl(Properties properties) {
    this(properties, null);
  }

  public ClientInfoImpl(Properties properties, AuthenticationToken token) {
    this.properties = properties;
    this.token = token;
  }

  @Override
  public String getInstanceName() {
    return getString(ClientProperty.INSTANCE_NAME);
  }

  @Override
  public String getZooKeepers() {
    return getString(ClientProperty.INSTANCE_ZOOKEEPERS);
  }

  @Override
  public int getZooKeepersSessionTimeOut() {
    return (int) ConfigurationTypeHelper
        .getTimeInMillis(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getValue(properties));
  }

  @Override
  public String getPrincipal() {
    return getString(ClientProperty.AUTH_PRINCIPAL);
  }

  @Override
  public Properties getProperties() {
    Properties result = new Properties();
    properties.forEach((key, value) -> {
      result.setProperty((String) key, (String) value);
    });
    return result;
  }

  @Override
  public AuthenticationToken getAuthenticationToken() {
    if (token == null) {
      token = ClientProperty.getAuthenticationToken(properties);
    }
    return token;
  }

  @Override
  public boolean saslEnabled() {
    return Boolean.valueOf(getString(ClientProperty.SASL_ENABLED));
  }

  private String getString(ClientProperty property) {
    return property.getValue(properties);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided configFile")
  public static Properties toProperties(String configFile) {
    Properties properties = new Properties();
    try (InputStream is = new FileInputStream(configFile)) {
      properties.load(is);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to load properties from " + configFile, e);
    }
    return properties;
  }
}
