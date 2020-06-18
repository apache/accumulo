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
package org.apache.accumulo.core.clientImpl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.hadoop.conf.Configuration;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ClientInfoImpl implements ClientInfo {

  private Properties properties;
  private AuthenticationToken token;
  private Configuration hadoopConf;

  public ClientInfoImpl(Path propertiesFile) {
    this(ClientInfoImpl.toProperties(propertiesFile));
  }

  public ClientInfoImpl(Properties properties) {
    this(properties, null);
  }

  public ClientInfoImpl(Properties properties, AuthenticationToken token) {
    this.properties = properties;
    this.token = token;
    this.hadoopConf = new Configuration();
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
    properties.forEach((key, value) -> result.setProperty((String) key, (String) value));
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
      justification = "code runs in same security context as user who provided propertiesFilePath")
  public static Properties toProperties(String propertiesFilePath) {
    return toProperties(Paths.get(propertiesFilePath));
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided propertiesFile")
  public static Properties toProperties(Path propertiesFile) {
    Properties properties = new Properties();
    try (InputStream is = new FileInputStream(propertiesFile.toFile())) {
      properties.load(is);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to load properties from " + propertiesFile, e);
    }
    return properties;
  }

  @Override
  public Configuration getHadoopConf() {
    return this.hadoopConf;
  }
}
