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
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Suppliers.memoize;
import static java.util.Objects.requireNonNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Suppliers;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ClientInfoImpl implements ClientInfo {

  private final Properties properties;

  // suppliers for lazily loading
  private final Supplier<AuthenticationToken> tokenSupplier;
  private final Supplier<Configuration> hadoopConf;
  private final BiFunction<String,String,ZooSession> zooSessionForName;

  public ClientInfoImpl(Properties properties, Optional<AuthenticationToken> tokenOpt) {
    this.properties = requireNonNull(properties);
    // convert the optional to a supplier to delay retrieval from the properties unless needed
    this.tokenSupplier = requireNonNull(tokenOpt).map(Suppliers::ofInstance)
        .orElse(memoize(() -> ClientProperty.getAuthenticationToken(properties)));
    this.hadoopConf = memoize(Configuration::new);
    this.zooSessionForName = (name, rootPath) -> new ZooSession(name, getZooKeepers() + rootPath,
        getZooKeepersSessionTimeOut(), null);
  }

  @Override
  public String getInstanceName() {
    return getString(ClientProperty.INSTANCE_NAME);
  }

  @Override
  public Supplier<ZooSession> getZooKeeperSupplier(String clientName, String rootPath) {
    return () -> zooSessionForName.apply(requireNonNull(clientName), requireNonNull(rootPath));
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
  public Properties getClientProperties() {
    Properties result = new Properties();
    properties.forEach((key, value) -> result.setProperty((String) key, (String) value));
    return result;
  }

  @Override
  public AuthenticationToken getAuthenticationToken() {
    return tokenSupplier.get();
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

  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "code runs in same security context as user who provided propertiesURL")
  public static Properties toProperties(URL propertiesURL) {
    Properties properties = new Properties();
    try (InputStream is = propertiesURL.openStream()) {
      properties.load(is);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to load properties from " + propertiesURL, e);
    }
    return properties;
  }

  @Override
  public Configuration getHadoopConf() {
    return hadoopConf.get();
  }
}
