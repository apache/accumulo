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
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ConnectionInfo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;

public class ConnectionInfoImpl implements ConnectionInfo {

  private Properties properties;

  public ConnectionInfoImpl(Properties properties) {
    this.properties = properties;
  }

  public ConnectionInfoImpl() {
    this(new Properties());
  }

  public Properties getProperties() {
    return properties;
  }

  public String getString(ClientProperty property) {
    return property.getValue(properties);
  }

  public Long getLong(ClientProperty property) {
    return property.getLong(properties);
  }

  public void setProperty(ClientProperty property, String value) {
    properties.setProperty(property.getKey(), value);
  }

  public void setProperty(ClientProperty property, Long value) {
    setProperty(property, Long.toString(value));
  }

  public void setProperty(ClientProperty property, Integer value) {
    setProperty(property, Integer.toString(value));
  }

  public Connector getConnector() throws AccumuloSecurityException, AccumuloException {
    return new ConnectorImpl(getClientContext());
  }

  public ClientContext getClientContext() {
    return new ClientContext(getInstance(), getCredentials(), getClientConfiguration(), getBatchWriterConfig());
  }

  public Instance getInstance() {
    String instanceName = getString(ClientProperty.INSTANCE_NAME);
    String zookeepers = getString(ClientProperty.INSTANCE_ZOOKEEPERS);
    return new ZooKeeperInstance(instanceName, zookeepers);
  }

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
      default:
        throw new IllegalArgumentException("An authentication type (basic, kerberos, etc) must be set");
    }
    Objects.nonNull(principal);
    return principal;
  }

  public AuthenticationToken getAuthenticationToken() {
    String authType = getString(ClientProperty.AUTH_TYPE);
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
      default:
        throw new IllegalArgumentException("An authentication type (basic, kerberos, etc) must be set");
    }
  }

  public Credentials getCredentials() {
    return new Credentials(getPrincipal(), getAuthenticationToken());
  }

  public BatchWriterConfig getBatchWriterConfig() {
    BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
    Long maxMemory = getLong(ClientProperty.BATCH_WRITER_MAX_MEMORY_BYTES);
    if (maxMemory != null) {
      batchWriterConfig.setMaxMemory(maxMemory);
    }
    Long maxLatency = getLong(ClientProperty.BATCH_WRITER_MAX_LATENCY_SEC);
    if (maxLatency != null) {
      batchWriterConfig.setMaxLatency(maxLatency, TimeUnit.SECONDS);
    }
    Long timeout = getLong(ClientProperty.BATCH_WRITER_MAX_TIMEOUT_SEC);
    if (timeout != null) {
      batchWriterConfig.setTimeout(timeout, TimeUnit.SECONDS);
    }
    String durability = getString(ClientProperty.BATCH_WRITER_DURABILITY);
    if (!durability.isEmpty()) {
      batchWriterConfig.setDurability(Durability.valueOf(durability.toUpperCase()));
    }
    return batchWriterConfig;
  }

  public ClientConfiguration getClientConfiguration() {
    ClientConfiguration config = ClientConfiguration.create();
    for (Object keyObj : properties.keySet()) {
      String key = (String) keyObj;
      String val = properties.getProperty(key);
      if (key.equals(ClientProperty.INSTANCE_ZOOKEEPERS.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST, val);
      } else if (key.equals(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT_SEC.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.INSTANCE_ZK_TIMEOUT, val);
      } else if (key.equals(ClientProperty.SSL_ENABLED.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.INSTANCE_RPC_SSL_ENABLED, val);
      } else if (key.equals(ClientProperty.SSL_KEYSTORE_PATH.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.RPC_SSL_KEYSTORE_PATH, val);
        config.setProperty(ClientConfiguration.ClientProperty.INSTANCE_RPC_SSL_CLIENT_AUTH, "true");
      } else if (key.equals(ClientProperty.SSL_KEYSTORE_TYPE.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.RPC_SSL_KEYSTORE_TYPE, val);
      } else if (key.equals(ClientProperty.SSL_KEYSTORE_PASSWORD.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.RPC_SSL_KEYSTORE_PASSWORD, val);
      } else if (key.equals(ClientProperty.SSL_TRUSTSTORE_PATH.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_PATH, val);
      } else if (key.equals(ClientProperty.SSL_TRUSTSTORE_TYPE.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_PATH, val);
      } else if (key.equals(ClientProperty.SSL_TRUSTSTORE_PASSWORD.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_PATH, val);
      } else if (key.equals(ClientProperty.SSL_USE_JSSE.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.RPC_USE_JSSE, val);
      } else if (key.equals(ClientProperty.SASL_ENABLED.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.INSTANCE_RPC_SSL_ENABLED, val);
      } else if (key.equals(ClientProperty.SASL_QOP.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.RPC_SASL_QOP, val);
      } else if (key.equals(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY, val);
      } else {
        config.setProperty(key, val);
      }
    }
    return config;
  }
}
