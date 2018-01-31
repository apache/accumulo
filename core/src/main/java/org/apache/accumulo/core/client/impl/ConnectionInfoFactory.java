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
import org.apache.accumulo.core.conf.ClientProperty;

/**
 * Creates internal objects using {@link ConnectionInfo}
 */
public class ConnectionInfoFactory {

  public static String getString(ConnectionInfo info, ClientProperty property) {
    return property.getValue(info.getProperties());
  }

  public static Long getLong(ConnectionInfo info, ClientProperty property) {
    return property.getLong(info.getProperties());
  }

  public static Connector getConnector(ConnectionInfo info) throws AccumuloSecurityException, AccumuloException {
    return new ConnectorImpl(getClientContext(info));
  }

  public static ClientContext getClientContext(ConnectionInfo info) {
    return new ClientContext(getInstance(info), getCredentials(info), getClientConfiguration(info), getBatchWriterConfig(info));
  }

  public static Instance getInstance(ConnectionInfo info) {
    String instanceName = getString(info, ClientProperty.INSTANCE_NAME);
    String zookeepers = getString(info, ClientProperty.INSTANCE_ZOOKEEPERS);
    return new ZooKeeperInstance(instanceName, zookeepers);
  }

  public static Credentials getCredentials(ConnectionInfo info) {
    return new Credentials(info.getPrincipal(), info.getAuthenticationToken());
  }

  public static BatchWriterConfig getBatchWriterConfig(ConnectionInfo info) {
    BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
    Long maxMemory = getLong(info, ClientProperty.BATCH_WRITER_MAX_MEMORY_BYTES);
    if (maxMemory != null) {
      batchWriterConfig.setMaxMemory(maxMemory);
    }
    Long maxLatency = getLong(info, ClientProperty.BATCH_WRITER_MAX_LATENCY_SEC);
    if (maxLatency != null) {
      batchWriterConfig.setMaxLatency(maxLatency, TimeUnit.SECONDS);
    }
    Long timeout = getLong(info, ClientProperty.BATCH_WRITER_MAX_TIMEOUT_SEC);
    if (timeout != null) {
      batchWriterConfig.setTimeout(timeout, TimeUnit.SECONDS);
    }
    String durability = getString(info, ClientProperty.BATCH_WRITER_DURABILITY);
    if (!durability.isEmpty()) {
      batchWriterConfig.setDurability(Durability.valueOf(durability.toUpperCase()));
    }
    return batchWriterConfig;
  }

  public static ClientConfiguration getClientConfiguration(ConnectionInfo info) {
    ClientConfiguration config = ClientConfiguration.create();
    for (Object keyObj : info.getProperties().keySet()) {
      String key = (String) keyObj;
      String val = info.getProperties().getProperty(key);
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
