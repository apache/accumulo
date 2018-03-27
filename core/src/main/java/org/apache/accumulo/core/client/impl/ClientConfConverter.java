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

import java.util.Iterator;
import java.util.Properties;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;

public class ClientConfConverter {

  public static ClientConfiguration toClientConf(Properties properties) {
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
        config.setProperty(ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_TYPE, val);
      } else if (key.equals(ClientProperty.SSL_TRUSTSTORE_PASSWORD.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_PASSWORD, val);
      } else if (key.equals(ClientProperty.SSL_USE_JSSE.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.RPC_USE_JSSE, val);
      } else if (key.equals(ClientProperty.SASL_ENABLED.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.INSTANCE_RPC_SASL_ENABLED, val);
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

  public static Properties toProperties(ClientConfiguration clientConf) {
    Properties props = new Properties();

    Iterator<String> clientConfIter = clientConf.getKeys();
    while (clientConfIter.hasNext()) {
      String key = clientConfIter.next();
      String val = clientConf.getString(key);
      if (key.equals(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST.getKey())) {
        props.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), val);
      } else if (key.equals(ClientConfiguration.ClientProperty.INSTANCE_ZK_TIMEOUT.getKey())) {
        props.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT_SEC.getKey(), val);
      } else if (key.equals(ClientConfiguration.ClientProperty.INSTANCE_RPC_SSL_ENABLED.getKey())) {
        props.setProperty(ClientProperty.SSL_ENABLED.getKey(), val);
      } else if (key.equals(ClientConfiguration.ClientProperty.RPC_SSL_KEYSTORE_PATH.getKey())) {
        props.setProperty(ClientProperty.SSL_KEYSTORE_PATH.getKey(), val);
      } else if (key.equals(ClientConfiguration.ClientProperty.RPC_SSL_KEYSTORE_TYPE.getKey())) {
        props.setProperty(ClientProperty.SSL_KEYSTORE_TYPE.getKey(), val);
      } else if (key.equals(ClientConfiguration.ClientProperty.RPC_SSL_KEYSTORE_PASSWORD.getKey())) {
        props.setProperty(ClientProperty.SSL_KEYSTORE_PASSWORD.getKey(), val);
      } else if (key.equals(ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_PATH.getKey())) {
        props.setProperty(ClientProperty.SSL_TRUSTSTORE_PATH.getKey(), val);
      } else if (key.equals(ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_TYPE.getKey())) {
        props.setProperty(ClientProperty.SSL_TRUSTSTORE_TYPE.getKey(), val);
      } else if (key.equals(ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_PASSWORD.getKey())) {
        props.setProperty(ClientProperty.SSL_TRUSTSTORE_PASSWORD.getKey(), val);
      } else if (key.equals(ClientConfiguration.ClientProperty.RPC_USE_JSSE.getKey())) {
        props.setProperty(ClientProperty.SSL_USE_JSSE.getKey(), val);
      } else if (key.equals(ClientConfiguration.ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey())) {
        props.setProperty(ClientProperty.SASL_ENABLED.getKey(), val);
      } else if (key.equals(ClientConfiguration.ClientProperty.RPC_SASL_QOP.getKey())) {
        props.setProperty(ClientProperty.SASL_QOP.getKey(), val);
      } else if (key.equals(ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY.getKey())) {
        props.setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey(), val);
      } else {
        props.setProperty(key, val);
      }
    }
    return props;
  }
}
