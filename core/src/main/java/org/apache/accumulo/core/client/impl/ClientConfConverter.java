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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;

@SuppressWarnings("deprecation")
public class ClientConfConverter {

  private static Map<String, String> confProps = new HashMap<>();
  private static Map<String, String> propsConf = new HashMap<>();

  static {
    propsConf.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(),
        ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST.getKey());
    propsConf.put(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT_SEC.getKey(),
        ClientConfiguration.ClientProperty.INSTANCE_ZK_TIMEOUT.getKey());
    propsConf.put(ClientProperty.SSL_ENABLED.getKey(),
        ClientConfiguration.ClientProperty.INSTANCE_RPC_SSL_ENABLED.getKey());
    propsConf.put(ClientProperty.SSL_KEYSTORE_PATH.getKey(),
        ClientConfiguration.ClientProperty.RPC_SSL_KEYSTORE_PATH.getKey());
    propsConf.put(ClientProperty.SSL_KEYSTORE_TYPE.getKey(),
        ClientConfiguration.ClientProperty.RPC_SSL_KEYSTORE_TYPE.getKey());
    propsConf.put(ClientProperty.SSL_KEYSTORE_PASSWORD.getKey(),
        ClientConfiguration.ClientProperty.RPC_SSL_KEYSTORE_PASSWORD.getKey());
    propsConf.put(ClientProperty.SSL_TRUSTSTORE_PATH.getKey(),
        ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_PATH.getKey());
    propsConf.put(ClientProperty.SSL_TRUSTSTORE_TYPE.getKey(),
        ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_TYPE.getKey());
    propsConf.put(ClientProperty.SSL_TRUSTSTORE_PASSWORD.getKey(),
        ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_PASSWORD.getKey());
    propsConf.put(ClientProperty.SSL_USE_JSSE.getKey(),
        ClientConfiguration.ClientProperty.RPC_USE_JSSE.getKey());
    propsConf.put(ClientProperty.SASL_ENABLED.getKey(),
        ClientConfiguration.ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey());
    propsConf.put(ClientProperty.SASL_QOP.getKey(),
        ClientConfiguration.ClientProperty.RPC_SASL_QOP.getKey());
    propsConf.put(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey(),
        ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY.getKey());

    for (Map.Entry<String, String> entry : propsConf.entrySet()) {
      confProps.put(entry.getValue(), entry.getKey());
    }
  }

  public static ClientConfiguration toClientConf(Properties properties) {
    ClientConfiguration config = ClientConfiguration.create();
    for (Object keyObj : properties.keySet()) {
      String propKey = (String) keyObj;
      String val = properties.getProperty(propKey);
      String confKey = propsConf.get(propKey);
      if (confKey == null) {
        config.setProperty(propKey, val);
      } else {
        config.setProperty(confKey, val);
      }
      if (propKey.equals(ClientProperty.SSL_KEYSTORE_PATH.getKey())) {
        config.setProperty(ClientConfiguration.ClientProperty.INSTANCE_RPC_SSL_CLIENT_AUTH, "true");
      }
    }
    return config;
  }

  public static Properties toProperties(ClientConfiguration clientConf) {
    Properties props = new Properties();
    Iterator<String> clientConfIter = clientConf.getKeys();
    while (clientConfIter.hasNext()) {
      String confKey = clientConfIter.next();
      String val = clientConf.getString(confKey);
      String propKey = confProps.get(confKey);
      if (propKey == null) {
        if (!confKey.equals(ClientConfiguration.ClientProperty.INSTANCE_RPC_SSL_CLIENT_AUTH.getKey())) {
          props.setProperty(confKey, val);
        }
      } else {
        props.setProperty(propKey, val);
      }
    }
    return props;
  }
}
