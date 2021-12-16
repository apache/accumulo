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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.HadoopCredentialProvider;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SaslConnectionParams;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientConfConverter {

  private static final Logger log = LoggerFactory.getLogger(ClientConfConverter.class);
  private static Map<String,String> confProps = new HashMap<>();
  private static Map<String,String> propsConf = new HashMap<>();

  @SuppressWarnings("deprecation")
  private static void init() {
    propsConf.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST
            .getKey());
    propsConf.put(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.INSTANCE_ZK_TIMEOUT
            .getKey());
    propsConf.put(ClientProperty.SSL_ENABLED.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.INSTANCE_RPC_SSL_ENABLED
            .getKey());
    propsConf.put(ClientProperty.SSL_KEYSTORE_PATH.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.RPC_SSL_KEYSTORE_PATH
            .getKey());
    propsConf.put(ClientProperty.SSL_KEYSTORE_TYPE.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.RPC_SSL_KEYSTORE_TYPE
            .getKey());
    propsConf.put(ClientProperty.SSL_KEYSTORE_PASSWORD.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.RPC_SSL_KEYSTORE_PASSWORD
            .getKey());
    propsConf.put(ClientProperty.SSL_TRUSTSTORE_PATH.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_PATH
            .getKey());
    propsConf.put(ClientProperty.SSL_TRUSTSTORE_TYPE.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_TYPE
            .getKey());
    propsConf.put(ClientProperty.SSL_TRUSTSTORE_PASSWORD.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.RPC_SSL_TRUSTSTORE_PASSWORD
            .getKey());
    propsConf.put(ClientProperty.SSL_USE_JSSE.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.RPC_USE_JSSE.getKey());
    propsConf.put(ClientProperty.SASL_ENABLED.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.INSTANCE_RPC_SASL_ENABLED
            .getKey());
    propsConf.put(ClientProperty.SASL_QOP.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.RPC_SASL_QOP.getKey());
    propsConf.put(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey(),
        org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY
            .getKey());

    for (Map.Entry<String,String> entry : propsConf.entrySet()) {
      confProps.put(entry.getValue(), entry.getKey());
    }
  }

  static {
    init();
  }

  @SuppressWarnings("deprecation")
  public static org.apache.accumulo.core.client.ClientConfiguration
      toClientConf(Properties properties) {
    org.apache.accumulo.core.client.ClientConfiguration config =
        org.apache.accumulo.core.client.ClientConfiguration.create();
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
        config.setProperty(
            org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.INSTANCE_RPC_SSL_CLIENT_AUTH,
            "true");
      }
    }
    return config;
  }

  @SuppressWarnings("deprecation")
  public static Properties
      toProperties(org.apache.accumulo.core.client.ClientConfiguration clientConf) {
    Properties props = new Properties();
    Iterator<String> clientConfIter = clientConf.getKeys();
    while (clientConfIter.hasNext()) {
      String confKey = clientConfIter.next();
      String val = clientConf.getString(confKey);
      String propKey = confProps.get(confKey);
      if (propKey == null) {
        if (!confKey.equals(
            org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.INSTANCE_RPC_SSL_CLIENT_AUTH
                .getKey())) {
          props.setProperty(confKey, val);
        }
      } else {
        props.setProperty(propKey, val);
      }
    }
    return props;
  }

  public static Properties toProperties(AccumuloConfiguration config) {
    return toProperties(toClientConf(config));
  }

  public static AccumuloConfiguration toAccumuloConf(Properties properties) {
    return toAccumuloConf(toClientConf(properties));
  }

  /**
   * A utility method for converting client configuration to a standard configuration object for use
   * internally.
   *
   * @param config
   *          the original config
   * @return the client configuration presented in the form of an {@link AccumuloConfiguration}
   */
  @SuppressWarnings("deprecation")
  public static AccumuloConfiguration
      toAccumuloConf(final org.apache.accumulo.core.client.ClientConfiguration config) {

    final AccumuloConfiguration defaults = DefaultConfiguration.getInstance();

    return new AccumuloConfiguration() {

      @Override
      public String get(Property property) {
        final String key = property.getKey();

        // Attempt to load sensitive properties from a CredentialProvider, if configured
        if (property.isSensitive()) {
          org.apache.hadoop.conf.Configuration hadoopConf = getHadoopConfiguration();
          if (hadoopConf != null) {
            char[] value = HadoopCredentialProvider.getValue(hadoopConf, key);
            if (value != null) {
              log.trace("Loaded sensitive value for {} from CredentialProvider", key);
              return new String(value);
            } else {
              log.trace("Tried to load sensitive value for {} from CredentialProvider, "
                  + "but none was found", key);
            }
          }
        }

        if (config.containsKey(key)) {
          return config.getString(key);
        } else {
          // Reconstitute the server kerberos property from the client config
          if (property == Property.GENERAL_KERBEROS_PRINCIPAL) {
            if (config.containsKey(
                org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY
                    .getKey())) {
              // Avoid providing a realm since we don't know what it is...
              return config.getString(
                  org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY
                      .getKey())
                  + "/_HOST@" + SaslConnectionParams.getDefaultRealm();
            }
          }
          return defaults.get(property);
        }
      }

      @Override
      public void getProperties(Map<String,String> props, Predicate<String> filter) {
        defaults.getProperties(props, filter);

        Iterator<String> keyIter = config.getKeys();
        while (keyIter.hasNext()) {
          String key = keyIter.next();
          if (filter.test(key)) {
            props.put(key, config.getString(key));
          }
        }

        // Two client props that don't exist on the server config. Client doesn't need to know about
        // the Kerberos instance from the principle, but servers do
        // Automatically reconstruct the server property when converting a client config.
        if (props.containsKey(
            org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY
                .getKey())) {
          final String serverPrimary = props.remove(
              org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY
                  .getKey());
          if (filter.test(Property.GENERAL_KERBEROS_PRINCIPAL.getKey())) {
            // Use the _HOST expansion. It should be unnecessary in "client land".
            props.put(Property.GENERAL_KERBEROS_PRINCIPAL.getKey(),
                serverPrimary + "/_HOST@" + SaslConnectionParams.getDefaultRealm());
          }
        }

        // Attempt to load sensitive properties from a CredentialProvider, if configured
        org.apache.hadoop.conf.Configuration hadoopConf = getHadoopConfiguration();
        if (hadoopConf != null) {
          for (String key : HadoopCredentialProvider.getKeys(hadoopConf)) {
            if (!Property.isValidPropertyKey(key) || !Property.isSensitive(key)) {
              continue;
            }
            if (filter.test(key)) {
              char[] value = HadoopCredentialProvider.getValue(hadoopConf, key);
              if (value != null) {
                props.put(key, new String(value));
              }
            }
          }
        }
      }

      private org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
        String credProviderPaths =
            config.getString(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey());
        if (credProviderPaths != null && !credProviderPaths.isEmpty()) {
          org.apache.hadoop.conf.Configuration hConf = new org.apache.hadoop.conf.Configuration();
          HadoopCredentialProvider.setPath(hConf, credProviderPaths);
          return hConf;
        }

        log.trace("Did not find credential provider configuration in ClientConfiguration");

        return null;
      }
    };
  }

  @SuppressWarnings("deprecation")
  public static org.apache.accumulo.core.client.ClientConfiguration
      toClientConf(AccumuloConfiguration conf) {
    org.apache.accumulo.core.client.ClientConfiguration clientConf =
        org.apache.accumulo.core.client.ClientConfiguration.create();

    // Servers will only have the full principal in their configuration -- parse the
    // primary and realm from it.
    final String serverPrincipal = conf.get(Property.GENERAL_KERBEROS_PRINCIPAL);

    final KerberosName krbName;
    if (serverPrincipal != null && !serverPrincipal.isEmpty()) {
      krbName = new KerberosName(serverPrincipal);
      clientConf.setProperty(
          org.apache.accumulo.core.client.ClientConfiguration.ClientProperty.KERBEROS_SERVER_PRIMARY,
          krbName.getServiceName());
    }

    HashSet<String> clientKeys = new HashSet<>();
    for (org.apache.accumulo.core.client.ClientConfiguration.ClientProperty prop : org.apache.accumulo.core.client.ClientConfiguration.ClientProperty
        .values()) {
      clientKeys.add(prop.getKey());
    }

    String key;
    for (Map.Entry<String,String> entry : conf) {
      key = entry.getKey();
      if (clientKeys.contains(key)) {
        clientConf.setProperty(key, entry.getValue());
      }
    }
    return clientConf;
  }
}
