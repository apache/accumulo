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

import java.util.HashMap;
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
  private static final Map<String,String> accumuloConfToClientProps = new HashMap<>();
  private static final Map<String,String> clientPropsToAccumuloConf = new HashMap<>();

  static {
    // mapping of ClientProperty equivalents in AccumuloConfiguration
    Map<ClientProperty,Property> conversions = new HashMap<>();
    conversions.put(ClientProperty.INSTANCE_ZOOKEEPERS, Property.INSTANCE_ZK_HOST);
    conversions.put(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, Property.INSTANCE_ZK_TIMEOUT);

    conversions.put(ClientProperty.SASL_ENABLED, Property.INSTANCE_RPC_SASL_ENABLED);
    conversions.put(ClientProperty.SASL_QOP, Property.RPC_SASL_QOP);

    conversions.put(ClientProperty.SSL_ENABLED, Property.INSTANCE_RPC_SSL_ENABLED);
    conversions.put(ClientProperty.SSL_KEYSTORE_PASSWORD, Property.RPC_SSL_KEYSTORE_PASSWORD);
    conversions.put(ClientProperty.SSL_KEYSTORE_PATH, Property.RPC_SSL_KEYSTORE_PATH);
    conversions.put(ClientProperty.SSL_KEYSTORE_TYPE, Property.RPC_SSL_KEYSTORE_TYPE);
    conversions.put(ClientProperty.SSL_TRUSTSTORE_PASSWORD, Property.RPC_SSL_TRUSTSTORE_PASSWORD);
    conversions.put(ClientProperty.SSL_TRUSTSTORE_PATH, Property.RPC_SSL_TRUSTSTORE_PATH);
    conversions.put(ClientProperty.SSL_TRUSTSTORE_TYPE, Property.RPC_SSL_TRUSTSTORE_TYPE);
    conversions.put(ClientProperty.SSL_USE_JSSE, Property.RPC_USE_JSSE);

    for (Map.Entry<ClientProperty,Property> entry : conversions.entrySet()) {
      accumuloConfToClientProps.put(entry.getValue().getKey(), entry.getKey().getKey());
      clientPropsToAccumuloConf.put(entry.getKey().getKey(), entry.getValue().getKey());
    }
  }

  public static Properties toProperties(AccumuloConfiguration config) {
    final var propsExtractedFromConfig = new Properties();

    // Extract kerberos primary from the config
    final String serverPrincipal = config.get(Property.GENERAL_KERBEROS_PRINCIPAL);
    if (serverPrincipal != null && !serverPrincipal.isEmpty()) {
      var krbName = new KerberosName(serverPrincipal);
      propsExtractedFromConfig.setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey(),
          krbName.getServiceName());
    }

    // Extract the remaining properties from the config
    config.stream().filter(e -> accumuloConfToClientProps.keySet().contains(e.getKey()))
        .forEach(e -> propsExtractedFromConfig.setProperty(e.getKey(), e.getValue()));

    // For all the extracted properties, convert them to their ClientProperty names
    final var convertedProps = new Properties();
    propsExtractedFromConfig.forEach((k, v) -> {
      String confKey = String.valueOf(k);
      String val = String.valueOf(v);
      String propKey = accumuloConfToClientProps.get(confKey);
      convertedProps.setProperty(propKey == null ? confKey : propKey, val);
    });
    return convertedProps;
  }

  public static AccumuloConfiguration toAccumuloConf(Properties properties) {
    final var convertedProps = new Properties();
    for (String propKey : properties.stringPropertyNames()) {
      String val = properties.getProperty(propKey);
      String confKey = clientPropsToAccumuloConf.get(propKey);
      if (propKey.equals(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey())) {
        confKey = Property.GENERAL_KERBEROS_PRINCIPAL.getKey();
        // Avoid providing a realm since we don't know what it is...
        val += "/_HOST@" + SaslConnectionParams.getDefaultRealm();
      }
      convertedProps.setProperty(confKey == null ? propKey : confKey, val);
      if (propKey.equals(ClientProperty.SSL_KEYSTORE_PATH.getKey())) {
        convertedProps.setProperty(Property.INSTANCE_RPC_SSL_CLIENT_AUTH.getKey(), "true");
      }
    }

    final AccumuloConfiguration defaults = DefaultConfiguration.getInstance();

    return new AccumuloConfiguration() {

      @Override
      public boolean isPropertySet(Property prop) {
        return convertedProps.containsKey(prop.getKey());
      }

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
        return convertedProps.getProperty(key, defaults.get(property));
      }

      @Override
      public void getProperties(Map<String,String> props, Predicate<String> filter) {
        defaults.getProperties(props, filter);
        for (String key : convertedProps.stringPropertyNames()) {
          if (filter.test(key)) {
            props.put(key, convertedProps.getProperty(key));
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
        String credProviderPaths = convertedProps
            .getProperty(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey());
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
}
