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

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

  public static Properties toProperties(AccumuloConfiguration config) {
    Properties props = new Properties();

    // Servers will only have the full principal in their configuration
    // parse the primary and realm from it.
    final String serverPrincipal = config.get(Property.GENERAL_KERBEROS_PRINCIPAL);
    if (serverPrincipal != null && !serverPrincipal.isEmpty()) {
      var krbName = new KerberosName(serverPrincipal);
      props.setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey(),
          krbName.getServiceName());
    }

    // copy any client properties from the config into the props
    Set<String> clientKeys = Arrays.stream(ClientProperty.values()).map(ClientProperty::getKey)
        .collect(Collectors.toSet());
    config.stream().filter(e -> clientKeys.contains(e.getKey()))
        .forEach(e -> props.setProperty(e.getKey(), e.getValue()));

    return props;
  }

  public static AccumuloConfiguration toAccumuloConf(Properties properties) {
    final var propertiesCopy = new Properties(properties);
    for (String propKey : propertiesCopy.stringPropertyNames()) {
      String val = propertiesCopy.getProperty(propKey);
      propertiesCopy.setProperty(propKey, val);
      if (propKey.equals(ClientProperty.SSL_KEYSTORE_PATH.getKey())) {
        propertiesCopy.setProperty(Property.INSTANCE_RPC_SSL_CLIENT_AUTH.getKey(), "true");
      }
    }

    final AccumuloConfiguration defaults = DefaultConfiguration.getInstance();

    return new AccumuloConfiguration() {

      @Override
      public boolean isPropertySet(Property prop) {
        return propertiesCopy.containsKey(prop.getKey());
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

        if (propertiesCopy.containsKey(key)) {
          return propertiesCopy.getProperty(key);
        } else {
          // Reconstitute the server kerberos property from the client config
          if (property == Property.GENERAL_KERBEROS_PRINCIPAL) {
            if (propertiesCopy.containsKey(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey())) {
              // Avoid providing a realm since we don't know what it is...
              return propertiesCopy
                  .getProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey()) + "/_HOST@"
                  + SaslConnectionParams.getDefaultRealm();
            }
          }
          return defaults.get(property);
        }
      }

      @Override
      public void getProperties(Map<String,String> props, Predicate<String> filter) {
        defaults.getProperties(props, filter);

        for (String key : propertiesCopy.stringPropertyNames()) {
          if (filter.test(key)) {
            props.put(key, propertiesCopy.getProperty(key));
          }
        }

        // Two client props that don't exist on the server config. Client doesn't need to know about
        // the Kerberos instance from the principle, but servers do
        // Automatically reconstruct the server property when converting a client config.
        if (props.containsKey(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey())) {
          final String serverPrimary =
              props.remove(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey());
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
        String credProviderPaths = propertiesCopy
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
