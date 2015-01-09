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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.CredentialProviderFactoryShim;
import org.apache.accumulo.core.conf.Property;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All client side code that needs a server side configuration object should obtain it from here.
 */
public class ServerConfigurationUtil {
  private static final Logger log = LoggerFactory.getLogger(ServerConfigurationUtil.class);

  @SuppressWarnings("deprecation")
  public static AccumuloConfiguration getConfiguration(Instance instance) {
    return instance.getConfiguration();
  }

  public static AccumuloConfiguration convertClientConfig(final AccumuloConfiguration base, final Configuration config) {

    return new AccumuloConfiguration() {
      @Override
      public String get(Property property) {
        final String key = property.getKey();

        // Attempt to load sensitive properties from a CredentialProvider, if configured
        if (property.isSensitive()) {
          org.apache.hadoop.conf.Configuration hadoopConf = getHadoopConfiguration();
          if (null != hadoopConf) {
            try {
              char[] value = CredentialProviderFactoryShim.getValueFromCredentialProvider(hadoopConf, key);
              if (null != value) {
                log.trace("Loaded sensitive value for {} from CredentialProvider", key);
                return new String(value);
              } else {
                log.trace("Tried to load sensitive value for {} from CredentialProvider, but none was found", key);
              }
            } catch (IOException e) {
              log.warn("Failed to extract sensitive property ({}) from Hadoop CredentialProvider, falling back to base AccumuloConfiguration", key, e);
            }
          }
        }
        if (config.containsKey(key))
          return config.getString(key);
        else
          return base.get(property);
      }

      @Override
      public void getProperties(Map<String,String> props, PropertyFilter filter) {
        base.getProperties(props, filter);

        @SuppressWarnings("unchecked")
        Iterator<String> keyIter = config.getKeys();
        while (keyIter.hasNext()) {
          String key = keyIter.next();
          if (filter.accept(key))
            props.put(key, config.getString(key));
        }

        // Attempt to load sensitive properties from a CredentialProvider, if configured
        org.apache.hadoop.conf.Configuration hadoopConf = getHadoopConfiguration();
        if (null != hadoopConf) {
          try {
            for (String key : CredentialProviderFactoryShim.getKeys(hadoopConf)) {
              if (!Property.isValidPropertyKey(key) || !Property.isSensitive(key)) {
                continue;
              }

              if (filter.accept(key)) {
                char[] value = CredentialProviderFactoryShim.getValueFromCredentialProvider(hadoopConf, key);
                if (null != value) {
                  props.put(key, new String(value));
                }
              }
            }
          } catch (IOException e) {
            log.warn("Failed to extract sensitive properties from Hadoop CredentialProvider, falling back to accumulo-site.xml", e);
          }
        }
      }

      private org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
        String credProviderPaths = config.getString(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey());
        if (null != credProviderPaths && !credProviderPaths.isEmpty()) {
          org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
          hadoopConf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, credProviderPaths);
          return hadoopConf;
        }

        log.trace("Did not find credential provider configuration in ClientConfiguration");

        return null;
      }
    };

  }
}
