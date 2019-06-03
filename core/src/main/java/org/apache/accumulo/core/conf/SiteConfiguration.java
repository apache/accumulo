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
package org.apache.accumulo.core.conf;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * An {@link AccumuloConfiguration} which first loads any properties set on the command-line (using
 * the -o option) and then from accumulo.properties. This implementation supports defaulting
 * undefined property values to a parent configuration's definitions.
 * <p>
 * The system property "accumulo.properties" can be used to specify the location of the properties
 * file on the classpath or filesystem if the path is prefixed with 'file://'. If the system
 * property is not defined, it defaults to "accumulo.properties" and will look on classpath for
 * file.
 * <p>
 * <b>Note</b>: Client code should not use this class, and it may be deprecated in the future.
 */
public class SiteConfiguration extends AccumuloConfiguration {

  private static final Logger log = LoggerFactory.getLogger(SiteConfiguration.class);

  private static final AccumuloConfiguration parent = DefaultConfiguration.getInstance();

  private final ImmutableMap<String,String> config;

  public SiteConfiguration() {
    this(getAccumuloPropsLocation());
  }

  public SiteConfiguration(Map<String,String> overrides) {
    this(getAccumuloPropsLocation(), overrides);
  }

  public SiteConfiguration(File accumuloPropsFile) {
    this(accumuloPropsFile, Collections.emptyMap());
  }

  public SiteConfiguration(File accumuloPropsFile, Map<String,String> overrides) {
    this(toURL(accumuloPropsFile), overrides);
  }

  public SiteConfiguration(URL accumuloPropsLocation) {
    this(accumuloPropsLocation, Collections.emptyMap());
  }

  public SiteConfiguration(URL accumuloPropsLocation, Map<String,String> overrides) {
    config = createMap(accumuloPropsLocation, overrides);
    ConfigSanityCheck.validate(config.entrySet());
  }

  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "location of props is specified by an admin")
  private static ImmutableMap<String,String> createMap(URL accumuloPropsLocation,
      Map<String,String> overrides) {
    CompositeConfiguration config = new CompositeConfiguration();
    if (accumuloPropsLocation != null) {
      FileBasedConfigurationBuilder<PropertiesConfiguration> propsBuilder =
          new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
              .configure(new Parameters().properties().setURL(accumuloPropsLocation));
      try {
        config.addConfiguration(propsBuilder.getConfiguration());
      } catch (ConfigurationException e) {
        throw new IllegalArgumentException(e);
      }
    }

    // Add all properties in config file
    Map<String,String> result = new HashMap<>();
    config.getKeys().forEachRemaining(key -> result.put(key, config.getString(key)));

    // Add all overrides
    overrides.forEach(result::put);

    // Add sensitive properties from credential provider (if set)
    String credProvider = result.get(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey());
    if (credProvider != null) {
      org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
      hadoopConf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, credProvider);
      for (Property property : Property.values()) {
        if (property.isSensitive()) {
          char[] value = CredentialProviderFactoryShim.getValueFromCredentialProvider(hadoopConf,
              property.getKey());
          if (value != null) {
            result.put(property.getKey(), new String(value));
          }
        }
      }
    }
    return ImmutableMap.copyOf(result);
  }

  private static URL toURL(File f) {
    try {
      return f.toURI().toURL();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static URL getAccumuloPropsLocation() {

    URL siteUrl = SiteConfiguration.class.getClassLoader().getResource("accumulo-site.xml");
    if (siteUrl != null) {
      throw new IllegalArgumentException("Found deprecated config file 'accumulo-site.xml' on "
          + "classpath. Since 2.0.0, this file was replaced by 'accumulo.properties'. Run the "
          + "following command to convert an old 'accumulo-site.xml' file to the new format: "
          + "accumulo convert-config -x /old/accumulo-site.xml -p /new/accumulo.properties");
    }

    String configFile = System.getProperty("accumulo.properties", "accumulo.properties");
    if (configFile.startsWith("file://")) {
      try {
        File f = new File(new URI(configFile));
        if (f.exists() && !f.isDirectory()) {
          log.info("Found Accumulo configuration at {}", configFile);
          return f.toURI().toURL();
        } else {
          throw new IllegalArgumentException(
              "Failed to load Accumulo configuration at " + configFile);
        }
      } catch (MalformedURLException | URISyntaxException e) {
        throw new IllegalArgumentException(
            "Failed to load Accumulo configuration from " + configFile, e);
      }
    } else {
      URL accumuloConfigUrl = SiteConfiguration.class.getClassLoader().getResource(configFile);
      if (accumuloConfigUrl == null) {
        throw new IllegalArgumentException(
            "Failed to load Accumulo configuration '" + configFile + "' from classpath");
      } else {
        log.info("Found Accumulo configuration on classpath at {}", accumuloConfigUrl.getFile());
        return accumuloConfigUrl;
      }
    }
  }

  @Override
  public String get(Property property) {
    String value = config.get(property.getKey());
    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null) {
        log.error("Using default value for {} due to improperly formatted {}: {}",
            property.getKey(), property.getType(), value);
      }
      value = parent.get(property);
    }
    return value;
  }

  @Override
  public boolean isPropertySet(Property prop, boolean cacheAndWatch) {
    return config.containsKey(prop.getKey()) || parent.isPropertySet(prop, cacheAndWatch);
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    getProperties(props, filter, true);
  }

  public void getProperties(Map<String,String> props, Predicate<String> filter,
      boolean useDefaults) {
    if (useDefaults) {
      parent.getProperties(props, filter);
    }
    config.keySet().forEach(k -> {
      if (filter.test(k))
        props.put(k, config.get(k));
    });
  }
}
