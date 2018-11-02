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
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * This class is a singleton.
 * <p>
 * <b>Note</b>: Client code should not use this class, and it may be deprecated in the future.
 */
public class SiteConfiguration extends AccumuloConfiguration {
  private static final Logger log = LoggerFactory.getLogger(SiteConfiguration.class);

  private static final AccumuloConfiguration parent = DefaultConfiguration.getInstance();

  private CompositeConfiguration internalConfig;

  private final Map<String,String> overrides;
  private final Map<String,String> staticConfigs;

  public SiteConfiguration() {
    this(getAccumuloPropsLocation());
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

  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "location of props is specified by an admin")
  public SiteConfiguration(URL accumuloPropsLocation, Map<String,String> overrides) {
    this.overrides = overrides;

    init();
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setDelimiterParsingDisabled(true);
    if (accumuloPropsLocation != null) {
      try {
        config.load(accumuloPropsLocation.openStream());
      } catch (IOException | ConfigurationException e) {
        throw new IllegalArgumentException(e);
      }
    }
    internalConfig.addConfiguration(config);

    Map<String,String> temp = StreamSupport
        .stream(((Iterable<String>) internalConfig::getKeys).spliterator(), false)
        .collect(Collectors.toMap(Function.identity(), internalConfig::getString));

    /*
     * If any of the configs used in hot codepaths are unset here, set a null so that we'll default
     * to the parent config without contending for the Hadoop Configuration object
     */
    for (Property hotConfig : Property.HOT_PATH_PROPERTIES) {
      if (!(temp.containsKey(hotConfig.getKey()))) {
        temp.put(hotConfig.getKey(), null);
      }
    }
    staticConfigs = Collections.unmodifiableMap(temp);
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

  private void init() {
    internalConfig = new CompositeConfiguration();
    internalConfig.setThrowExceptionOnMissing(false);
    internalConfig.setDelimiterParsingDisabled(true);
  }

  private synchronized Configuration getConfiguration() {
    if (internalConfig == null) {
      init();
    }
    return internalConfig;
  }

  @Override
  public String get(Property property) {
    if (overrides.containsKey(property.getKey())) {
      return overrides.get(property.getKey());
    }

    String key = property.getKey();
    // If the property is sensitive, see if CredentialProvider was configured.
    if (property.isSensitive()) {
      String hadoopVal = getSensitiveFromHadoop(property);
      if (hadoopVal != null) {
        return hadoopVal;
      }
    }

    /*
     * Check the available-on-load configs and fall-back to the possibly-update Configuration
     * object.
     */
    String value = staticConfigs.containsKey(key) ? staticConfigs.get(key)
        : getConfiguration().getString(key);

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null)
        log.error("Using default value for {} due to improperly formatted {}: {}", key,
            property.getType(), value);
      value = parent.get(property);
    }

    return value;
  }

  private String getSensitiveFromHadoop(Property property) {
    org.apache.hadoop.conf.Configuration hadoopConf = getHadoopConfiguration();
    if (null != hadoopConf) {
      // Try to find the sensitive value from the CredentialProvider
      try {
        char[] value = CredentialProviderFactoryShim.getValueFromCredentialProvider(hadoopConf,
            property.getKey());
        if (null != value) {
          return new String(value);
        }
      } catch (IOException e) {
        log.warn("Failed to extract sensitive property (" + property.getKey()
            + ") from Hadoop CredentialProvider, falling back to accumulo.properties", e);
      }
    }
    return null;
  }

  @Override
  public boolean isPropertySet(Property prop, boolean cacheAndWatch) {
    if (prop.isSensitive()) {
      String hadoopVal = getSensitiveFromHadoop(prop);
      if (hadoopVal != null) {
        return true;
      }
    }
    return overrides.containsKey(prop.getKey()) || staticConfigs.containsKey(prop.getKey())
        || getConfiguration().containsKey(prop.getKey()) || parent.isPropertySet(prop, cacheAndWatch);
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

    StreamSupport.stream(((Iterable<String>) getConfiguration()::getKeys).spliterator(), false)
        .filter(filter).forEach(k -> props.put(k, getConfiguration().getString(k)));

    // CredentialProvider should take precedence over site
    org.apache.hadoop.conf.Configuration hadoopConf = getHadoopConfiguration();
    if (null != hadoopConf) {
      try {
        for (String key : CredentialProviderFactoryShim.getKeys(hadoopConf)) {
          if (!Property.isValidPropertyKey(key) || !Property.isSensitive(key)) {
            continue;
          }

          if (filter.test(key)) {
            char[] value = CredentialProviderFactoryShim.getValueFromCredentialProvider(hadoopConf,
                key);
            if (null != value) {
              props.put(key, new String(value));
            }
          }
        }
      } catch (IOException e) {
        log.warn("Failed to extract sensitive properties from Hadoop"
            + " CredentialProvider, falling back to accumulo.properties", e);
      }
    }
    if (overrides != null) {
      for (Map.Entry<String,String> entry : overrides.entrySet()) {
        if (filter.test(entry.getKey())) {
          props.put(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  protected org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
    String credProviderPathsKey = Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey();
    String credProviderPathsValue = getConfiguration().getString(credProviderPathsKey);

    if (null != credProviderPathsValue) {
      // We have configuration for a CredentialProvider
      // Try to pull the sensitive password from there
      org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(
          CachedConfiguration.getInstance());
      conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, credProviderPathsValue);
      return conf;
    }

    return null;
  }

  /**
   * Sets a property. This method supports testing and should not be called.
   *
   * @param property
   *          property to set
   * @param value
   *          property value
   */
  public void set(Property property, String value) {
    set(property.getKey(), value);
  }

  /**
   * Sets a property. This method supports testing and should not be called.
   *
   * @param key
   *          key of property to set
   * @param value
   *          property value
   */
  public void set(String key, String value) {
    getConfiguration().setProperty(key, value);
  }
}
