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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;

import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * An {@link AccumuloConfiguration} which first loads any properties set on the command-line (using
 * the -o option) and then from an XML file, usually accumulo-site.xml. This implementation supports
 * defaulting undefined property values to a parent configuration's definitions.
 * <p>
 * The system property "accumulo.configuration" can be used to specify the location of the XML
 * configuration file on the classpath or filesystem if the path is prefixed with 'file://'. If the
 * system property is not defined, it defaults to "accumulo-site.xml" and will look on classpath for
 * file.
 * <p>
 * This class is a singleton.
 * <p>
 * <b>Note</b>: Client code should not use this class, and it may be deprecated in the future.
 */
public class SiteConfiguration extends AccumuloConfiguration {
  private static final Logger log = LoggerFactory.getLogger(SiteConfiguration.class);

  private static final AccumuloConfiguration parent = DefaultConfiguration.getInstance();
  private static SiteConfiguration instance = null;

  private final Map<String,String> overrides;
  private static Configuration xmlConfig;
  private final Map<String,String> staticConfigs;

  private SiteConfiguration() {
    this(getAccumuloSiteLocation(), Collections.emptyMap());
  }

  private SiteConfiguration(URL accumuloSiteLocation, Map<String,String> overrides) {
    this.overrides = overrides;
    /*
     * Make a read-only copy of static configs so we can avoid lock contention on the Hadoop
     * Configuration object
     */
    xmlConfig = new Configuration(false);
    if (accumuloSiteLocation != null) {
      xmlConfig.addResource(accumuloSiteLocation);
    }

    final Configuration conf = xmlConfig;
    Map<String,String> temp = new HashMap<>((int) (Math.ceil(conf.size() / 0.75f)), 0.75f);
    for (Entry<String,String> entry : conf) {
      temp.put(entry.getKey(), entry.getValue());
    }
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

  synchronized public static SiteConfiguration create(URL accumuloSiteUrl,
      Map<String,String> overrides) {
    if (instance != null) {
      throw new IllegalStateException(
          "SiteConfiguration.create() has been called after SiteConfiguration was already created.");
    }
    instance = new SiteConfiguration(accumuloSiteUrl, overrides);
    ConfigSanityCheck.validate(instance);
    return instance;
  }

  public static SiteConfiguration create(URL accumuloSiteUrl) {
    return create(accumuloSiteUrl, Collections.emptyMap());
  }

  public static SiteConfiguration create(File accumuloSiteFile, Map<String,String> overrides) {
    return create(toURL(accumuloSiteFile), overrides);
  }

  public static SiteConfiguration create(File accumuloSiteFile) {
    return create(toURL(accumuloSiteFile), Collections.emptyMap());
  }

  /**
   * Gets an instance of this class. A new instance is only created on the first call.
   *
   * @throws RuntimeException
   *           if the configuration is invalid
   */
  synchronized public static SiteConfiguration getInstance() {
    if (instance == null) {
      instance = new SiteConfiguration();
      ConfigSanityCheck.validate(instance);
    }
    return instance;
  }

  /**
   * Creates SiteConfiguration even if accumulo-site.xml is not provided on classpath
   */
  @VisibleForTesting
  synchronized public static SiteConfiguration getTestInstance() {
    if (instance == null) {
      URL accumuloSiteUrl = null;
      try {
        accumuloSiteUrl = getAccumuloSiteLocation();
      } catch (IllegalArgumentException e) {
        // ignore this exception during testing
      }
      instance = new SiteConfiguration(accumuloSiteUrl, Collections.emptyMap());
      ConfigSanityCheck.validate(instance);
    }
    return instance;
  }

  public static URL getAccumuloSiteLocation() {
    String configFile = System.getProperty("accumulo.configuration", "accumulo-site.xml");
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

  private Configuration getXmlConfig() {
    if (xmlConfig == null) {
      return new Configuration(false);
    }
    return xmlConfig;
  }

  @Override
  public String get(Property property) {
    if (overrides.containsKey(property.getKey())) {
      return overrides.get(property.getKey());
    }

    String key = property.getKey();
    // If the property is sensitive, see if CredentialProvider was configured.
    if (property.isSensitive()) {
      Configuration hadoopConf = getHadoopConfiguration();
      if (null != hadoopConf) {
        // Try to find the sensitive value from the CredentialProvider
        try {
          char[] value = CredentialProviderFactoryShim.getValueFromCredentialProvider(hadoopConf,
              key);
          if (null != value) {
            return new String(value);
          }
        } catch (IOException e) {
          log.warn("Failed to extract sensitive property (" + key
              + ") from Hadoop CredentialProvider, falling back to accumulo-site.xml", e);
        }
      }
    }

    /*
     * Check the available-on-load configs and fall-back to the possibly-update Configuration
     * object.
     */
    String value = staticConfigs.containsKey(key) ? staticConfigs.get(key)
        : getXmlConfig().get(key);

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null)
        log.error("Using default value for {} due to improperly formatted {}: {}", key,
            property.getType(), value);
      value = parent.get(property);
    }

    return value;
  }

  @Override
  public boolean isPropertySet(Property prop) {
    Preconditions.checkArgument(!prop.isSensitive(),
        "This method not implemented for sensitive props");
    return overrides.containsKey(prop.getKey()) || staticConfigs.containsKey(prop.getKey())
        || getXmlConfig().get(prop.getKey()) != null || parent.isPropertySet(prop);
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

    for (Entry<String,String> entry : getXmlConfig())
      if (filter.test(entry.getKey()))
        props.put(entry.getKey(), entry.getValue());

    // CredentialProvider should take precedence over site
    Configuration hadoopConf = getHadoopConfiguration();
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
            + " CredentialProvider, falling back to accumulo-site.xml", e);
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

  protected Configuration getHadoopConfiguration() {
    String credProviderPathsKey = Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey();
    String credProviderPathsValue = getXmlConfig().get(credProviderPathsKey);

    if (null != credProviderPathsValue) {
      // We have configuration for a CredentialProvider
      // Try to pull the sensitive password from there
      Configuration conf = new Configuration(CachedConfiguration.getInstance());
      conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, credProviderPathsValue);
      return conf;
    }

    return null;
  }

  /**
   * Clears the configuration properties in this configuration (but not the parent). This method
   * supports testing and should not be called.
   */
  synchronized public static void clearInstance() {
    instance = null;
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
    getXmlConfig().set(key, value);
  }
}
