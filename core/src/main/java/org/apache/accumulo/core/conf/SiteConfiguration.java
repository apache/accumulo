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

/**
 * An {@link AccumuloConfiguration} which first loads any properties set on the command-line (using the -o option) and then from an XML file, usually
 * accumulo-site.xml. This implementation supports defaulting undefined property values to a parent configuration's definitions.
 * <p>
 * The system property "accumulo.configuration" can be used to specify the location of the XML configuration file on the classpath or filesystem if the path is
 * prefixed with 'file://'. If the system property is not defined, it defaults to "accumulo-site.xml" and will look on classpath for file.
 * <p>
 * This class is a singleton.
 * <p>
 * <b>Note</b>: Client code should not use this class, and it may be deprecated in the future.
 */
public class SiteConfiguration extends AccumuloConfiguration {
  private static final Logger log = LoggerFactory.getLogger(SiteConfiguration.class);

  private static final AccumuloConfiguration parent = DefaultConfiguration.getInstance();
  private static SiteConfiguration instance = null;

  private static Configuration xmlConfig;
  private final Map<String,String> staticConfigs;

  private SiteConfiguration() {
    /*
     * Make a read-only copy of static configs so we can avoid lock contention on the Hadoop Configuration object
     */
    final Configuration conf = getXmlConfig();
    Map<String,String> temp = new HashMap<>((int) (Math.ceil(conf.size() / 0.75f)), 0.75f);
    for (Entry<String,String> entry : conf) {
      temp.put(entry.getKey(), entry.getValue());
    }
    /*
     * If any of the configs used in hot codepaths are unset here, set a null so that we'll default to the parent config without contending for the Hadoop
     * Configuration object
     */
    for (Property hotConfig : Property.HOT_PATH_PROPERTIES) {
      if (!(temp.containsKey(hotConfig.getKey()))) {
        temp.put(hotConfig.getKey(), null);
      }
    }
    staticConfigs = Collections.unmodifiableMap(temp);
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

  synchronized private static Configuration getXmlConfig() {
    if (xmlConfig == null) {
      xmlConfig = new Configuration(false);
      String configFile = System.getProperty("accumulo.configuration", "accumulo-site.xml");
      if (configFile.startsWith("file://")) {
        try {
          File f = new File(new URI(configFile));
          if (f.exists() && !f.isDirectory()) {
            xmlConfig.addResource(f.toURI().toURL());
            log.info("Loaded configuration from filesystem at {}", configFile);
          } else {
            log.warn("Failed to load Accumulo configuration from " + configFile, new Throwable());
          }
        } catch (MalformedURLException | URISyntaxException e) {
          log.warn("Failed to load Accumulo configuration from " + configFile, e);
        }
      } else {
        URL accumuloConfigUrl = SiteConfiguration.class.getClassLoader().getResource(configFile);
        if (accumuloConfigUrl == null) {
          log.warn("Accumulo configuration '" + configFile + "' is not on classpath", new Throwable());
        } else {
          xmlConfig.addResource(accumuloConfigUrl);
          log.info("Loaded configuration from classpath at {}", accumuloConfigUrl.getFile());
        }
      }
    }
    return xmlConfig;
  }

  @Override
  public String get(Property property) {
    if (CliConfiguration.get(property) != null) {
      return CliConfiguration.get(property);
    }

    String key = property.getKey();
    // If the property is sensitive, see if CredentialProvider was configured.
    if (property.isSensitive()) {
      Configuration hadoopConf = getHadoopConfiguration();
      if (null != hadoopConf) {
        // Try to find the sensitive value from the CredentialProvider
        try {
          char[] value = CredentialProviderFactoryShim.getValueFromCredentialProvider(hadoopConf, key);
          if (null != value) {
            return new String(value);
          }
        } catch (IOException e) {
          log.warn("Failed to extract sensitive property (" + key + ") from Hadoop CredentialProvider, falling back to accumulo-site.xml", e);
        }
      }
    }

    /* Check the available-on-load configs and fall-back to the possibly-update Configuration object. */
    String value = staticConfigs.containsKey(key) ? staticConfigs.get(key) : getXmlConfig().get(key);

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null)
        log.error("Using default value for {} due to improperly formatted {}: {}", key, property.getType(), value);
      value = parent.get(property);
    }

    return value;
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    getProperties(props, filter, true);
  }

  public void getProperties(Map<String,String> props, Predicate<String> filter, boolean useDefaults) {
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
    CliConfiguration.getProperties(props, filter);
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
   * Clears the configuration properties in this configuration (but not the parent). This method supports testing and should not be called.
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
