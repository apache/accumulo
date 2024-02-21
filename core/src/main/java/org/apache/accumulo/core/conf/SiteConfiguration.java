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
package org.apache.accumulo.core.conf;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
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
 * <b>Note</b>: Client code should not use this class, and it may be deprecated in the future.
 */
public class SiteConfiguration extends AccumuloConfiguration {

  private static final Logger log = LoggerFactory.getLogger(SiteConfiguration.class);

  private static final AccumuloConfiguration parent = DefaultConfiguration.getInstance();

  public interface Buildable {
    SiteConfiguration build();
  }

  public interface OverridesOption extends Buildable {
    Buildable withOverrides(Map<String,String> overrides);
  }

  static class Builder implements OverridesOption, Buildable {
    private URL url = null;
    private Map<String,String> overrides = Collections.emptyMap();

    // visible to package-private for testing only
    Builder() {}

    private OverridesOption noFile() {
      return this;
    }

    // exists for testing only
    OverridesOption fromUrl(URL propertiesFileUrl) {
      url = requireNonNull(propertiesFileUrl);
      return this;
    }

    public OverridesOption fromEnv() {
      URL siteUrl = SiteConfiguration.class.getClassLoader().getResource("accumulo-site.xml");
      if (siteUrl != null) {
        throw new IllegalArgumentException("Found deprecated config file 'accumulo-site.xml' on "
            + "classpath. Since 2.0.0, this file was replaced by 'accumulo.properties'. Run the "
            + "following command to convert an old 'accumulo-site.xml' file to the new format: "
            + "accumulo convert-config -x /old/accumulo-site.xml -p /new/accumulo.properties");
      }

      String configFile = System.getProperty("accumulo.properties", "accumulo.properties");
      if (configFile.startsWith("file://")) {
        File f;
        try {
          f = new File(new URI(configFile));
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(
              "Failed to load Accumulo configuration from " + configFile, e);
        }
        if (f.exists() && !f.isDirectory()) {
          log.info("Found Accumulo configuration at {}", configFile);
          return fromFile(f);
        } else {
          throw new IllegalArgumentException(
              "Failed to load Accumulo configuration at " + configFile);
        }
      } else {
        URL accumuloConfigUrl = SiteConfiguration.class.getClassLoader().getResource(configFile);
        if (accumuloConfigUrl == null) {
          throw new IllegalArgumentException(
              "Failed to load Accumulo configuration '" + configFile + "' from classpath");
        } else {
          log.info("Found Accumulo configuration on classpath at {}", accumuloConfigUrl.getFile());
          url = accumuloConfigUrl;
          return this;
        }
      }
    }

    public OverridesOption fromFile(File propertiesFileLocation) {
      try {
        url = requireNonNull(propertiesFileLocation).toURI().toURL();
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e);
      }
      return this;
    }

    @Override
    public Buildable withOverrides(Map<String,String> overrides) {
      this.overrides = requireNonNull(overrides);
      return this;
    }

    @Override
    public SiteConfiguration build() {
      // load properties from configuration file
      var propsFileConfig = getPropsFileConfig(url);

      // load properties from command-line overrides
      var overrideConfig = new MapConfiguration(overrides);

      // load credential provider property
      var credProviderProps = new HashMap<String,String>();
      for (var c : new AbstractConfiguration[] {propsFileConfig, overrideConfig}) {
        var credProvider =
            c.getString(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey());
        if (credProvider != null && !credProvider.isEmpty()) {
          loadCredProviderProps(credProvider, credProviderProps);
          break;
        }
      }
      var credProviderConfig = new MapConfiguration(credProviderProps);

      var config = new CompositeConfiguration();
      // add in specific order; use credential provider first, then overrides, then properties file
      config.addConfiguration(credProviderConfig);
      config.addConfiguration(overrideConfig);
      config.addConfiguration(propsFileConfig);

      // Make sure any deprecated property names aren't using both the old and new name.
      DeprecatedPropertyUtil.sanityCheckManagerProperties(config);

      var result = new HashMap<String,String>();
      config.getKeys().forEachRemaining(orig -> {
        String resolved = DeprecatedPropertyUtil.getReplacementName(orig, (log, replacement) -> {
          log.warn("{} has been deprecated and will be removed in a future release;"
              + " loading its replacement {} instead.", orig, replacement);
        });
        result.put(resolved, config.getString(orig));
      });
      return new SiteConfiguration(Collections.unmodifiableMap(result));
    }
  }

  /**
   * Build a SiteConfiguration from the environmental configuration with the option to override.
   */
  public static SiteConfiguration.OverridesOption fromEnv() {
    return new SiteConfiguration.Builder().fromEnv();
  }

  /**
   * Build a SiteConfiguration from the provided properties file with the option to override.
   */
  public static SiteConfiguration.OverridesOption fromFile(File propertiesFileLocation) {
    return new SiteConfiguration.Builder().fromFile(propertiesFileLocation);
  }

  /**
   * Build a SiteConfiguration that is initially empty with the option to override.
   */
  public static SiteConfiguration.OverridesOption empty() {
    return new SiteConfiguration.Builder().noFile();
  }

  /**
   * Build a SiteConfiguration from the environmental configuration and no overrides.
   */
  public static SiteConfiguration auto() {
    return new SiteConfiguration.Builder().fromEnv().build();
  }

  private final Map<String,String> config;

  private SiteConfiguration(Map<String,String> config) {
    ConfigCheckUtil.validate(config.entrySet(), "site config");
    this.config = config;
  }

  // load properties from config file
  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "url is specified by an admin, not unchecked user input")
  private static AbstractConfiguration getPropsFileConfig(URL accumuloPropsLocation) {
    var config = new PropertiesConfiguration();
    if (accumuloPropsLocation != null) {
      try (var reader = new InputStreamReader(accumuloPropsLocation.openStream(), UTF_8)) {
        config.read(reader);
      } catch (ConfigurationException | IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
    return config;
  }

  // load sensitive properties from Hadoop credential provider
  private static void loadCredProviderProps(String provider, Map<String,String> props) {
    var hadoopConf = new org.apache.hadoop.conf.Configuration();
    HadoopCredentialProvider.setPath(hadoopConf, provider);
    Stream.of(Property.values()).filter(Property::isSensitive).forEach(p -> {
      char[] value = HadoopCredentialProvider.getValue(hadoopConf, p.getKey());
      if (value != null) {
        props.put(p.getKey(), new String(value));
      }
    });
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
  public boolean isPropertySet(Property prop) {
    return config.containsKey(prop.getKey()) || parent.isPropertySet(prop);
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
      if (filter.test(k)) {
        props.put(k, config.get(k));
      }
    });
  }

  @Override
  public AccumuloConfiguration getParent() {
    return parent;
  }
}
