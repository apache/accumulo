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
package org.apache.accumulo.core.client;

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.PropertyType;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Contains a list of property keys recognized by the Accumulo client and convenience methods for setting them.
 *
 * @since 1.6.0
 */
public class ClientConfiguration extends CompositeConfiguration {
  public static final String USER_ACCUMULO_DIR_NAME = ".accumulo";
  public static final String USER_CONF_FILENAME = "config";
  public static final String GLOBAL_CONF_FILENAME = "client.conf";

  public enum ClientProperty {
    RPC_SSL_TRUSTSTORE_PATH(Property.RPC_SSL_TRUSTSTORE_PATH),
    RPC_SSL_TRUSTSTORE_PASSWORD(Property.RPC_SSL_TRUSTSTORE_PASSWORD),
    RPC_SSL_TRUSTSTORE_TYPE(Property.RPC_SSL_TRUSTSTORE_TYPE),
    RPC_SSL_KEYSTORE_PATH(Property.RPC_SSL_KEYSTORE_PATH),
    RPC_SSL_KEYSTORE_PASSWORD(Property.RPC_SSL_KEYSTORE_PASSWORD),
    RPC_SSL_KEYSTORE_TYPE(Property.RPC_SSL_KEYSTORE_TYPE),
    RPC_USE_JSSE(Property.RPC_USE_JSSE),
    GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS),
    INSTANCE_RPC_SSL_CLIENT_AUTH(Property.INSTANCE_RPC_SSL_CLIENT_AUTH),
    INSTANCE_RPC_SSL_ENABLED(Property.INSTANCE_RPC_SSL_ENABLED),
    INSTANCE_ZK_HOST(Property.INSTANCE_ZK_HOST),
    INSTANCE_ZK_TIMEOUT(Property.INSTANCE_ZK_TIMEOUT),
    INSTANCE_NAME("instance.name", null, PropertyType.STRING, "Name of Accumulo instance to connect to"),
    INSTANCE_ID("instance.id", null, PropertyType.STRING, "UUID of Accumulo instance to connect to"), ;

    private String key;
    private String defaultValue;
    private PropertyType type;
    private String description;

    private Property accumuloProperty = null;

    private ClientProperty(Property prop) {
      this(prop.getKey(), prop.getDefaultValue(), prop.getType(), prop.getDescription());
      accumuloProperty = prop;
    }

    private ClientProperty(String key, String defaultValue, PropertyType type, String description) {
      this.key = key;
      this.defaultValue = defaultValue;
      this.type = type;
      this.description = description;
    }

    public String getKey() {
      return key;
    }

    public String getDefaultValue() {
      return defaultValue;
    }

    public PropertyType getType() {
      return type;
    }

    public String getDescription() {
      return description;
    }

    public Property getAccumuloProperty() {
      return accumuloProperty;
    }

    public static ClientProperty getPropertyByKey(String key) {
      for (ClientProperty prop : ClientProperty.values())
        if (prop.getKey().equals(key))
          return prop;
      return null;
    }
  };

  public ClientConfiguration(List<? extends Configuration> configs) {
    super(configs);
    // Don't do list interpolation
    this.setListDelimiter('\0');
  }

  /**
   * Iterates through the Configuration objects, populating this object.
   *
   * @see PropertiesConfiguration
   * @see #loadDefault()
   */

  public ClientConfiguration(Configuration... configs) {
    this(Arrays.asList(configs));
  }

  /**
   * Attempts to load a configuration file from the system. Uses the "ACCUMULO_CLIENT_CONF_PATH" environment variable, split on File.pathSeparator, for a list
   * of target files. If not set, uses the following in this order- ~/.accumulo/config $ACCUMULO_CONF_DIR/client.conf -OR- $ACCUMULO_HOME/conf/client.conf
   * (depending on whether $ACCUMULO_CONF_DIR is set) /etc/accumulo/client.conf
   *
   * A client configuration will then be read from each location using PropertiesConfiguration to construct a configuration. That means the latest item will be
   * the one in the configuration.
   *
   * @see PropertiesConfiguration
   * @see File#pathSeparator
   */
  public static ClientConfiguration loadDefault() {
    return loadFromSearchPath(getDefaultSearchPath());
  }

  private static ClientConfiguration loadFromSearchPath(List<String> paths) {
    try {
      List<Configuration> configs = new LinkedList<Configuration>();
      for (String path : paths) {
        File conf = new File(path);
        if (conf.canRead()) {
          configs.add(new PropertiesConfiguration(conf));
        }
      }
      return new ClientConfiguration(configs);
    } catch (ConfigurationException e) {
      throw new IllegalStateException("Error loading client configuration", e);
    }
  }

  public static ClientConfiguration deserialize(String serializedConfig) {
    PropertiesConfiguration propConfig = new PropertiesConfiguration();
    try {
      propConfig.load(new StringReader(serializedConfig));
    } catch (ConfigurationException e) {
      throw new IllegalArgumentException("Error deserializing client configuration: " + serializedConfig, e);
    }
    return new ClientConfiguration(propConfig);
  }

  private static List<String> getDefaultSearchPath() {
    String clientConfSearchPath = System.getenv("ACCUMULO_CLIENT_CONF_PATH");
    List<String> clientConfPaths;
    if (clientConfSearchPath != null) {
      clientConfPaths = Arrays.asList(clientConfSearchPath.split(File.pathSeparator));
    } else {
      // if $ACCUMULO_CLIENT_CONF_PATH env isn't set, priority from top to bottom is:
      // ~/.accumulo/config
      // $ACCUMULO_CONF_DIR/client.conf -OR- $ACCUMULO_HOME/conf/client.conf (depending on whether $ACCUMULO_CONF_DIR is set)
      // /etc/accumulo/client.conf
      clientConfPaths = new LinkedList<String>();
      clientConfPaths.add(System.getProperty("user.home") + File.separator + USER_ACCUMULO_DIR_NAME + File.separator + USER_CONF_FILENAME);
      if (System.getenv("ACCUMULO_CONF_DIR") != null) {
        clientConfPaths.add(System.getenv("ACCUMULO_CONF_DIR") + File.separator + GLOBAL_CONF_FILENAME);
      } else if (System.getenv("ACCUMULO_HOME") != null) {
        clientConfPaths.add(System.getenv("ACCUMULO_HOME") + File.separator + "conf" + File.separator + GLOBAL_CONF_FILENAME);
      }
      clientConfPaths.add("/etc/accumulo/" + GLOBAL_CONF_FILENAME);
    }
    return clientConfPaths;
  }

  public String serialize() {
    PropertiesConfiguration propConfig = new PropertiesConfiguration();
    propConfig.copy(this);
    StringWriter writer = new StringWriter();
    try {
      propConfig.save(writer);
    } catch (ConfigurationException e) {
      // this should never happen
      throw new IllegalStateException(e);
    }
    return writer.toString();
  }

  /**
   * Returns the value for prop, the default value if not present.
   *
   */
  public String get(ClientProperty prop) {
    if (this.containsKey(prop.getKey()))
      return this.getString(prop.getKey());
    else
      return prop.getDefaultValue();
  }

  /**
   * Sets the value of property to value
   *
   */
  public void setProperty(ClientProperty prop, String value) {
    this.setProperty(prop.getKey(), value);
  }

  /**
   * Same as {@link #setProperty(ClientProperty, String)} but returns the ClientConfiguration for chaining purposes
   */
  public ClientConfiguration with(ClientProperty prop, String value) {
    this.setProperty(prop.getKey(), value);
    return this;
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_NAME
   *
   */
  public ClientConfiguration withInstance(String instanceName) {
    ArgumentChecker.notNull(instanceName);
    return with(ClientProperty.INSTANCE_NAME, instanceName);
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_ID
   *
   */
  public ClientConfiguration withInstance(UUID instanceId) {
    ArgumentChecker.notNull(instanceId);
    return with(ClientProperty.INSTANCE_ID, instanceId.toString());
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_ZK_HOST
   *
   */
  public ClientConfiguration withZkHosts(String zooKeepers) {
    ArgumentChecker.notNull(zooKeepers);
    return with(ClientProperty.INSTANCE_ZK_HOST, zooKeepers);
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_ZK_TIMEOUT
   *
   */
  public ClientConfiguration withZkTimeout(int timeout) {
    return with(ClientProperty.INSTANCE_ZK_TIMEOUT, String.valueOf(timeout));
  }

  /**
   * Same as {@link #withSsl(boolean, boolean)} with useJsseConfig set to false
   *
   */
  public ClientConfiguration withSsl(boolean sslEnabled) {
    return withSsl(sslEnabled, false);
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_RPC_SSL_ENABLED and ClientProperty.RPC_USE_JSSE
   *
   */
  public ClientConfiguration withSsl(boolean sslEnabled, boolean useJsseConfig) {
    return with(ClientProperty.INSTANCE_RPC_SSL_ENABLED, String.valueOf(sslEnabled)).with(ClientProperty.RPC_USE_JSSE, String.valueOf(useJsseConfig));
  }

  /**
   * Same as {@link #withTruststore(String)} with password null and type null
   *
   */
  public ClientConfiguration withTruststore(String path) {
    return withTruststore(path, null, null);
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.RPC_SSL_TRUSTORE_PATH, ClientProperty.RPC_SSL_TRUSTORE_PASSWORD, and
   * ClientProperty.RPC_SSL_TRUSTORE_TYPE
   *
   */
  public ClientConfiguration withTruststore(String path, String password, String type) {
    ArgumentChecker.notNull(path);
    setProperty(ClientProperty.RPC_SSL_TRUSTSTORE_PATH, path);
    if (password != null)
      setProperty(ClientProperty.RPC_SSL_TRUSTSTORE_PASSWORD, password);
    if (type != null)
      setProperty(ClientProperty.RPC_SSL_TRUSTSTORE_TYPE, type);
    return this;
  }

  /**
   * Same as {@link #withKeystore(String, String, String)} with password null and type null
   *
   */
  public ClientConfiguration withKeystore(String path) {
    return withKeystore(path, null, null);
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_RPC_SSL_CLIENT_AUTH, ClientProperty.RPC_SSL_KEYSTORE_PATH,
   * ClientProperty.RPC_SSL_KEYSTORE_PASSWORD, and ClientProperty.RPC_SSL_KEYSTORE_TYPE
   *
   */
  public ClientConfiguration withKeystore(String path, String password, String type) {
    ArgumentChecker.notNull(path);
    setProperty(ClientProperty.INSTANCE_RPC_SSL_CLIENT_AUTH, "true");
    setProperty(ClientProperty.RPC_SSL_KEYSTORE_PATH, path);
    if (password != null)
      setProperty(ClientProperty.RPC_SSL_KEYSTORE_PASSWORD, password);
    if (type != null)
      setProperty(ClientProperty.RPC_SSL_KEYSTORE_TYPE, type);
    return this;
  }
}
