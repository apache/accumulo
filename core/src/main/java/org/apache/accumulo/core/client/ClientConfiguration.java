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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.PropertyType;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains a list of property keys recognized by the Accumulo client and convenience methods for setting them.
 *
 * @since 1.6.0
 */
public class ClientConfiguration extends CompositeConfiguration {
  private static final Logger log = LoggerFactory.getLogger(ClientConfiguration.class);

  public static final String USER_ACCUMULO_DIR_NAME = ".accumulo";
  public static final String USER_CONF_FILENAME = "config";
  public static final String GLOBAL_CONF_FILENAME = "client.conf";

  public enum ClientProperty {
    // SSL
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

    // ZooKeeper
    INSTANCE_ZK_HOST(Property.INSTANCE_ZK_HOST),
    INSTANCE_ZK_TIMEOUT(Property.INSTANCE_ZK_TIMEOUT),

    // Instance information
    INSTANCE_NAME("instance.name", null, PropertyType.STRING, "Name of Accumulo instance to connect to"),
    INSTANCE_ID("instance.id", null, PropertyType.STRING, "UUID of Accumulo instance to connect to"),

    // Tracing
    TRACE_SPAN_RECEIVERS(Property.TRACE_SPAN_RECEIVERS),
    TRACE_SPAN_RECEIVER_PREFIX(Property.TRACE_SPAN_RECEIVER_PREFIX),
    TRACE_ZK_PATH(Property.TRACE_ZK_PATH),

    // SASL / GSSAPI(Kerberos)
    /**
     * @since 1.7.0
     */
    INSTANCE_RPC_SASL_ENABLED(Property.INSTANCE_RPC_SASL_ENABLED),
    /**
     * @since 1.7.0
     */
    RPC_SASL_QOP(Property.RPC_SASL_QOP),
    /**
     * @since 1.7.0
     */
    KERBEROS_SERVER_PRIMARY("kerberos.server.primary", "accumulo", PropertyType.STRING,
        "The first component of the Kerberos principal, the 'primary', that Accumulo servers use to login");

    private String key;
    private String defaultValue;
    private PropertyType type;
    private String description;

    private ClientProperty(Property prop) {
      this(prop.getKey(), prop.getDefaultValue(), prop.getType(), prop.getDescription());
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

    private PropertyType getType() {
      return type;
    }

    public String getDescription() {
      return description;
    }

    public static ClientProperty getPropertyByKey(String key) {
      for (ClientProperty prop : ClientProperty.values())
        if (prop.getKey().equals(key))
          return prop;
      return null;
    }
  }

  public ClientConfiguration(String configFile) throws ConfigurationException {
    this(new PropertiesConfiguration(), configFile);
  }

  private ClientConfiguration(PropertiesConfiguration propertiesConfiguration, String configFile) throws ConfigurationException {
    super(propertiesConfiguration);
    // Don't do list interpolation
    this.setListDelimiter('\0');
    propertiesConfiguration.setListDelimiter('\0');
    propertiesConfiguration.load(configFile);
  }

  public ClientConfiguration(File configFile) throws ConfigurationException {
    this(new PropertiesConfiguration(), configFile);
  }

  private ClientConfiguration(PropertiesConfiguration propertiesConfiguration, File configFile) throws ConfigurationException {
    super(propertiesConfiguration);
    // Don't do list interpolation
    this.setListDelimiter('\0');
    propertiesConfiguration.setListDelimiter('\0');
    propertiesConfiguration.load(configFile);
  }

  public ClientConfiguration(List<? extends Configuration> configs) {
    super(configs);
    // Don't do list interpolation
    this.setListDelimiter('\0');
    for (Configuration c : configs) {
      if (c instanceof AbstractConfiguration) {
        AbstractConfiguration abstractConfiguration = (AbstractConfiguration) c;
        if (!abstractConfiguration.isDelimiterParsingDisabled() && abstractConfiguration.getListDelimiter() != '\0') {
          log.warn("Client configuration constructed with a Configuration that did not have list delimiter disabled or overridden, multi-valued config "
              + "properties may be unavailable");
          abstractConfiguration.setListDelimiter('\0');
        }
      }
    }
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
   * Attempts to load a configuration file from the system using the default search paths. Uses the <em>ACCUMULO_CLIENT_CONF_PATH</em> environment variable,
   * split on <em>File.pathSeparator</em>, for a list of target files.
   * <p>
   * If <em>ACCUMULO_CLIENT_CONF_PATH</em> is not set, uses the following in this order:
   * <ul>
   * <li>~/.accumulo/config
   * <li><em>$ACCUMULO_CONF_DIR</em>/client.conf, if <em>$ACCUMULO_CONF_DIR</em> is defined.
   * <li>/etc/accumulo/client.conf
   * <li>/etc/accumulo/conf/client.conf
   * </ul>
   * <p>
   * A client configuration will then be read from each location using <em>PropertiesConfiguration</em> to construct a configuration. That means the latest item
   * will be the one in the configuration.
   * <p>
   *
   * @see PropertiesConfiguration
   * @see File#pathSeparator
   */
  public static ClientConfiguration loadDefault() {
    return loadFromSearchPath(getDefaultSearchPath());
  }

  private static ClientConfiguration loadFromSearchPath(List<String> paths) {
    try {
      List<Configuration> configs = new LinkedList<>();
      for (String path : paths) {
        File conf = new File(path);
        if (conf.isFile() && conf.canRead()) {
          configs.add(new ClientConfiguration(conf));
        }
      }
      // We couldn't find the client configuration anywhere
      if (configs.isEmpty()) {
        log.warn("Found no client.conf in default paths. Using default client configuration values.");
      }
      return new ClientConfiguration(configs);
    } catch (ConfigurationException e) {
      throw new IllegalStateException("Error loading client configuration", e);
    }
  }

  public static ClientConfiguration deserialize(String serializedConfig) {
    PropertiesConfiguration propConfig = new PropertiesConfiguration();
    propConfig.setListDelimiter('\0');
    try {
      propConfig.load(new StringReader(serializedConfig));
    } catch (ConfigurationException e) {
      throw new IllegalArgumentException("Error deserializing client configuration: " + serializedConfig, e);
    }
    return new ClientConfiguration(propConfig);
  }

  /**
   * Muck the value of {@code clientConfPath} if it points to a directory by appending {@code client.conf} to the end of the file path. This is a no-op if the
   * value is not a directory on the filesystem.
   *
   * @param clientConfPath
   *          The value of ACCUMULO_CLIENT_CONF_PATH.
   */
  static String getClientConfPath(String clientConfPath) {
    if (null == clientConfPath) {
      return null;
    }
    File filePath = new File(clientConfPath);
    // If clientConfPath is a directory, tack on the default client.conf file name.
    if (filePath.exists() && filePath.isDirectory()) {
      return new File(filePath, "client.conf").toString();
    }
    return clientConfPath;
  }

  private static List<String> getDefaultSearchPath() {
    String clientConfSearchPath = getClientConfPath(System.getenv("ACCUMULO_CLIENT_CONF_PATH"));
    List<String> clientConfPaths;
    if (clientConfSearchPath != null) {
      clientConfPaths = Arrays.asList(clientConfSearchPath.split(File.pathSeparator));
    } else {
      // if $ACCUMULO_CLIENT_CONF_PATH env isn't set, priority from top to bottom is:
      // ~/.accumulo/config
      // $ACCUMULO_CONF_DIR/client.conf
      // /etc/accumulo/client.conf
      // /etc/accumulo/conf/client.conf
      clientConfPaths = new LinkedList<>();
      clientConfPaths.add(System.getProperty("user.home") + File.separator + USER_ACCUMULO_DIR_NAME + File.separator + USER_CONF_FILENAME);
      if (System.getenv("ACCUMULO_CONF_DIR") != null) {
        clientConfPaths.add(System.getenv("ACCUMULO_CONF_DIR") + File.separator + GLOBAL_CONF_FILENAME);
      }
      clientConfPaths.add("/etc/accumulo/" + GLOBAL_CONF_FILENAME);
      clientConfPaths.add("/etc/accumulo/conf/" + GLOBAL_CONF_FILENAME);
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

  private void checkType(ClientProperty property, PropertyType type) {
    if (!property.getType().equals(type)) {
      String msg = "Configuration method intended for type " + type + " called with a " + property.getType() + " argument (" + property.getKey() + ")";
      throw new IllegalArgumentException(msg);
    }
  }

  /**
   * Gets all properties under the given prefix in this configuration.
   *
   * @param property
   *          prefix property, must be of type PropertyType.PREFIX
   * @return a map of property keys to values
   * @throws IllegalArgumentException
   *           if property is not a prefix
   */
  public Map<String,String> getAllPropertiesWithPrefix(ClientProperty property) {
    checkType(property, PropertyType.PREFIX);

    Map<String,String> propMap = new HashMap<>();
    String prefix = property.getKey();
    if (prefix.endsWith(".")) {
      prefix = prefix.substring(0, prefix.length() - 1);
    }
    Iterator<?> iter = this.getKeys(prefix);
    while (iter.hasNext()) {
      String p = (String) iter.next();
      propMap.put(p, getString(p));
    }
    return propMap;
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
    checkArgument(instanceName != null, "instanceName is null");
    return with(ClientProperty.INSTANCE_NAME, instanceName);
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_ID
   *
   */
  public ClientConfiguration withInstance(UUID instanceId) {
    checkArgument(instanceId != null, "instanceId is null");
    return with(ClientProperty.INSTANCE_ID, instanceId.toString());
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_ZK_HOST
   *
   */
  public ClientConfiguration withZkHosts(String zooKeepers) {
    checkArgument(zooKeepers != null, "zooKeepers is null");
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
    checkArgument(path != null, "path is null");
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
    checkArgument(path != null, "path is null");
    setProperty(ClientProperty.INSTANCE_RPC_SSL_CLIENT_AUTH, "true");
    setProperty(ClientProperty.RPC_SSL_KEYSTORE_PATH, path);
    if (password != null)
      setProperty(ClientProperty.RPC_SSL_KEYSTORE_PASSWORD, password);
    if (type != null)
      setProperty(ClientProperty.RPC_SSL_KEYSTORE_TYPE, type);
    return this;
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_RPC_SASL_ENABLED.
   *
   * @since 1.7.0
   */
  public ClientConfiguration withSasl(boolean saslEnabled) {
    return with(ClientProperty.INSTANCE_RPC_SASL_ENABLED, String.valueOf(saslEnabled));
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_RPC_SASL_ENABLED and ClientProperty.GENERAL_KERBEROS_PRINCIPAL.
   *
   * @param saslEnabled
   *          Should SASL(kerberos) be enabled
   * @param kerberosServerPrimary
   *          The 'primary' component of the Kerberos principal Accumulo servers use to login (e.g. 'accumulo' in 'accumulo/_HOST@REALM')
   * @since 1.7.0
   */
  public ClientConfiguration withSasl(boolean saslEnabled, String kerberosServerPrimary) {
    return withSasl(saslEnabled).with(ClientProperty.KERBEROS_SERVER_PRIMARY, kerberosServerPrimary);
  }
}
