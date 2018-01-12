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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.PropertyType;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.event.ConfigurationErrorEvent;
import org.apache.commons.configuration.event.ConfigurationErrorListener;
import org.apache.commons.configuration.event.ConfigurationEvent;
import org.apache.commons.configuration.event.ConfigurationListener;
import org.apache.commons.configuration.interpol.ConfigurationInterpolator;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.logging.Log;
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

    /**
     * @deprecated since 1.7.0 This method returns a type that is not part of the public API and not guaranteed to be stable.
     */
    @Deprecated
    public PropertyType getType() {
      return type;
    }

    public String getDescription() {
      return description;
    }

    /**
     * @deprecated since 1.7.0 This method returns a type that is not part of the public API and not guaranteed to be stable.
     */
    @Deprecated
    public Property getAccumuloProperty() {
      return accumuloProperty;
    }

    public static ClientProperty getPropertyByKey(String key) {
      for (ClientProperty prop : ClientProperty.values())
        if (prop.getKey().equals(key))
          return prop;
      return null;
    }
  }

  // helper for the constructor which takes a String file name
  private static PropertiesConfiguration newPropsFile(String file) throws ConfigurationException {
    PropertiesConfiguration props = new PropertiesConfiguration();
    props.setListDelimiter('\0');
    props.load(file);
    return props;
  }

  // helper for the constructor which takes a File
  private static PropertiesConfiguration newPropsFile(File file) throws ConfigurationException {
    PropertiesConfiguration props = new PropertiesConfiguration();
    props.setListDelimiter('\0');
    props.load(file);
    return props;
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API; use {@link #fromFile(File)} instead.
   */
  @Deprecated
  public ClientConfiguration(String configFile) throws ConfigurationException {
    this(Collections.singletonList(newPropsFile(configFile)));
  }

  /**
   * Load a client configuration from the provided configuration properties file
   *
   * @param configFile
   *          the path to the properties file
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API; use {@link #fromFile(File)} instead.
   */
  @Deprecated
  public ClientConfiguration(File configFile) throws ConfigurationException {
    this(Collections.singletonList(newPropsFile(configFile)));
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
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
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
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

  /**
   * Initializes an empty configuration object to be further configured with other methods on the class.
   *
   * @since 1.9.0
   */
  public static ClientConfiguration create() {
    return new ClientConfiguration(Collections.<Configuration> emptyList());
  }

  /**
   * Initializes a configuration object from the contents of a configuration file. Currently supports Java "properties" files. The returned object can be
   * further configured with subsequent calls to other methods on this class.
   *
   * @param file
   *          the path to the configuration file
   * @since 1.9.0
   */
  public static ClientConfiguration fromFile(File file) {
    try {
      return new ClientConfiguration(file);
    } catch (ConfigurationException e) {
      throw new IllegalArgumentException("Bad configuration file: " + file, e);
    }
  }

  /**
   * Initializes a configuration object from the contents of a map. The returned object can be further configured with subsequent calls to other methods on this
   * class.
   *
   * @param properties
   *          a map containing the configuration properties to use
   * @since 1.9.0
   */
  public static ClientConfiguration fromMap(Map<String,String> properties) {
    MapConfiguration mapConf = new MapConfiguration(properties);
    mapConf.setListDelimiter('\0');
    return new ClientConfiguration(Collections.singletonList(mapConf));
  }

  private static ClientConfiguration loadFromSearchPath(List<String> paths) {
    List<Configuration> configs = new LinkedList<>();
    for (String path : paths) {
      File conf = new File(path);
      if (conf.isFile() && conf.canRead()) {
        PropertiesConfiguration props = new PropertiesConfiguration();
        props.setListDelimiter('\0');
        try {
          props.load(conf);
          log.info("Loaded client configuration file {}", conf);
        } catch (ConfigurationException e) {
          throw new IllegalStateException("Error loading client configuration file " + conf, e);
        }
        configs.add(props);
      }
    }
    // We couldn't find the client configuration anywhere
    if (configs.isEmpty()) {
      log.warn("Found no client.conf in default paths. Using default client configuration values.");
    }
    return new ClientConfiguration(configs);
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
      // $ACCUMULO_CONF_DIR/client.conf -OR- $ACCUMULO_HOME/conf/client.conf (depending on whether $ACCUMULO_CONF_DIR is set)
      // /etc/accumulo/client.conf
      clientConfPaths = new LinkedList<>();
      clientConfPaths.add(System.getProperty("user.home") + File.separator + USER_ACCUMULO_DIR_NAME + File.separator + USER_CONF_FILENAME);
      if (System.getenv("ACCUMULO_CONF_DIR") != null) {
        clientConfPaths.add(System.getenv("ACCUMULO_CONF_DIR") + File.separator + GLOBAL_CONF_FILENAME);
      } else if (System.getenv("ACCUMULO_HOME") != null) {
        clientConfPaths.add(System.getenv("ACCUMULO_HOME") + File.separator + "conf" + File.separator + GLOBAL_CONF_FILENAME);
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
    with(prop, value);
  }

  /**
   * Same as {@link #setProperty(ClientProperty, String)} but returns the ClientConfiguration for chaining purposes
   */
  public ClientConfiguration with(ClientProperty prop, String value) {
    return with(prop.getKey(), value);
  }

  /**
   * Sets the value of property to value
   *
   * @since 1.9.0
   */
  public void setProperty(String prop, String value) {
    with(prop, value);
  }

  /**
   * Same as {@link #setProperty(String, String)} but returns the ClientConfiguration for chaining purposes
   *
   * @since 1.9.0
   */
  public ClientConfiguration with(String prop, String value) {
    super.setProperty(prop, value);
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
   * Show whether SASL has been set on this configuration.
   *
   * @since 1.9.0
   */
  public boolean hasSasl() {
    return getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), Boolean.parseBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getDefaultValue()));
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

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Configuration getConfiguration(int index) {
    return super.getConfiguration(index);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Configuration getSource(String key) {
    return super.getSource(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void removeConfiguration(Configuration config) {
    super.removeConfiguration(config);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void addConfiguration(Configuration config) {
    super.addConfiguration(config);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Configuration getInMemoryConfiguration() {
    return super.getInMemoryConfiguration();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Log getLogger() {
    return super.getLogger();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Configuration subset(String prefix) {
    return super.subset(prefix);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Configuration interpolatedConfiguration() {
    return super.interpolatedConfiguration();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void setLogger(Log log) {
    super.setLogger(log);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public ConfigurationInterpolator getInterpolator() {
    return super.getInterpolator();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public synchronized StrSubstitutor getSubstitutor() {
    return super.getSubstitutor();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void append(Configuration c) {
    super.append(c);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void copy(Configuration c) {
    super.copy(c);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void addConfigurationListener(ConfigurationListener l) {
    super.addConfigurationListener(l);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public boolean removeConfigurationListener(ConfigurationListener l) {
    return super.removeConfigurationListener(l);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public boolean removeErrorListener(ConfigurationErrorListener l) {
    return super.removeErrorListener(l);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void addErrorListener(ConfigurationErrorListener l) {
    super.addErrorListener(l);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void addErrorLogListener() {
    super.addErrorLogListener();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void addProperty(String key, Object value) {
    super.addProperty(key, value);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  protected void addPropertyDirect(String key, Object token) {
    super.addPropertyDirect(key, token);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void clear() {
    super.clear();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void clearConfigurationListeners() {
    super.clearConfigurationListeners();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void clearErrorListeners() {
    super.clearErrorListeners();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void clearProperty(String key) {
    super.clearProperty(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  protected void clearPropertyDirect(String key) {
    super.clearPropertyDirect(key);
  }

  @Override
  public boolean containsKey(String key) {
    return super.containsKey(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  protected ConfigurationErrorEvent createErrorEvent(int type, String propName, Object propValue, Throwable ex) {
    return super.createErrorEvent(type, propName, propValue, ex);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  protected ConfigurationEvent createEvent(int type, String propName, Object propValue, boolean before) {
    return super.createEvent(type, propName, propValue, before);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  protected ConfigurationInterpolator createInterpolator() {
    return super.createInterpolator();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  protected void fireError(int type, String propName, Object propValue, Throwable ex) {
    super.fireError(type, propName, propValue, ex);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  protected void fireEvent(int type, String propName, Object propValue, boolean before) {
    super.fireEvent(type, propName, propValue, before);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public BigDecimal getBigDecimal(String key) {
    return super.getBigDecimal(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public BigDecimal getBigDecimal(String key, BigDecimal defaultValue) {
    return super.getBigDecimal(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public BigInteger getBigInteger(String key) {
    return super.getBigInteger(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public BigInteger getBigInteger(String key, BigInteger defaultValue) {
    return super.getBigInteger(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public boolean getBoolean(String key) {
    return super.getBoolean(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public boolean getBoolean(String key, boolean defaultValue) {
    return super.getBoolean(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Boolean getBoolean(String key, Boolean defaultValue) {
    return super.getBoolean(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public byte getByte(String key) {
    return super.getByte(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public byte getByte(String key, byte defaultValue) {
    return super.getByte(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Byte getByte(String key, Byte defaultValue) {
    return super.getByte(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @SuppressWarnings("rawtypes")
  @Override
  public Collection getConfigurationListeners() {
    return super.getConfigurationListeners();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public double getDouble(String key) {
    return super.getDouble(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Double getDouble(String key, Double defaultValue) {
    return super.getDouble(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public double getDouble(String key, double defaultValue) {
    return super.getDouble(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @SuppressWarnings("rawtypes")
  @Override
  public Collection getErrorListeners() {
    return super.getErrorListeners();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public float getFloat(String key) {
    return super.getFloat(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Float getFloat(String key, Float defaultValue) {
    return super.getFloat(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public float getFloat(String key, float defaultValue) {
    return super.getFloat(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public int getInt(String key) {
    return super.getInt(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public int getInt(String key, int defaultValue) {
    return super.getInt(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Integer getInteger(String key, Integer defaultValue) {
    return super.getInteger(key, defaultValue);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<String> getKeys() {
    return super.getKeys();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  @Override
  public Iterator<String> getKeys(String key) {
    return super.getKeys(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @SuppressWarnings("rawtypes")
  @Override
  public List getList(String key) {
    return super.getList(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @SuppressWarnings("rawtypes")
  @Override
  public List getList(String key, List defaultValue) {
    return super.getList(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public char getListDelimiter() {
    return super.getListDelimiter();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public long getLong(String key) {
    return super.getLong(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public long getLong(String key, long defaultValue) {
    return super.getLong(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Long getLong(String key, Long defaultValue) {
    return super.getLong(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public int getNumberOfConfigurations() {
    return super.getNumberOfConfigurations();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Properties getProperties(String key) {
    return super.getProperties(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Properties getProperties(String key, Properties defaults) {
    return super.getProperties(key, defaults);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Object getProperty(String key) {
    return super.getProperty(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public short getShort(String key) {
    return super.getShort(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public short getShort(String key, short defaultValue) {
    return super.getShort(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public Short getShort(String key, Short defaultValue) {
    return super.getShort(key, defaultValue);
  }

  @Override
  public String getString(String key) {
    return super.getString(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public String getString(String key, String defaultValue) {
    return super.getString(key, defaultValue);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public String[] getStringArray(String key) {
    return super.getStringArray(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  protected Object interpolate(Object value) {
    return super.interpolate(value);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  protected String interpolate(String base) {
    return super.interpolate(base);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @SuppressWarnings("rawtypes")
  @Override
  protected String interpolateHelper(String base, List priorVariables) {
    return super.interpolateHelper(base, priorVariables);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public boolean isDelimiterParsingDisabled() {
    return super.isDelimiterParsingDisabled();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public boolean isDetailEvents() {
    return super.isDetailEvents();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public boolean isEmpty() {
    return super.isEmpty();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public boolean isThrowExceptionOnMissing() {
    return super.isThrowExceptionOnMissing();
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  protected Object resolveContainerStore(String key) {
    return super.resolveContainerStore(key);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void setDelimiterParsingDisabled(boolean delimiterParsingDisabled) {
    super.setDelimiterParsingDisabled(delimiterParsingDisabled);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void setDetailEvents(boolean enable) {
    super.setDetailEvents(enable);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void setListDelimiter(char listDelimiter) {
    super.setListDelimiter(listDelimiter);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void setProperty(String key, Object value) {
    super.setProperty(key, value);
  }

  /**
   * @deprecated since 1.9.0; will be removed in 2.0.0 to eliminate commons config leakage into Accumulo API
   */
  @Deprecated
  @Override
  public void setThrowExceptionOnMissing(boolean throwExceptionOnMissing) {
    super.setThrowExceptionOnMissing(throwExceptionOnMissing);
  }

}
