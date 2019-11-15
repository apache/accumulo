/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.PropertyType;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Contains a list of property keys recognized by the Accumulo client and convenience methods for
 * setting them.
 *
 * @since 1.6.0
 * @deprecated since 2.0.0, replaced by {@link Accumulo#newClient()}
 */
@Deprecated
public class ClientConfiguration {
  private static final Logger log = LoggerFactory.getLogger(ClientConfiguration.class);

  public static final String USER_ACCUMULO_DIR_NAME = ".accumulo";
  public static final String USER_CONF_FILENAME = "config";
  public static final String GLOBAL_CONF_FILENAME = "client.conf";

  private final CompositeConfiguration compositeConfig;

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
    INSTANCE_NAME("instance.name", null, PropertyType.STRING,
        "Name of Accumulo instance to connect to"),
    INSTANCE_ID("instance.id", null, PropertyType.STRING,
        "UUID of Accumulo instance to connect to"),

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
        "The first component of the Kerberos principal, the 'primary', "
            + "that Accumulo servers use to login");

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

  private ClientConfiguration(List<? extends Configuration> configs) {
    compositeConfig = new CompositeConfiguration(configs);
  }

  /**
   * Attempts to load a configuration file from the system using the default search paths. Uses the
   * <em>ACCUMULO_CLIENT_CONF_PATH</em> environment variable, split on <em>File.pathSeparator</em>,
   * for a list of target files.
   * <p>
   * If <em>ACCUMULO_CLIENT_CONF_PATH</em> is not set, uses the following in this order:
   * <ul>
   * <li>~/.accumulo/config
   * <li><em>$ACCUMULO_CONF_DIR</em>/client.conf, if <em>$ACCUMULO_CONF_DIR</em> is defined.
   * <li>/etc/accumulo/client.conf
   * <li>/etc/accumulo/conf/client.conf
   * </ul>
   * <p>
   * A client configuration will then be read from each location using
   * <em>PropertiesConfiguration</em> to construct a configuration. That means the latest item will
   * be the one in the configuration.
   *
   * @see PropertiesConfiguration
   * @see File#pathSeparator
   */
  public static ClientConfiguration loadDefault() {
    return loadFromSearchPath(getDefaultSearchPath());
  }

  /**
   * Initializes an empty configuration object to be further configured with other methods on the
   * class.
   *
   * @since 1.9.0
   */
  public static ClientConfiguration create() {
    return new ClientConfiguration(Collections.emptyList());
  }

  /**
   * Initializes a configuration object from the contents of a configuration file. Currently
   * supports Java "properties" files. The returned object can be further configured with subsequent
   * calls to other methods on this class.
   *
   * @param file
   *          the path to the configuration file
   * @since 1.9.0
   */
  public static ClientConfiguration fromFile(File file) {
    FileBasedConfigurationBuilder<PropertiesConfiguration> propsBuilder =
        new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
            .configure(new Parameters().properties().setFile(file));
    try {
      return new ClientConfiguration(Collections.singletonList(propsBuilder.getConfiguration()));
    } catch (ConfigurationException e) {
      throw new IllegalArgumentException("Bad configuration file: " + file, e);
    }
  }

  /**
   * Initializes a configuration object from the contents of a map. The returned object can be
   * further configured with subsequent calls to other methods on this class.
   *
   * @param properties
   *          a map containing the configuration properties to use
   * @since 1.9.0
   */
  public static ClientConfiguration fromMap(Map<String,String> properties) {
    MapConfiguration mapConf = new MapConfiguration(properties);
    return new ClientConfiguration(Collections.singletonList(mapConf));
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "process runs in same security context as user who provided path")
  private static ClientConfiguration loadFromSearchPath(List<String> paths) {
    List<Configuration> configs = new LinkedList<>();
    for (String path : paths) {
      File conf = new File(path);
      if (conf.isFile() && conf.canRead()) {
        FileBasedConfigurationBuilder<PropertiesConfiguration> propsBuilder =
            new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                .configure(new Parameters().properties().setFile(conf));
        try {
          configs.add(propsBuilder.getConfiguration());
          log.info("Loaded client configuration file {}", conf);
        } catch (ConfigurationException e) {
          throw new IllegalStateException("Error loading client configuration file " + conf, e);
        }
      }
    }
    // We couldn't find the client configuration anywhere
    if (configs.isEmpty()) {
      log.debug(
          "Found no client.conf in default paths. Using default client configuration values.");
    }
    return new ClientConfiguration(configs);
  }

  public static ClientConfiguration deserialize(String serializedConfig) {
    PropertiesConfiguration propConfig = new PropertiesConfiguration();
    try {
      propConfig.getLayout().load(propConfig, new StringReader(serializedConfig));
    } catch (ConfigurationException e) {
      throw new IllegalArgumentException(
          "Error deserializing client configuration: " + serializedConfig, e);
    }
    return new ClientConfiguration(Collections.singletonList(propConfig));
  }

  /**
   * Muck the value of {@code clientConfPath} if it points to a directory by appending
   * {@code client.conf} to the end of the file path. This is a no-op if the value is not a
   * directory on the filesystem.
   *
   * @param clientConfPath
   *          The value of ACCUMULO_CLIENT_CONF_PATH.
   */
  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "process runs in same security context as user who provided path")
  static String getClientConfPath(String clientConfPath) {
    if (clientConfPath == null) {
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
      clientConfPaths.add(System.getProperty("user.home") + File.separator + USER_ACCUMULO_DIR_NAME
          + File.separator + USER_CONF_FILENAME);
      if (System.getenv("ACCUMULO_CONF_DIR") != null) {
        clientConfPaths
            .add(System.getenv("ACCUMULO_CONF_DIR") + File.separator + GLOBAL_CONF_FILENAME);
      }
      clientConfPaths.add("/etc/accumulo/" + GLOBAL_CONF_FILENAME);
      clientConfPaths.add("/etc/accumulo/conf/" + GLOBAL_CONF_FILENAME);
    }
    return clientConfPaths;
  }

  public String serialize() {
    PropertiesConfiguration propConfig = new PropertiesConfiguration();
    propConfig.copy(compositeConfig);
    StringWriter writer = new StringWriter();
    try {
      propConfig.getLayout().save(propConfig, writer);
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
    if (compositeConfig.containsKey(prop.getKey()))
      return compositeConfig.getString(prop.getKey());
    else
      return prop.getDefaultValue();
  }

  private void checkType(ClientProperty property, PropertyType type) {
    if (!property.getType().equals(type)) {
      String msg = "Configuration method intended for type " + type + " called with a "
          + property.getType() + " argument (" + property.getKey() + ")";
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
    Iterator<?> iter = compositeConfig.getKeys(prefix);
    while (iter.hasNext()) {
      String p = (String) iter.next();
      propMap.put(p, compositeConfig.getString(p));
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
   * Same as {@link #setProperty(ClientProperty, String)} but returns the ClientConfiguration for
   * chaining purposes
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
   * Same as {@link #setProperty(String, String)} but returns the ClientConfiguration for chaining
   * purposes
   *
   * @since 1.9.0
   */
  public ClientConfiguration with(String prop, String value) {
    compositeConfig.setProperty(prop, value);
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
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_RPC_SSL_ENABLED and
   * ClientProperty.RPC_USE_JSSE
   *
   */
  public ClientConfiguration withSsl(boolean sslEnabled, boolean useJsseConfig) {
    return with(ClientProperty.INSTANCE_RPC_SSL_ENABLED, String.valueOf(sslEnabled))
        .with(ClientProperty.RPC_USE_JSSE, String.valueOf(useJsseConfig));
  }

  /**
   * Same as {@link #withTruststore(String, String, String)} with password null and type null
   *
   */
  public ClientConfiguration withTruststore(String path) {
    return withTruststore(path, null, null);
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.RPC_SSL_TRUSTORE_PATH,
   * ClientProperty.RPC_SSL_TRUSTORE_PASSWORD, and ClientProperty.RPC_SSL_TRUSTORE_TYPE
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
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_RPC_SSL_CLIENT_AUTH,
   * ClientProperty.RPC_SSL_KEYSTORE_PATH, ClientProperty.RPC_SSL_KEYSTORE_PASSWORD, and
   * ClientProperty.RPC_SSL_KEYSTORE_TYPE
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
    return compositeConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(),
        Boolean.parseBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getDefaultValue()));
  }

  /**
   * Same as {@link #with(ClientProperty, String)} for ClientProperty.INSTANCE_RPC_SASL_ENABLED and
   * ClientProperty.GENERAL_KERBEROS_PRINCIPAL.
   *
   * @param saslEnabled
   *          Should SASL(kerberos) be enabled
   * @param kerberosServerPrimary
   *          The 'primary' component of the Kerberos principal Accumulo servers use to login (e.g.
   *          'accumulo' in 'accumulo/_HOST@REALM')
   * @since 1.7.0
   */
  public ClientConfiguration withSasl(boolean saslEnabled, String kerberosServerPrimary) {
    return withSasl(saslEnabled).with(ClientProperty.KERBEROS_SERVER_PRIMARY,
        kerberosServerPrimary);
  }

  public boolean containsKey(String key) {
    return compositeConfig.containsKey(key);
  }

  public Iterator<String> getKeys() {
    return compositeConfig.getKeys();
  }

  public String getString(String key) {
    return compositeConfig.getString(key);
  }

}
