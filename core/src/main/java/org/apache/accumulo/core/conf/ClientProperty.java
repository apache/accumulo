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

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.accumulo.core.client.admin.TableOperations.ImportMappingOptions;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.CredentialProviderToken;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.spi.scan.ConfigurableScanServerSelector;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public enum ClientProperty {

  // Instance
  INSTANCE_NAME("instance.name", "", PropertyType.STRING, "Name of Accumulo instance to connect to",
      "2.0.0", true),
  INSTANCE_ZOOKEEPERS("instance.zookeepers", "localhost:2181", PropertyType.HOSTLIST,
      "Zookeeper connection information for Accumulo instance", "2.0.0", true),
  INSTANCE_ZOOKEEPERS_TIMEOUT("instance.zookeepers.timeout", "30s", PropertyType.TIMEDURATION,
      "Zookeeper session timeout", "2.0.0", false),

  // Authentication
  AUTH_TYPE("auth.type", "password", PropertyType.STRING,
      "Authentication method (i.e password, kerberos, PasswordToken, KerberosToken, etc)", "2.0.0",
      true),
  AUTH_PRINCIPAL("auth.principal", "", PropertyType.STRING,
      "Accumulo principal/username for chosen authentication method", "2.0.0", true),
  AUTH_TOKEN("auth.token", "", PropertyType.STRING,
      "Authentication token (ex. mypassword, /path/to/keytab)", "2.0.0", true),

  // BatchWriter
  BATCH_WRITER_MEMORY_MAX("batch.writer.memory.max", "50M", PropertyType.BYTES,
      "Max memory (in bytes) to batch before writing", "2.0.0", false),
  BATCH_WRITER_LATENCY_MAX("batch.writer.latency.max", "120s", PropertyType.TIMEDURATION,
      "Max amount of time (in seconds) to hold data in memory before flushing it", "2.0.0", false),
  BATCH_WRITER_TIMEOUT_MAX("batch.writer.timeout.max", "0", PropertyType.TIMEDURATION,
      "Max amount of time (in seconds) an unresponsive server will be re-tried. An"
          + " exception is thrown when this timeout is exceeded. Set to zero for no timeout.",
      "2.0.0", false),
  BATCH_WRITER_THREADS_MAX("batch.writer.threads.max", "3", PropertyType.COUNT,
      "Maximum number of threads to use for writing data to tablet servers.", "2.0.0", false),
  BATCH_WRITER_DURABILITY("batch.writer.durability", "default", PropertyType.DURABILITY,
      Property.TABLE_DURABILITY.getDescription() + " Setting this property will "
          + "change the durability for the BatchWriter session. A value of \"default\" will"
          + " use the table's durability setting. ",
      "2.0.0", false),

  // ConditionalWriter
  CONDITIONAL_WRITER_TIMEOUT_MAX("conditional.writer.timeout.max", "0", PropertyType.TIMEDURATION,
      "Maximum amount of time an unresponsive server will be re-tried. A value of 0 will use "
          + "Long.MAX_VALUE.",
      "2.1.0", false),
  CONDITIONAL_WRITER_THREADS_MAX("conditional.writer.threads.max", "3", PropertyType.COUNT,
      "Maximum number of threads to use for writing data to tablet servers.", "2.1.0", false),
  CONDITIONAL_WRITER_DURABILITY("conditional.writer.durability", "default", PropertyType.DURABILITY,
      Property.TABLE_DURABILITY.getDescription() + " Setting this property will change the "
          + "durability for the ConditionalWriter session. A value of \"default\" will use the"
          + " table's durability setting. ",
      "2.1.0", false),

  // Scanner
  SCANNER_BATCH_SIZE("scanner.batch.size", "1000", PropertyType.COUNT,
      "Number of key/value pairs that will be fetched at time from tablet server", "2.0.0", false),

  SCAN_SERVER_SELECTOR("scan.server.selector.impl", ConfigurableScanServerSelector.class.getName(),
      PropertyType.CLASSNAME, "Class used by client to find Scan Servers", "2.1.0", false),

  SCAN_SERVER_SELECTOR_OPTS_PREFIX("scan.server.selector.opts.", "", PropertyType.PREFIX,
      "Properties in this category are related to the configuration of the scan.server.selector.impl class",
      "2.1.0", false),

  // BatchScanner
  BATCH_SCANNER_NUM_QUERY_THREADS("batch.scanner.num.query.threads", "3", PropertyType.COUNT,
      "Number of concurrent query threads to spawn for querying", "2.0.0", false),

  // Bulk load
  BULK_LOAD_THREADS("bulk.threads", ImportMappingOptions.BULK_LOAD_THREADS_DEFAULT,
      PropertyType.COUNT,
      "The number of threads used to inspect bulk load files to determine where files go.  "
          + "If the value ends with C, then it will be multiplied by the number of cores on the "
          + "system. This property is only used by the bulk import API introduced in 2.0.0.",
      "2.0.0", false),

  // SSL
  SSL_ENABLED("ssl.enabled", "false", "Enable SSL for client RPC"),
  SSL_KEYSTORE_PASSWORD("ssl.keystore.password", "", "Password used to encrypt keystore"),
  SSL_KEYSTORE_PATH("ssl.keystore.path", "", PropertyType.PATH, "Path to SSL keystore file",
      "2.0.0", false),
  SSL_KEYSTORE_TYPE("ssl.keystore.type", "jks", "Type of SSL keystore"),
  SSL_TRUSTSTORE_PASSWORD("ssl.truststore.password", "", "Password used to encrypt truststore"),
  SSL_TRUSTSTORE_PATH("ssl.truststore.path", "", PropertyType.PATH, "Path to SSL truststore file",
      "2.0.0", false),
  SSL_TRUSTSTORE_TYPE("ssl.truststore.type", "jks", "Type of SSL truststore"),
  SSL_USE_JSSE("ssl.use.jsse", "false", "Use JSSE system properties to configure SSL"),

  // SASL
  SASL_ENABLED("sasl.enabled", "false", "Enable SASL for client RPC"),
  SASL_QOP("sasl.qop", "auth",
      "SASL quality of protection. Valid values are 'auth', 'auth-int', and 'auth-conf'"),
  SASL_KERBEROS_SERVER_PRIMARY("sasl.kerberos.server.primary", "accumulo",
      "Kerberos principal/primary that Accumulo servers use to login"),

  // RPC
  RPC_TRANSPORT_IDLE_TIMEOUT("rpc.transport.idle.timeout", "3s", PropertyType.TIMEDURATION,
      "The maximum duration to leave idle transports open in the client's transport pool", "2.1.0",
      false),

  ;

  private final String key;
  private final String defaultValue;
  private final PropertyType type;
  private final String description;
  private final String since;
  private final boolean required;

  ClientProperty(String key, String defaultValue, PropertyType type, String description,
      String since, boolean required) {
    this.key = Objects.requireNonNull(key);
    this.defaultValue = Objects.requireNonNull(defaultValue);
    this.type = Objects.requireNonNull(type);
    this.description = Objects.requireNonNull(description);
    this.since = Objects.requireNonNull(since);
    this.required = required;
  }

  ClientProperty(String key, String defaultValue, String description) {
    this(key, defaultValue, PropertyType.STRING, description, "", false);
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

  public String getSince() {
    return since;
  }

  public boolean isRequired() {
    return required;
  }

  public String getValue(Properties properties) {
    Objects.requireNonNull(properties);
    String value = properties.getProperty(getKey());
    if (value == null || value.isEmpty()) {
      value = getDefaultValue();
    }
    Objects.requireNonNull(value);
    if (isRequired() && value.isEmpty()) {
      throw new IllegalArgumentException(getKey() + " must be set!");
    }
    if (!type.isValidFormat(value)) {
      throw new IllegalArgumentException(
          "Invalid format for type \"" + type + "\" for provided value: " + value);
    }
    return value;
  }

  public boolean isEmpty(Properties properties) {
    Objects.requireNonNull(properties);
    String value = properties.getProperty(getKey());
    return (value == null || value.isEmpty());
  }

  public Long getBytes(Properties properties) {
    String value = getValue(properties);
    if (value.isEmpty()) {
      return null;
    }
    checkState(getType() == PropertyType.BYTES,
        "Invalid type getting bytes. Type must be " + PropertyType.BYTES + ", not " + getType());
    return ConfigurationTypeHelper.getMemoryAsBytes(value);
  }

  public Long getTimeInMillis(Properties properties) {
    String value = getValue(properties);
    if (value.isEmpty()) {
      return null;
    }
    checkState(getType() == PropertyType.TIMEDURATION, "Invalid type getting time. Type must be "
        + PropertyType.TIMEDURATION + ", not " + getType());
    return ConfigurationTypeHelper.getTimeInMillis(value);
  }

  public Integer getInteger(Properties properties) {
    String value = getValue(properties);
    if (value.isEmpty()) {
      return null;
    }
    return Integer.parseInt(value);
  }

  public boolean getBoolean(Properties properties) {
    String value = getValue(properties);
    if (value.isEmpty()) {
      return false;
    }
    return Boolean.parseBoolean(value);
  }

  public void setBytes(Properties properties, Long bytes) {
    checkState(getType() == PropertyType.BYTES,
        "Invalid type setting bytes. Type must be " + PropertyType.BYTES + ", not " + getType());
    properties.setProperty(getKey(), bytes.toString());
  }

  public void setTimeInMillis(Properties properties, Long milliseconds) {
    checkState(getType() == PropertyType.TIMEDURATION, "Invalid type setting "
        + "time. Type must be " + PropertyType.TIMEDURATION + ", not " + getType());
    properties.setProperty(getKey(), milliseconds + "ms");
  }

  public static Properties getPrefix(Properties properties, String prefix) {
    Properties props = new Properties();
    for (Object keyObj : properties.keySet()) {
      String key = (String) keyObj;
      if (key.startsWith(prefix)) {
        props.put(key, properties.getProperty(key));
      }
    }
    return props;
  }

  public static Map<String,String> toMap(Properties properties) {
    Map<String,String> propMap = new HashMap<>();
    for (Object obj : properties.keySet()) {
      propMap.put((String) obj, properties.getProperty((String) obj));
    }
    return propMap;
  }

  public static String encodeToken(AuthenticationToken token) {
    return Base64.getEncoder()
        .encodeToString(AuthenticationToken.AuthenticationTokenSerializer.serialize(token));
  }

  public static AuthenticationToken decodeToken(String className, String tokenString) {
    return AuthenticationToken.AuthenticationTokenSerializer.deserialize(className,
        Base64.getDecoder().decode(tokenString));
  }

  public static void setPassword(Properties properties, CharSequence password) {
    properties.setProperty(ClientProperty.AUTH_TYPE.getKey(), "password");
    properties.setProperty(ClientProperty.AUTH_TOKEN.getKey(), password.toString());
  }

  public static void setKerberosKeytab(Properties properties, String keytabPath) {
    properties.setProperty(ClientProperty.AUTH_TYPE.getKey(), "kerberos");
    properties.setProperty(ClientProperty.AUTH_TOKEN.getKey(), keytabPath);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who providing the token file")
  public static AuthenticationToken getAuthenticationToken(Properties properties) {
    String authType = ClientProperty.AUTH_TYPE.getValue(properties);
    String token = ClientProperty.AUTH_TOKEN.getValue(properties);
    switch (authType) {
      case "password":
        return new PasswordToken(token);
      case "PasswordToken":
        return decodeToken(PasswordToken.class.getName(), token);
      case "kerberos":
        try {
          String principal = ClientProperty.AUTH_PRINCIPAL.getValue(properties);
          return new KerberosToken(principal, new File(token));
        } catch (IOException e) {
          throw new IllegalArgumentException(e);
        }
      case "KerberosToken":
        return decodeToken(KerberosToken.class.getName(), token);
      case "CredentialProviderToken":
        return decodeToken(CredentialProviderToken.class.getName(), token);
      case "DelegationToken":
        return decodeToken(DelegationToken.class.getName(), token);
      default:
        return decodeToken(authType, token);
    }
  }

  public static void setAuthenticationToken(Properties properties, AuthenticationToken token) {
    properties.setProperty(ClientProperty.AUTH_TYPE.getKey(), token.getClass().getName());
    properties.setProperty(ClientProperty.AUTH_TOKEN.getKey(), encodeToken(token));
  }

  public static void validateProperty(Properties properties, ClientProperty prop) {
    if (!properties.containsKey(prop.getKey()) || prop.getValue(properties).isEmpty()) {
      throw new IllegalArgumentException(prop.getKey() + " is not set");
    }
  }

  public static void validate(Properties properties, boolean validateToken) {
    validateProperty(properties, ClientProperty.INSTANCE_NAME);
    validateProperty(properties, ClientProperty.INSTANCE_ZOOKEEPERS);
    validateProperty(properties, ClientProperty.AUTH_TYPE);
    validateProperty(properties, ClientProperty.AUTH_PRINCIPAL);
    if (validateToken) {
      validateProperty(properties, ClientProperty.AUTH_TOKEN);
    }
  }

  /**
   * @throws IllegalArgumentException if Properties does not contain all required
   * @throws NullPointerException if {@code properties == null}
   */
  public static void validate(Properties properties) {
    validate(properties, true);
  }
}
