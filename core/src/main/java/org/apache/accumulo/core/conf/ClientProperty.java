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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.accumulo.core.Constants;

public enum ClientProperty {

  // Instance
  INSTANCE_NAME("instance.name", "", "Name of Accumulo instance to connect to", "", true),
  INSTANCE_ZOOKEEPERS("instance.zookeepers", "localhost:2181",
      "Zookeeper connection information for Accumulo instance", "", true),
  INSTANCE_ZOOKEEPERS_TIMEOUT_SEC("instance.zookeepers.timeout.sec", "30",
      "Zookeeper session timeout (in seconds)"),

  // Authentication
  AUTH_METHOD("auth.method", "password",
      "Authentication method (i.e password,"
          + " kerberos, provider). Set more properties for chosen method below.",
      "", true),
  AUTH_USERNAME("auth.username", "", "Accumulo username/principal for chosen authentication method",
      "", true),
  AUTH_PASSWORD("auth.password", "", "Accumulo user password", "", true),
  AUTH_KERBEROS_KEYTAB_PATH("auth.kerberos.keytab.path", "", "Path to Kerberos keytab"),
  AUTH_PROVIDER_NAME("auth.provider.name", "",
      "Alias used to extract Accumulo user password from CredentialProvider"),
  AUTH_PROVIDER_URLS("auth.provider.urls", "",
      "Comma separated list of URLs defining CredentialProvider(s)"),

  // BatchWriter
  BATCH_WRITER_MAX_MEMORY_BYTES("batch.writer.max.memory.bytes", "52428800",
      "Max memory (in bytes) to batch before writing"),
  BATCH_WRITER_MAX_LATENCY_SEC("batch.writer.max.latency.sec", "120",
      "Max amount of time (in seconds) to hold data in memory before flushing it"),
  BATCH_WRITER_MAX_TIMEOUT_SEC("batch.writer.max.timeout.sec", "0",
      "Max amount" + " of time (in seconds) an unresponsive server will be re-tried. An"
          + " exception is thrown when this timeout is exceeded. Set to zero for no timeout."),
  BATCH_WRITER_MAX_WRITE_THREADS("batch.writer.max.write.threads", "3",
      "Maximum number of threads to use for writing data to tablet servers."),
  BATCH_WRITER_DURABILITY("batch.writer.durability", "default",
      "Change the" + " durability for the BatchWriter session. To use the table's durability"
          + " setting. use \"default\" which is the table's durability setting."),

  // Bulk load
  BULK_LOAD_THREADS("bulk.threads", "8C",
      "The number of threads used to inspect bulk load files to determine where files go.  "
          + "If the value ends with C, then it will be multiplied by the number of cores on the "
          + "system. This property is only used by the bulk import API introduced in 2.0.0."),

  // SSL
  SSL_ENABLED("ssl.enabled", "false", "Enable SSL for client RPC"),
  SSL_KEYSTORE_PASSWORD("ssl.keystore.password", "", "Password used to encrypt keystore"),
  SSL_KEYSTORE_PATH("ssl.keystore.path", "", "Path to SSL keystore file"),
  SSL_KEYSTORE_TYPE("ssl.keystore.type", "jks", "Type of SSL keystore"),
  SSL_TRUSTSTORE_PASSWORD("ssl.truststore.password", "", "Password used to encrypt truststore"),
  SSL_TRUSTSTORE_PATH("ssl.truststore.path", "", "Path to SSL truststore file"),
  SSL_TRUSTSTORE_TYPE("ssl.truststore.type", "jks", "Type of SSL truststore"),
  SSL_USE_JSSE("ssl.use.jsse", "false", "Use JSSE system properties to configure SSL"),

  // SASL
  SASL_ENABLED("sasl.enabled", "false", "Enable SASL for client RPC"),
  SASL_QOP("sasl.qop", "auth",
      "SASL quality of protection. Valid values are 'auth', 'auth-int', and 'auth-conf'"),
  SASL_KERBEROS_SERVER_PRIMARY("sasl.kerberos.server.primary", "accumulo",
      "Kerberos principal/primary that Accumulo servers use to login"),

  // Trace
  TRACE_SPAN_RECEIVERS("trace.span.receivers", "org.apache.accumulo.tracer.ZooTraceClient",
      "A list of span receiver classes to send trace spans"),
  TRACE_ZOOKEEPER_PATH("trace.zookeeper.path", Constants.ZTRACERS,
      "The zookeeper node where tracers are registered");

  public static final String TRACE_SPAN_RECEIVER_PREFIX = "trace.span.receiver";

  private String key;
  private String defaultValue;
  private String description;
  private String since;
  private boolean required;

  ClientProperty(String key, String defaultValue, String description, String since,
      boolean required) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(defaultValue);
    Objects.requireNonNull(description);
    Objects.requireNonNull(since);
    this.key = key;
    this.defaultValue = defaultValue;
    this.description = description;
    this.since = since;
    this.required = required;
  }

  ClientProperty(String key, String defaultValue, String description, String since) {
    this(key, defaultValue, description, since, false);
  }

  ClientProperty(String key, String defaultValue, String description) {
    this(key, defaultValue, description, "");
  }

  public String getKey() {
    return key;
  }

  public String getDefaultValue() {
    return defaultValue;
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
    return value;
  }

  public Long getLong(Properties properties) {
    String value = getValue(properties);
    if (value.isEmpty()) {
      return null;
    }
    return Long.parseLong(value);
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
    return Boolean.valueOf(value);
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
}
