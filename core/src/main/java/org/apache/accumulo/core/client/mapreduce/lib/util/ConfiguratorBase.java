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
package org.apache.accumulo.core.client.mapreduce.lib.util;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.AuthenticationTokenSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;

/**
 * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
 * @since 1.5.0
 */
@Deprecated
public class ConfiguratorBase {

  /**
   * Configuration keys for {@link Instance#getConnector(String, AuthenticationToken)}.
   *
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static enum ConnectorInfo {
    IS_CONFIGURED, PRINCIPAL, TOKEN, TOKEN_CLASS
  }

  /**
   * Configuration keys for {@link Instance}, {@link ZooKeeperInstance}, and {@link MockInstance}.
   *
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  protected static enum InstanceOpts {
    TYPE, NAME, ZOO_KEEPERS;
  }

  /**
   * Configuration keys for general configuration options.
   *
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  protected static enum GeneralOpts {
    LOG_LEVEL
  }

  /**
   * Provides a configuration key for a given feature enum, prefixed by the implementingClass
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param e
   *          the enum used to provide the unique part of the configuration key
   * @return the configuration key
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  protected static String enumToConfKey(Class<?> implementingClass, Enum<?> e) {
    return implementingClass.getSimpleName() + "." + e.getDeclaringClass().getSimpleName() + "." + StringUtils.camelize(e.name().toLowerCase());
  }

  /**
   * Sets the connector information needed to communicate with Accumulo in this job.
   *
   * <p>
   * <b>WARNING:</b> The serialized token is stored in the configuration and shared with all MapReduce tasks. It is BASE64 encoded to provide a charset safe
   * conversion to a string, and is not intended to be secure.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param principal
   *          a valid Accumulo user name
   * @param token
   *          the user's password
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setConnectorInfo(Class<?> implementingClass, Configuration conf, String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.setConnectorInfo(implementingClass, conf, principal, token);
  }

  /**
   * Determines if the connector info has already been set for this instance.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the connector info has already been set, false otherwise
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setConnectorInfo(Class, Configuration, String, AuthenticationToken)
   */
  @Deprecated
  public static Boolean isConnectorInfoSet(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.isConnectorInfoSet(implementingClass, conf);
  }

  /**
   * Gets the user name from the configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the principal
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setConnectorInfo(Class, Configuration, String, AuthenticationToken)
   */
  @Deprecated
  public static String getPrincipal(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.getPrincipal(implementingClass, conf);
  }

  /**
   * DON'T USE THIS. No, really, don't use this. You already have an {@link AuthenticationToken} with
   * {@link org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase#getAuthenticationToken(Class, Configuration)}. You don't need to construct it
   * yourself.
   * <p>
   * Gets the serialized token class from the configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the principal
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setConnectorInfo(Class, Configuration, String, AuthenticationToken)
   */
  @Deprecated
  public static String getTokenClass(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.getAuthenticationToken(implementingClass, conf).getClass().getName();
  }

  /**
   * DON'T USE THIS. No, really, don't use this. You already have an {@link AuthenticationToken} with
   * {@link org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase#getAuthenticationToken(Class, Configuration)}. You don't need to construct it
   * yourself.
   * <p>
   * Gets the password from the configuration. WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to
   * provide a charset safe conversion to a string, and is not intended to be secure.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the decoded principal's authentication token
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setConnectorInfo(Class, Configuration, String, AuthenticationToken)
   */
  @Deprecated
  public static byte[] getToken(Class<?> implementingClass, Configuration conf) {
    return AuthenticationTokenSerializer.serialize(org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.getAuthenticationToken(
        implementingClass, conf));
  }

  /**
   * Configures a {@link ZooKeeperInstance} for this job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param instanceName
   *          the Accumulo instance name
   * @param zooKeepers
   *          a comma-separated list of zookeeper servers
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setZooKeeperInstance(Class<?> implementingClass, Configuration conf, String instanceName, String zooKeepers) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.setZooKeeperInstance(implementingClass, conf,
        new ClientConfiguration().withInstance(instanceName).withZkHosts(zooKeepers));
  }

  /**
   * Configures a {@link MockInstance} for this job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param instanceName
   *          the Accumulo instance name
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setMockInstance(Class<?> implementingClass, Configuration conf, String instanceName) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.setMockInstance(implementingClass, conf, instanceName);
  }

  /**
   * Initializes an Accumulo {@link Instance} based on the configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return an Accumulo instance
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setZooKeeperInstance(Class, Configuration, String, String)
   * @see #setMockInstance(Class, Configuration, String)
   */
  @Deprecated
  public static Instance getInstance(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.getInstance(implementingClass, conf);
  }

  /**
   * Sets the log level for this job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param level
   *          the logging level
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setLogLevel(Class<?> implementingClass, Configuration conf, Level level) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.setLogLevel(implementingClass, conf, level);
  }

  /**
   * Gets the log level from this configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the log level
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setLogLevel(Class, Configuration, Level)
   */
  @Deprecated
  public static Level getLogLevel(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.getLogLevel(implementingClass, conf);
  }

}
