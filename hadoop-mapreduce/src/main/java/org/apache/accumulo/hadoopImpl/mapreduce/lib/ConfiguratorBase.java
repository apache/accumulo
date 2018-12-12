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
package org.apache.accumulo.hadoopImpl.mapreduce.lib;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Scanner;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.clientImpl.ClientInfoImpl;
import org.apache.accumulo.core.clientImpl.DelegationTokenImpl;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.6.0
 */
public class ConfiguratorBase {

  private static final Logger log = LoggerFactory.getLogger(ConfiguratorBase.class);

  /**
   * Specifies that connection info was configured
   *
   * @since 1.6.0
   */
  public enum ConnectorInfo {
    IS_CONFIGURED
  }

  public enum ClientOpts {
    CLIENT_PROPS, CLIENT_PROPS_FILE
  }

  /**
   * Configuration keys for general configuration options.
   *
   * @since 1.6.0
   */
  public enum GeneralOpts {
    LOG_LEVEL, VISIBILITY_CACHE_SIZE
  }

  /**
   * Provides a configuration key for a given feature enum, prefixed by the implementingClass
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param e
   *          the enum used to provide the unique part of the configuration key
   * @return the configuration key
   * @since 1.6.0
   */
  protected static String enumToConfKey(Class<?> implementingClass, Enum<?> e) {
    return implementingClass.getSimpleName() + "." + e.getDeclaringClass().getSimpleName() + "."
        + StringUtils.camelize(e.name().toLowerCase());
  }

  /**
   * Provides a configuration key for a given feature enum.
   *
   * @param e
   *          the enum used to provide the unique part of the configuration key
   * @return the configuration key
   */
  protected static String enumToConfKey(Enum<?> e) {
    return e.getDeclaringClass().getSimpleName() + "."
        + StringUtils.camelize(e.name().toLowerCase());
  }

  public static ClientInfo updateToken(org.apache.hadoop.security.Credentials credentials,
      ClientInfo info) {
    ClientInfo result = info;
    if (info.getAuthenticationToken() instanceof KerberosToken) {
      log.info("Received KerberosToken, attempting to fetch DelegationToken");
      try (AccumuloClient client = Accumulo.newClient().from(info.getProperties()).build()) {
        AuthenticationToken token = client.securityOperations()
            .getDelegationToken(new DelegationTokenConfig());
        result = ClientInfo.from(Accumulo.newClientProperties().from(info.getProperties())
            .as(info.getPrincipal(), token).build());
      } catch (Exception e) {
        log.warn("Failed to automatically obtain DelegationToken, "
            + "Mappers/Reducers will likely fail to communicate with Accumulo", e);
      }
    }
    // DelegationTokens can be passed securely from user to task without serializing insecurely in
    // the configuration
    if (info.getAuthenticationToken() instanceof DelegationTokenImpl) {
      DelegationTokenImpl delegationToken = (DelegationTokenImpl) info.getAuthenticationToken();

      // Convert it into a Hadoop Token
      AuthenticationTokenIdentifier identifier = delegationToken.getIdentifier();
      Token<AuthenticationTokenIdentifier> hadoopToken = new Token<>(identifier.getBytes(),
          delegationToken.getPassword(), identifier.getKind(), delegationToken.getServiceName());

      // Add the Hadoop Token to the Job so it gets serialized and passed along.
      credentials.addToken(hadoopToken.getService(), hadoopToken);
    }
    return result;
  }

  public static void setClientInfo(Class<?> implementingClass, Configuration conf,
      ClientInfo info) {
    setClientProperties(implementingClass, conf, info.getProperties());
    conf.setBoolean(enumToConfKey(implementingClass, ConnectorInfo.IS_CONFIGURED), true);
  }

  public static ClientInfo getClientInfo(Class<?> implementingClass, Configuration conf) {
    Properties props = getClientProperties(implementingClass, conf);
    return new ClientInfoImpl(props);
  }

  public static void setClientPropertiesFile(Class<?> implementingClass, Configuration conf,
      String clientPropertiesFile) {
    try {
      DistributedCacheHelper.addCacheFile(new URI(clientPropertiesFile), conf);
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Unable to add client properties file \""
          + clientPropertiesFile + "\" to distributed cache.");
    }
    conf.set(enumToConfKey(implementingClass, ClientOpts.CLIENT_PROPS_FILE), clientPropertiesFile);
    conf.setBoolean(enumToConfKey(implementingClass, ConnectorInfo.IS_CONFIGURED), true);
  }

  public static void setClientProperties(Class<?> implementingClass, Configuration conf,
      Properties props) {
    StringWriter writer = new StringWriter();
    try {
      props.store(writer, "client properties");
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    conf.set(enumToConfKey(implementingClass, ClientOpts.CLIENT_PROPS), writer.toString());
  }

  public static Properties getClientProperties(Class<?> implementingClass, Configuration conf) {
    String propString;
    String clientPropsFile = conf
        .get(enumToConfKey(implementingClass, ClientOpts.CLIENT_PROPS_FILE), "");
    if (!clientPropsFile.isEmpty()) {
      try {
        URI[] uris = DistributedCacheHelper.getCacheFiles(conf);
        Path path = null;
        for (URI u : uris) {
          if (u.toString().equals(clientPropsFile)) {
            path = new Path(u);
          }
        }
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(path);
        StringBuilder sb = new StringBuilder();
        try (Scanner scanner = new Scanner(inputStream)) {
          while (scanner.hasNextLine()) {
            sb.append(scanner.nextLine() + "\n");
          }
        }
        propString = sb.toString();
      } catch (IOException e) {
        throw new IllegalStateException(
            "Failed to read client properties from distributed cache: " + clientPropsFile);
      }
    } else {
      propString = conf.get(enumToConfKey(implementingClass, ClientOpts.CLIENT_PROPS), "");
    }
    Properties props = new Properties();
    if (!propString.isEmpty()) {
      try {
        props.load(new StringReader(propString));
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
    return props;
  }

  /**
   * Sets the connector information needed to communicate with Accumulo in this job.
   *
   * <p>
   * <b>WARNING:</b> The serialized token is stored in the configuration and shared with all
   * MapReduce tasks. It is BASE64 encoded to provide a charset safe conversion to a string, and is
   * not intended to be secure.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param principal
   *          a valid Accumulo user name
   * @param token
   *          the user's password
   * @since 1.6.0
   */
  public static void setConnectorInfo(Class<?> implementingClass, Configuration conf,
      String principal, AuthenticationToken token) {
    checkArgument(principal != null, "principal is null");
    checkArgument(token != null, "token is null");
    Properties props = getClientProperties(implementingClass, conf);
    props.setProperty(ClientProperty.AUTH_PRINCIPAL.getKey(), principal);
    ClientProperty.setAuthenticationToken(props, token);
    setClientProperties(implementingClass, conf, props);
    conf.setBoolean(enumToConfKey(implementingClass, ConnectorInfo.IS_CONFIGURED), true);
  }

  /**
   * Determines if the connector info has already been set for this instance.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the connector info has already been set, false otherwise
   * @since 1.6.0
   * @see #setConnectorInfo(Class, Configuration, String, AuthenticationToken)
   */
  public static Boolean isConnectorInfoSet(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, ConnectorInfo.IS_CONFIGURED), false);
  }

  /**
   * Gets the user name from the configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the principal
   * @since 1.6.0
   * @see #setConnectorInfo(Class, Configuration, String, AuthenticationToken)
   */
  public static String getPrincipal(Class<?> implementingClass, Configuration conf) {
    Properties props = getClientProperties(implementingClass, conf);
    return props.getProperty(ClientProperty.AUTH_PRINCIPAL.getKey());
  }

  /**
   * Gets the authenticated token from either the specified token file or directly from the
   * configuration, whichever was used when the job was configured.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the principal's authentication token
   * @since 1.6.0
   * @see #setConnectorInfo(Class, Configuration, String, AuthenticationToken)
   */
  public static AuthenticationToken getAuthenticationToken(Class<?> implementingClass,
      Configuration conf) {
    Properties props = getClientProperties(implementingClass, conf);
    return ClientProperty.getAuthenticationToken(props);
  }

  /**
   * Creates an {@link AccumuloClient} based on the configuration that must be closed by user
   *
   * @param implementingClass
   *          class whose name will be used as a prefix for the property configuration
   * @param conf
   *          Hadoop configuration object
   * @return {@link AccumuloClient} that must be closed by user
   * @since 2.0.0
   */
  public static AccumuloClient createClient(Class<?> implementingClass, Configuration conf) {
    return Accumulo.newClient().from(getClientProperties(implementingClass, conf)).build();
  }

  /**
   * Sets the valid visibility count for this job.
   *
   * @param conf
   *          the Hadoop configuration object to configure
   * @param visibilityCacheSize
   *          the LRU cache size
   */
  public static void setVisibilityCacheSize(Configuration conf, int visibilityCacheSize) {
    conf.setInt(enumToConfKey(GeneralOpts.VISIBILITY_CACHE_SIZE), visibilityCacheSize);
  }

  /**
   * Gets the valid visibility count for this job.
   *
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the valid visibility count
   */
  public static int getVisibilityCacheSize(Configuration conf) {
    return conf.getInt(enumToConfKey(GeneralOpts.VISIBILITY_CACHE_SIZE),
        Constants.DEFAULT_VISIBILITY_CACHE_SIZE);
  }
}
