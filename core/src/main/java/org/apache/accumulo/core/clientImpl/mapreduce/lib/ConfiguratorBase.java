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
package org.apache.accumulo.core.clientImpl.mapreduce.lib;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.Properties;
import java.util.Scanner;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.clientImpl.DelegationTokenImpl;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @since 1.6.0
 * @deprecated since 2.0.0
 */
@Deprecated
public class ConfiguratorBase {

  protected static final Logger log = Logger.getLogger(ConfiguratorBase.class);

  /**
   * Specifies that connection info was configured
   *
   * @since 1.6.0
   */
  public enum ConnectorInfo {
    IS_CONFIGURED, PRINCIPAL, TOKEN
  }

  public static enum TokenSource {
    FILE, INLINE, JOB;

    private String prefix;

    private TokenSource() {
      prefix = name().toLowerCase() + ":";
    }

    public String prefix() {
      return prefix;
    }
  }

  /**
   * Configuration keys for available {@link Instance} types.
   *
   * @since 1.6.0
   */
  public static enum InstanceOpts {
    TYPE, NAME, ZOO_KEEPERS, CLIENT_CONFIG;
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

  public static Properties updateToken(org.apache.hadoop.security.Credentials credentials,
      Properties props) {
    Properties result = new Properties();
    props.forEach((key, value) -> result.setProperty((String) key, (String) value));

    AuthenticationToken token = ClientProperty.getAuthenticationToken(result);
    if (token instanceof KerberosToken) {
      log.info("Received KerberosToken, attempting to fetch DelegationToken");
      try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
        AuthenticationToken delegationToken = client.securityOperations()
            .getDelegationToken(new DelegationTokenConfig());
        ClientProperty.setAuthenticationToken(result, delegationToken);
      } catch (Exception e) {
        log.warn("Failed to automatically obtain DelegationToken, "
            + "Mappers/Reducers will likely fail to communicate with Accumulo", e);
      }
    }
    // DelegationTokens can be passed securely from user to task without serializing insecurely in
    // the configuration
    if (token instanceof DelegationTokenImpl) {
      DelegationTokenImpl delegationToken = (DelegationTokenImpl) token;

      // Convert it into a Hadoop Token
      AuthenticationTokenIdentifier identifier = delegationToken.getIdentifier();
      Token<AuthenticationTokenIdentifier> hadoopToken = new Token<>(identifier.getBytes(),
          delegationToken.getPassword(), identifier.getKind(), delegationToken.getServiceName());

      // Add the Hadoop Token to the Job so it gets serialized and passed along.
      credentials.addToken(hadoopToken.getService(), hadoopToken);
    }
    return result;
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
    conf.setBoolean(enumToConfKey(implementingClass, ConnectorInfo.IS_CONFIGURED), true);
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
    if (isConnectorInfoSet(implementingClass, conf))
      throw new IllegalStateException("Connector info for " + implementingClass.getSimpleName()
          + " can only be set once per job");

    checkArgument(principal != null, "principal is null");
    checkArgument(token != null, "token is null");
    conf.setBoolean(enumToConfKey(implementingClass, ConnectorInfo.IS_CONFIGURED), true);
  }

  /**
   * Sets the connector information needed to communicate with Accumulo in this job.
   *
   * <p>
   * Pulls a token file into the Distributed Cache that contains the authentication token in an
   * attempt to be more secure than storing the password in the Configuration. Token file created
   * with "bin/accumulo create-token".
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param principal
   *          a valid Accumulo user name
   * @param tokenFile
   *          the path to the token file in DFS
   * @since 1.6.0
   */
  public static void setConnectorInfo(Class<?> implementingClass, Configuration conf,
      String principal, String tokenFile) {
    if (isConnectorInfoSet(implementingClass, conf))
      throw new IllegalStateException("Connector info for " + implementingClass.getSimpleName()
          + " can only be set once per job");

    checkArgument(principal != null, "principal is null");
    checkArgument(tokenFile != null, "tokenFile is null");

    try {
      DistributedCacheHelper.addCacheFile(new URI(tokenFile), conf);
    } catch (URISyntaxException e) {
      throw new IllegalStateException(
          "Unable to add tokenFile \"" + tokenFile + "\" to distributed cache.");
    }

    conf.setBoolean(enumToConfKey(implementingClass, ConnectorInfo.IS_CONFIGURED), true);
    conf.set(enumToConfKey(implementingClass, ConnectorInfo.PRINCIPAL), principal);
    conf.set(enumToConfKey(implementingClass, ConnectorInfo.TOKEN),
        TokenSource.FILE.prefix() + tokenFile);
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
    return conf.get(enumToConfKey(implementingClass, ConnectorInfo.PRINCIPAL));
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
   * @see #setConnectorInfo(Class, Configuration, String, String)
   */
  public static AuthenticationToken getAuthenticationToken(Class<?> implementingClass,
      Configuration conf) {
    String token = conf.get(enumToConfKey(implementingClass, ConnectorInfo.TOKEN));
    if (token == null || token.isEmpty())
      return null;
    if (token.startsWith(TokenSource.INLINE.prefix())) {
      String[] args = token.substring(TokenSource.INLINE.prefix().length()).split(":", 2);
      if (args.length == 2)
        return org.apache.accumulo.core.client.security.tokens.AuthenticationToken.AuthenticationTokenSerializer
            .deserialize(args[0], Base64.getDecoder().decode(args[1].getBytes(UTF_8)));
    } else if (token.startsWith(TokenSource.FILE.prefix())) {
      String tokenFileName = token.substring(TokenSource.FILE.prefix().length());
      return getTokenFromFile(conf, getPrincipal(implementingClass, conf), tokenFileName);
    } else if (token.startsWith(TokenSource.JOB.prefix())) {
      String[] args = token.substring(TokenSource.JOB.prefix().length()).split(":", 2);
      if (args.length == 2) {
        String className = args[0], serviceName = args[1];
        if (DelegationTokenImpl.class.getName().equals(className)) {
          return new org.apache.accumulo.core.clientImpl.mapreduce.DelegationTokenStub(serviceName);
        }
      }
    }

    throw new IllegalStateException("Token was not properly serialized into the configuration");
  }

  /**
   * Reads from the token file in distributed cache. Currently, the token file stores data separated
   * by colons e.g. principal:token_class:token
   *
   * @param conf
   *          the Hadoop context for the configured job
   * @return path to the token file as a String
   * @since 1.6.0
   * @see #setConnectorInfo(Class, Configuration, String, AuthenticationToken)
   */
  public static AuthenticationToken getTokenFromFile(Configuration conf, String principal,
      String tokenFile) {
    FSDataInputStream in = null;
    try {
      URI[] uris = DistributedCacheHelper.getCacheFiles(conf);
      Path path = null;
      for (URI u : uris) {
        if (u.toString().equals(tokenFile)) {
          path = new Path(u);
        }
      }
      if (path == null) {
        throw new IllegalArgumentException(
            "Couldn't find password file called \"" + tokenFile + "\" in cache.");
      }
      FileSystem fs = FileSystem.get(conf);
      in = fs.open(path);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Couldn't open password file called \"" + tokenFile + "\".");
    }
    try (java.util.Scanner fileScanner = new java.util.Scanner(in)) {
      while (fileScanner.hasNextLine()) {
        org.apache.accumulo.core.clientImpl.Credentials creds = org.apache.accumulo.core.clientImpl.Credentials
            .deserialize(fileScanner.nextLine());
        if (principal.equals(creds.getPrincipal())) {
          return creds.getToken();
        }
      }
      throw new IllegalArgumentException(
          "Couldn't find token for user \"" + principal + "\" in file \"" + tokenFile + "\"");
    }
  }

  /**
   * Configures a {@link org.apache.accumulo.core.client.ZooKeeperInstance} for this job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param clientConfig
   *          client configuration for specifying connection timeouts, SSL connection options, etc.
   * @since 1.6.0
   */
  public static void setZooKeeperInstance(Class<?> implementingClass, Configuration conf,
      org.apache.accumulo.core.client.ClientConfiguration clientConfig) {
    String key = enumToConfKey(implementingClass, InstanceOpts.TYPE);
    if (!conf.get(key, "").isEmpty())
      throw new IllegalStateException(
          "Instance info can only be set once per job; it has already been configured with "
              + conf.get(key));
    conf.set(key, "ZooKeeperInstance");
    if (clientConfig != null) {
      conf.set(enumToConfKey(implementingClass, InstanceOpts.CLIENT_CONFIG),
          clientConfig.serialize());
    }
  }

  /**
   * Initializes an Accumulo {@link org.apache.accumulo.core.client.Instance} based on the
   * configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return an Accumulo instance
   * @since 1.6.0
   * @see #setZooKeeperInstance(Class, Configuration,
   *      org.apache.accumulo.core.client.ClientConfiguration)
   */
  public static org.apache.accumulo.core.client.Instance getInstance(Class<?> implementingClass,
      Configuration conf) {
    String instanceType = conf.get(enumToConfKey(implementingClass, InstanceOpts.TYPE), "");

    if ("ZooKeeperInstance".equals(instanceType)) {
      return new org.apache.accumulo.core.client.ZooKeeperInstance(
          getClientConfiguration(implementingClass, conf));
    } else if (instanceType.isEmpty()) {
      throw new IllegalStateException(
          "Instance has not been configured for " + implementingClass.getSimpleName());
    } else
      throw new IllegalStateException("Unrecognized instance type " + instanceType);
  }

  /**
   * Obtain a ClientConfiguration based on the configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   *
   * @return A ClientConfiguration
   * @since 1.7.0
   */
  public static org.apache.accumulo.core.client.ClientConfiguration getClientConfiguration(
      Class<?> implementingClass, Configuration conf) {
    String clientConfigString = conf
        .get(enumToConfKey(implementingClass, InstanceOpts.CLIENT_CONFIG));
    if (null != clientConfigString) {
      return org.apache.accumulo.core.client.ClientConfiguration.deserialize(clientConfigString);
    }

    String instanceName = conf.get(enumToConfKey(implementingClass, InstanceOpts.NAME));
    String zookeepers = conf.get(enumToConfKey(implementingClass, InstanceOpts.ZOO_KEEPERS));
    org.apache.accumulo.core.client.ClientConfiguration clientConf = org.apache.accumulo.core.client.ClientConfiguration
        .loadDefault();
    if (null != instanceName) {
      clientConf.withInstance(instanceName);
    }
    if (null != zookeepers) {
      clientConf.withZkHosts(zookeepers);
    }
    return clientConf;
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
   * @since 1.6.0
   */
  public static void setLogLevel(Class<?> implementingClass, Configuration conf, Level level) {
    checkArgument(level != null, "level is null");
    Logger.getLogger(implementingClass).setLevel(level);
    conf.setInt(enumToConfKey(implementingClass, GeneralOpts.LOG_LEVEL), level.toInt());
  }

  /**
   * Gets the log level from this configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the log level
   * @since 1.6.0
   * @see #setLogLevel(Class, Configuration, Level)
   */
  public static Level getLogLevel(Class<?> implementingClass, Configuration conf) {
    return Level.toLevel(
        conf.getInt(enumToConfKey(implementingClass, GeneralOpts.LOG_LEVEL), Level.INFO.toInt()));
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

  /**
   * Unwraps the provided {@link AuthenticationToken} if it is an instance of
   * {@link org.apache.accumulo.core.clientImpl.mapreduce.DelegationTokenStub}, reconstituting it
   * from the provided {@link JobConf}.
   *
   * @param job
   *          The job
   * @param token
   *          The authentication token
   */
  public static AuthenticationToken unwrapAuthenticationToken(JobConf job,
      AuthenticationToken token) {
    requireNonNull(job);
    requireNonNull(token);
    if (token instanceof org.apache.accumulo.core.clientImpl.mapreduce.DelegationTokenStub) {
      org.apache.accumulo.core.clientImpl.mapreduce.DelegationTokenStub delTokenStub = (org.apache.accumulo.core.clientImpl.mapreduce.DelegationTokenStub) token;
      Token<? extends TokenIdentifier> hadoopToken = job.getCredentials()
          .getToken(new Text(delTokenStub.getServiceName()));
      AuthenticationTokenIdentifier identifier = new AuthenticationTokenIdentifier();
      try {
        identifier
            .readFields(new DataInputStream(new ByteArrayInputStream(hadoopToken.getIdentifier())));
        return new DelegationTokenImpl(hadoopToken.getPassword(), identifier);
      } catch (IOException e) {
        throw new RuntimeException("Could not construct DelegationToken from JobConf Credentials",
            e);
      }
    }
    return token;
  }

  /**
   * Unwraps the provided {@link AuthenticationToken} if it is an instance of
   * {@link org.apache.accumulo.core.clientImpl.mapreduce.DelegationTokenStub}, reconstituting it
   * from the provided {@link JobConf}.
   *
   * @param job
   *          The job
   * @param token
   *          The authentication token
   */
  public static AuthenticationToken unwrapAuthenticationToken(JobContext job,
      AuthenticationToken token) {
    requireNonNull(job);
    requireNonNull(token);
    if (token instanceof org.apache.accumulo.core.clientImpl.mapreduce.DelegationTokenStub) {
      org.apache.accumulo.core.clientImpl.mapreduce.DelegationTokenStub delTokenStub = (org.apache.accumulo.core.clientImpl.mapreduce.DelegationTokenStub) token;
      Token<? extends TokenIdentifier> hadoopToken = job.getCredentials()
          .getToken(new Text(delTokenStub.getServiceName()));
      AuthenticationTokenIdentifier identifier = new AuthenticationTokenIdentifier();
      try {
        identifier
            .readFields(new DataInputStream(new ByteArrayInputStream(hadoopToken.getIdentifier())));
        return new DelegationTokenImpl(hadoopToken.getPassword(), identifier);
      } catch (IOException e) {
        throw new RuntimeException("Could not construct DelegationToken from JobConf Credentials",
            e);
      }
    }
    return token;
  }
}
