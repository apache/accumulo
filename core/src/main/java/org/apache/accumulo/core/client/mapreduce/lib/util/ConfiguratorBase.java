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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @since 1.5.0
 */
public class ConfiguratorBase {
  
  /**
   * Configuration keys for {@link Instance#getConnector(String, byte[])}.
   * 
   * @since 1.5.0
   */
  public static enum ConnectorInfo {
    IS_CONFIGURED, PRINCIPAL, TOKEN, TOKEN_CLASS, TOKEN_FILE
  }
  
  /**
   * Configuration keys for {@link Instance}, {@link ZooKeeperInstance}, and {@link MockInstance}.
   * 
   * @since 1.5.0
   */
  protected static enum InstanceOpts {
    TYPE, NAME, ZOO_KEEPERS;
  }
  
  /**
   * Configuration keys for general configuration options.
   * 
   * @since 1.5.0
   */
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
   * @since 1.5.0
   */
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
   * @throws AccumuloSecurityException
   * @since 1.5.0
   */
  public static void setConnectorInfo(Class<?> implementingClass, Configuration conf, String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    if (isConnectorInfoSet(implementingClass, conf))
      throw new IllegalStateException("Connector info for " + implementingClass.getSimpleName() + " can only be set once per job");
    
    ArgumentChecker.notNull(principal, token);
    conf.setBoolean(enumToConfKey(implementingClass, ConnectorInfo.IS_CONFIGURED), true);
    conf.set(enumToConfKey(implementingClass, ConnectorInfo.PRINCIPAL), principal);
    conf.set(enumToConfKey(implementingClass, ConnectorInfo.TOKEN_CLASS), token.getClass().getCanonicalName());
    conf.set(enumToConfKey(implementingClass, ConnectorInfo.TOKEN), CredentialHelper.tokenAsBase64(token));
  }
  
  /**
   * Sets the connector information needed to communicate with Accumulo in this job.
   * 
   * <p>
   * Pulls a token file into the Distributed Cache that contains the authentication token in an attempt to be more secure than storing the password in the
   * Configuration. Token file created with "bin/accumulo create-token".
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param principal
   *          a valid Accumulo user name
   * @param tokenFile
   *          the path to the token file
   * @throws AccumuloSecurityException
   * @since 1.6.0
   */
  public static void setConnectorInfo(Class<?> implementingClass, Configuration conf, String principal, String tokenFile) throws AccumuloSecurityException {
    if (isConnectorInfoSet(implementingClass, conf))
      throw new IllegalStateException("Connector info for " + implementingClass.getSimpleName() + " can only be set once per job");
    
    ArgumentChecker.notNull(principal, tokenFile);
    
    try {
      DistributedCache.addCacheFile(new URI(tokenFile), conf);
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Unable to add tokenFile \"" + tokenFile + "\" to distributed cache.");
    }
    
    conf.setBoolean(enumToConfKey(implementingClass, ConnectorInfo.IS_CONFIGURED), true);
    conf.set(enumToConfKey(implementingClass, ConnectorInfo.TOKEN_FILE), tokenFile);
    conf.set(enumToConfKey(implementingClass, ConnectorInfo.PRINCIPAL), principal);
  }
  
  /**
   * Determines if the connector info has already been set for this instance.
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the connector info has already been set, false otherwise
   * @since 1.5.0
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
   * @since 1.5.0
   * @see #setConnectorInfo(Class, Configuration, String, AuthenticationToken)
   */
  public static String getPrincipal(Class<?> implementingClass, Configuration conf) {
    return conf.get(enumToConfKey(implementingClass, ConnectorInfo.PRINCIPAL));
  }
  
  /**
   * Gets the serialized token class from either the configuration or the token file.
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the principal
   * @since 1.5.0
   * @see #setConnectorInfo(Class, Configuration, String, AuthenticationToken)
   */
  public static String getTokenClass(Class<?> implementingClass, Configuration conf) {
    String tokenFile = getTokenFile(implementingClass, conf);
    if (tokenFile.isEmpty()) {
      return conf.get(enumToConfKey(implementingClass, ConnectorInfo.TOKEN_CLASS));
    } else {
      return readTokenFile(implementingClass, conf).split(":")[1];
    }
  }
  
  /**
   * Gets the password from either the configuration or the token file. WARNING: If no token file is specified, the password is stored in the Configuration and
   * shared with all MapReduce tasks; It is BASE64 encoded to provide a charset safe conversion to a string, and is not intended to be secure.
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the decoded principal's authentication token
   * @since 1.5.0
   * @see #setConnectorInfo(Class, Configuration, String, AuthenticationToken)
   */
  public static byte[] getToken(Class<?> implementingClass, Configuration conf) {
    String tokenFile = getTokenFile(implementingClass, conf);
    String token = null;
    if (tokenFile.isEmpty()) {
      token = conf.get(enumToConfKey(implementingClass, ConnectorInfo.TOKEN));
    } else {
      token = readTokenFile(implementingClass, conf).split(":")[2];
    }
    return Base64.decodeBase64(token.getBytes(Charset.forName("UTF-8")));
  }
  
  /**
   * Grabs the token file's path out of the Configuration.
   * 
   * @param job
   *          the Hadoop context for the configured job
   * @return path to the token file as a String
   * @since 1.6.0
   * @see #setConnectorInfo(JobConf, String, AuthenticationToken)
   */
  public static String getTokenFile(Class<?> implementingClass, Configuration conf) {
    return conf.get(enumToConfKey(implementingClass, ConnectorInfo.TOKEN_FILE), "");
  }
  
  /**
   * Reads from the token file in distributed cache. Currently, the token file stores data separated by colons e.g. principal:token_class:token
   * 
   * @param job
   *          the Hadoop context for the configured job
   * @return path to the token file as a String
   * @since 1.6.0
   * @see #setConnectorInfo(JobConf, String, AuthenticationToken)
   */
  public static String readTokenFile(Class<?> implementingClass, Configuration conf) {
    String tokenFile = getTokenFile(implementingClass, conf);
    FSDataInputStream in = null;
    try {
      URI[] uris = DistributedCache.getCacheFiles(conf);
      Path path = null;
      for (URI u : uris) {
        if (u.toString().equals(tokenFile)) {
          path = new Path(u);
        }
      }
      if (path == null) {
        throw new IllegalArgumentException("Couldn't find password file called \"" + tokenFile + "\" in cache.");
      }
      FileSystem fs = FileSystem.get(conf);
      in = fs.open(path);
    } catch (IOException e) {
      throw new IllegalArgumentException("Couldn't open password file called \"" + tokenFile + "\".");
    }
    java.util.Scanner fileScanner = new java.util.Scanner(in);
    try {
      String line = null;
      boolean found = false;
      String principal = getPrincipal(implementingClass, conf);
      while (!found && fileScanner.hasNextLine()) {
        line = fileScanner.nextLine();
        if (line.startsWith(principal + ":")) {
          found = true;
          break;
        }
      }
      if (found)
        return line;
      else
        throw new IllegalArgumentException("Couldn't find token for user \"" + principal + "\" in file \"" + tokenFile + "\"");
    } finally {
      if (fileScanner != null && fileScanner.ioException() == null)
        fileScanner.close();
      else if (fileScanner.ioException() != null)
        throw new RuntimeException(fileScanner.ioException());
    }
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
   * @since 1.5.0
   */
  public static void setZooKeeperInstance(Class<?> implementingClass, Configuration conf, String instanceName, String zooKeepers) {
    String key = enumToConfKey(implementingClass, InstanceOpts.TYPE);
    if (!conf.get(key, "").isEmpty())
      throw new IllegalStateException("Instance info can only be set once per job; it has already been configured with " + conf.get(key));
    conf.set(key, "ZooKeeperInstance");
    
    ArgumentChecker.notNull(instanceName, zooKeepers);
    conf.set(enumToConfKey(implementingClass, InstanceOpts.NAME), instanceName);
    conf.set(enumToConfKey(implementingClass, InstanceOpts.ZOO_KEEPERS), zooKeepers);
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
   * @since 1.5.0
   */
  public static void setMockInstance(Class<?> implementingClass, Configuration conf, String instanceName) {
    String key = enumToConfKey(implementingClass, InstanceOpts.TYPE);
    if (!conf.get(key, "").isEmpty())
      throw new IllegalStateException("Instance info can only be set once per job; it has already been configured with " + conf.get(key));
    conf.set(key, "MockInstance");
    
    ArgumentChecker.notNull(instanceName);
    conf.set(enumToConfKey(implementingClass, InstanceOpts.NAME), instanceName);
  }
  
  /**
   * Initializes an Accumulo {@link Instance} based on the configuration.
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return an Accumulo instance
   * @since 1.5.0
   * @see #setZooKeeperInstance(Class, Configuration, String, String)
   * @see #setMockInstance(Class, Configuration, String)
   */
  public static Instance getInstance(Class<?> implementingClass, Configuration conf) {
    String instanceType = conf.get(enumToConfKey(implementingClass, InstanceOpts.TYPE), "");
    if ("MockInstance".equals(instanceType))
      return new MockInstance(conf.get(enumToConfKey(implementingClass, InstanceOpts.NAME)));
    else if ("ZooKeeperInstance".equals(instanceType))
      return new ZooKeeperInstance(conf.get(enumToConfKey(implementingClass, InstanceOpts.NAME)), conf.get(enumToConfKey(implementingClass,
          InstanceOpts.ZOO_KEEPERS)));
    else if (instanceType.isEmpty())
      throw new IllegalStateException("Instance has not been configured for " + implementingClass.getSimpleName());
    else
      throw new IllegalStateException("Unrecognized instance type " + instanceType);
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
   * @since 1.5.0
   */
  public static void setLogLevel(Class<?> implementingClass, Configuration conf, Level level) {
    ArgumentChecker.notNull(level);
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
   * @since 1.5.0
   * @see #setLogLevel(Class, Configuration, Level)
   */
  public static Level getLogLevel(Class<?> implementingClass, Configuration conf) {
    return Level.toLevel(conf.getInt(enumToConfKey(implementingClass, GeneralOpts.LOG_LEVEL), Level.INFO.toInt()));
  }
  
}
