/**
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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.tokens.SecurityToken;
import org.apache.accumulo.core.security.tokens.TokenHelper;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
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
    IS_CONFIGURED, TOKEN, TOKEN_IS_CACHE_FILE
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
   * @param token
   *          a valid AccumuloToken
   * @throws AccumuloSecurityException 
   * @since 1.5.0
   */
  public static void setConnectorInfo(Class<?> implementingClass, Configuration conf, SecurityToken token) throws AccumuloSecurityException {
    if (isConnectorInfoSet(implementingClass, conf))
      throw new IllegalStateException("Connector info for " + implementingClass.getSimpleName() + " can only be set once per job");
    
    ArgumentChecker.notNull(token);
    conf.setBoolean(enumToConfKey(implementingClass, ConnectorInfo.IS_CONFIGURED), true);
    conf.setBoolean(enumToConfKey(implementingClass, ConnectorInfo.TOKEN_IS_CACHE_FILE), false);
    conf.set(enumToConfKey(implementingClass, ConnectorInfo.TOKEN), TokenHelper.asBase64String(token));
  }
  
  /**
   * Sets the connector information needed to communicate with Accumulo in this job. The authentication information will be read from the specified file when
   * the job runs. This prevents the user's token from being exposed on the Job Tracker web page. The specified path will be placed in the
   * {@link DistributedCache}, for better performance during job execution. Users can create the contents of this file using
   * {@link TokenHelper#asBase64String(SecurityToken)}.
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param path
   *          the path to a file in the configured file system, containing the serialized, base-64 encoded {@link SecurityToken} with the user's authentication
   * @since 1.5.0
   */
  public static void setConnectorInfo(Class<?> implementingClass, Configuration conf, Path path) {
    if (isConnectorInfoSet(implementingClass, conf))
      throw new IllegalStateException("Connector info for " + implementingClass.getSimpleName() + " can only be set once per job");
    
    ArgumentChecker.notNull(path);
    URI uri = path.toUri();
    conf.setBoolean(enumToConfKey(implementingClass, ConnectorInfo.IS_CONFIGURED), true);
    conf.setBoolean(enumToConfKey(implementingClass, ConnectorInfo.TOKEN_IS_CACHE_FILE), true);
    DistributedCache.addCacheFile(uri, conf);
    conf.set(enumToConfKey(implementingClass, ConnectorInfo.TOKEN), uri.getPath());
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
   * @see #setConnectorInfo(Class, Configuration, SecurityToken)
   * @see #setConnectorInfo(Class, Configuration, Path)
   */
  public static Boolean isConnectorInfoSet(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, ConnectorInfo.IS_CONFIGURED), false);
  }
  
  /**
   * Gets the AccumuloToken from the configuration.
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the AccumuloToken
   * @throws AccumuloSecurityException 
   * @since 1.5.0
   * @see #setConnectorInfo(Class, Configuration, SecurityToken)
   * @see #setConnectorInfo(Class, Configuration, Path)
   */
  public static SecurityToken getToken(Class<?> implementingClass, Configuration conf) throws AccumuloSecurityException {
    if (!isConnectorInfoSet(implementingClass, conf))
      throw new IllegalStateException("Connector info for " + implementingClass.getSimpleName() + " has not been set");
    
    String token = conf.get(enumToConfKey(implementingClass, ConnectorInfo.TOKEN));
    
    if (conf.getBoolean(enumToConfKey(implementingClass, ConnectorInfo.TOKEN_IS_CACHE_FILE), false)) {
      String tokenFile = token;
      token = null;
      
      try {
        Path[] cf = DistributedCache.getLocalCacheFiles(conf);
        if (cf != null) {
          for (Path path : cf) {
            if (path.toUri().getPath().endsWith(tokenFile.substring(tokenFile.lastIndexOf('/')))) {
              StringBuilder fileContents = new StringBuilder();
              Scanner in = new Scanner(new BufferedReader(new FileReader(path.toString())));
              try {
                while (in.hasNextLine())
                  fileContents.append(in.nextLine());
              } finally {
                in.close();
              }
              token = fileContents.toString();
              break;
            }
          }
        }
        throw new FileNotFoundException(tokenFile + " not found in distributed cache");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return TokenHelper.fromBase64String(token);
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
