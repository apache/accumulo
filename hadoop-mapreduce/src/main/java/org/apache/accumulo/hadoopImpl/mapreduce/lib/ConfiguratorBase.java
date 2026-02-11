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
package org.apache.accumulo.hadoopImpl.mapreduce.lib;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;
import java.util.Scanner;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

/**
 * @since 1.6.0
 */
public class ConfiguratorBase {

  public enum ClientOpts {
    CLIENT_PROPS, CLIENT_PROPS_FILE, IS_CONFIGURED, STORE_JOB_CALLED
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
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param e the enum used to provide the unique part of the configuration key
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
   * @param e the enum used to provide the unique part of the configuration key
   * @return the configuration key
   */
  protected static String enumToConfKey(Enum<?> e) {
    return e.getDeclaringClass().getSimpleName() + "."
        + StringUtils.camelize(e.name().toLowerCase());
  }

  private static String cachedClientPropsFileName(Class<?> implementingClass) {
    return implementingClass.getSimpleName() + ".propsfile";
  }

  public static void setClientProperties(Class<?> implementingClass, Configuration conf,
      Properties props, String clientPropsPath) {
    if (clientPropsPath != null) {
      DistributedCacheHelper.addCacheFile(clientPropsPath,
          cachedClientPropsFileName(implementingClass), conf);
      conf.set(enumToConfKey(implementingClass, ClientOpts.CLIENT_PROPS_FILE), clientPropsPath);
    } else {
      StringWriter writer = new StringWriter();
      try {
        props.store(writer, "client properties");
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
      conf.set(enumToConfKey(implementingClass, ClientOpts.CLIENT_PROPS), writer.toString());
    }
    conf.setBoolean(enumToConfKey(implementingClass, ClientOpts.IS_CONFIGURED), true);
  }

  public static Properties getClientProperties(Class<?> implementingClass, Configuration conf) {
    String propString;
    String clientPropsFile =
        conf.get(enumToConfKey(implementingClass, ClientOpts.CLIENT_PROPS_FILE), "");
    if (clientPropsFile.isEmpty()) {
      propString = conf.get(enumToConfKey(implementingClass, ClientOpts.CLIENT_PROPS), "");
    } else {
      try (InputStream inputStream = DistributedCacheHelper.openCachedFile(clientPropsFile,
          cachedClientPropsFileName(implementingClass), conf)) {

        StringBuilder sb = new StringBuilder();
        try (Scanner scanner = new Scanner(inputStream, UTF_8)) {
          while (scanner.hasNextLine()) {
            sb.append(scanner.nextLine() + "\n");
          }
        }
        propString = sb.toString();
      } catch (IOException e) {
        throw new IllegalStateException("Error closing client properties file stream", e);
      }
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
   * Determines if the connector info has already been set for this instance.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return true if the connector info has already been set, false otherwise
   * @since 1.6.0
   */
  public static Boolean isClientConfigured(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, ClientOpts.IS_CONFIGURED), false);
  }

  /**
   * Creates an {@link AccumuloClient} based on the configuration that must be closed by user
   *
   * @param implementingClass class whose name will be used as a prefix for the property
   *        configuration
   * @param conf Hadoop configuration object
   * @return {@link AccumuloClient} that must be closed by user
   * @since 2.0.0
   */
  public static AccumuloClient createClient(Class<?> implementingClass, Configuration conf) {
    return Accumulo.newClient().from(getClientProperties(implementingClass, conf)).build();
  }

  /**
   * Sets the valid visibility count for this job.
   *
   * @param conf the Hadoop configuration object to configure
   * @param visibilityCacheSize the LRU cache size
   */
  public static void setVisibilityCacheSize(Configuration conf, int visibilityCacheSize) {
    conf.setInt(enumToConfKey(GeneralOpts.VISIBILITY_CACHE_SIZE), visibilityCacheSize);
  }

  /**
   * Gets the valid visibility count for this job.
   *
   * @param conf the Hadoop configuration object to configure
   * @return the valid visibility count
   */
  public static int getVisibilityCacheSize(Configuration conf) {
    return conf.getInt(enumToConfKey(GeneralOpts.VISIBILITY_CACHE_SIZE),
        Constants.DEFAULT_VISIBILITY_CACHE_SIZE);
  }

  /**
   * The store method was called.
   *
   * @since 2.0.0
   */
  public static void setJobStored(Class<?> implementingClass, Configuration conf) {
    conf.setBoolean(enumToConfKey(implementingClass, ClientOpts.STORE_JOB_CALLED), true);
  }

  /**
   * Checks if the job store method was called. If not throw exception.
   *
   * @since 2.0.0
   */
  public static void checkJobStored(Class<?> implementingClass, Configuration conf) {
    if (!conf.getBoolean(enumToConfKey(implementingClass, ClientOpts.STORE_JOB_CALLED), false)) {
      throw new IllegalStateException("Bad configuration: the store method was not called.");
    }
  }
}
