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

import static org.apache.accumulo.core.conf.ClientProperty.BATCH_WRITER_DURABILITY;
import static org.apache.accumulo.core.conf.ClientProperty.BATCH_WRITER_LATENCY_MAX;
import static org.apache.accumulo.core.conf.ClientProperty.BATCH_WRITER_MEMORY_MAX;
import static org.apache.accumulo.core.conf.ClientProperty.BATCH_WRITER_THREADS_MAX;
import static org.apache.accumulo.core.conf.ClientProperty.BATCH_WRITER_TIMEOUT_MAX;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.clientImpl.DurabilityImpl;
import org.apache.hadoop.conf.Configuration;

/**
 * @since 1.6.0
 */
public class OutputConfigurator extends ConfiguratorBase {

  /**
   * Configuration keys for {@link BatchWriter}.
   *
   * @since 1.6.0
   */
  public static enum WriteOpts {
    DEFAULT_TABLE_NAME, BATCH_WRITER_CONFIG
  }

  /**
   * Configuration keys for various features.
   *
   * @since 1.6.0
   */
  public static enum Features {
    CAN_CREATE_TABLES, SIMULATION_MODE
  }

  /**
   * Sets the default table name to use if one emits a null in place of a table name for a given
   * mutation. Table names can only be alpha-numeric and underscores.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param tableName the table to use when the tablename is null in the write call
   * @since 1.6.0
   */
  public static void setDefaultTableName(Class<?> implementingClass, Configuration conf,
      String tableName) {
    if (tableName != null) {
      conf.set(enumToConfKey(implementingClass, WriteOpts.DEFAULT_TABLE_NAME), tableName);
    }
  }

  /**
   * Gets the default table name from the configuration.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return the default table name
   * @since 1.6.0
   * @see #setDefaultTableName(Class, Configuration, String)
   */
  public static String getDefaultTableName(Class<?> implementingClass, Configuration conf) {
    return conf.get(enumToConfKey(implementingClass, WriteOpts.DEFAULT_TABLE_NAME));
  }

  /**
   * Gets the {@link BatchWriterConfig} settings that were stored with ClientInfo
   */
  public static BatchWriterConfig getBatchWriterOptions(Class<?> implementingClass,
      Configuration conf) {
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    Properties props = getClientProperties(implementingClass, conf);
    String property = props.getProperty(BATCH_WRITER_DURABILITY.getKey());
    if (property != null) {
      bwConfig.setDurability(DurabilityImpl.fromString(property));
    }
    Long value = BATCH_WRITER_LATENCY_MAX.getTimeInMillis(props);
    if (value != null) {
      bwConfig.setMaxLatency(value, TimeUnit.MILLISECONDS);
    }
    value = BATCH_WRITER_MEMORY_MAX.getBytes(props);
    if (value != null) {
      bwConfig.setMaxMemory(value);
    }
    value = BATCH_WRITER_TIMEOUT_MAX.getTimeInMillis(props);
    if (value != null) {
      bwConfig.setTimeout(value, TimeUnit.MILLISECONDS);
    }
    Integer intValue = BATCH_WRITER_THREADS_MAX.getInteger(props);
    if (intValue != null) {
      bwConfig.setMaxWriteThreads(intValue);
    }

    return bwConfig;
  }

  /**
   * Sets the directive to create new tables, as necessary. Table names can only be alpha-numeric
   * and underscores.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param enableFeature the feature is enabled if true, disabled otherwise
   * @since 1.6.0
   */
  public static void setCreateTables(Class<?> implementingClass, Configuration conf,
      boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.CAN_CREATE_TABLES), enableFeature);
  }

  /**
   * Determines whether tables are permitted to be created as needed.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return true if the feature is disabled, false otherwise
   * @since 1.6.0
   * @see #setCreateTables(Class, Configuration, boolean)
   */
  public static Boolean canCreateTables(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.CAN_CREATE_TABLES), false);
  }

  /**
   * Sets the directive to use simulation mode for this job. In simulation mode, no output is
   * produced. This is useful for testing.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param enableFeature the feature is enabled if true, disabled otherwise
   * @since 1.6.0
   */
  public static void setSimulationMode(Class<?> implementingClass, Configuration conf,
      boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.SIMULATION_MODE), enableFeature);
  }

  /**
   * Determines whether this feature is enabled.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @since 1.6.0
   * @see #setSimulationMode(Class, Configuration, boolean)
   */
  public static Boolean getSimulationMode(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.SIMULATION_MODE), false);
  }

}
