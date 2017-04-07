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

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.hadoop.conf.Configuration;

/**
 * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
 * @since 1.5.0
 */
@Deprecated
public class OutputConfigurator extends ConfiguratorBase {

  /**
   * Configuration keys for {@link BatchWriter}.
   *
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static enum WriteOpts {
    DEFAULT_TABLE_NAME, BATCH_WRITER_CONFIG
  }

  /**
   * Configuration keys for various features.
   *
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static enum Features {
    CAN_CREATE_TABLES, SIMULATION_MODE
  }

  /**
   * Sets the default table name to use if one emits a null in place of a table name for a given mutation. Table names can only be alpha-numeric and
   * underscores.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param tableName
   *          the table to use when the tablename is null in the write call
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setDefaultTableName(Class<?> implementingClass, Configuration conf, String tableName) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator.setDefaultTableName(implementingClass, conf, tableName);
  }

  /**
   * Gets the default table name from the configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the default table name
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setDefaultTableName(Class, Configuration, String)
   */
  @Deprecated
  public static String getDefaultTableName(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator.getDefaultTableName(implementingClass, conf);
  }

  /**
   * Sets the configuration for for the job's {@link BatchWriter} instances. If not set, a new {@link BatchWriterConfig}, with sensible built-in defaults is
   * used. Setting the configuration multiple times overwrites any previous configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param bwConfig
   *          the configuration for the {@link BatchWriter}
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setBatchWriterOptions(Class<?> implementingClass, Configuration conf, BatchWriterConfig bwConfig) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator.setBatchWriterOptions(implementingClass, conf, bwConfig);
  }

  /**
   * Gets the {@link BatchWriterConfig} settings.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the configuration object
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setBatchWriterOptions(Class, Configuration, BatchWriterConfig)
   */
  @Deprecated
  public static BatchWriterConfig getBatchWriterOptions(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator.getBatchWriterOptions(implementingClass, conf);
  }

  /**
   * Sets the directive to create new tables, as necessary. Table names can only be alpha-numeric and underscores.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setCreateTables(Class<?> implementingClass, Configuration conf, boolean enableFeature) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator.setCreateTables(implementingClass, conf, enableFeature);
  }

  /**
   * Determines whether tables are permitted to be created as needed.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the feature is disabled, false otherwise
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setCreateTables(Class, Configuration, boolean)
   */
  @Deprecated
  public static Boolean canCreateTables(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator.canCreateTables(implementingClass, conf);
  }

  /**
   * Sets the directive to use simulation mode for this job. In simulation mode, no output is produced. This is useful for testing.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setSimulationMode(Class<?> implementingClass, Configuration conf, boolean enableFeature) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator.setSimulationMode(implementingClass, conf, enableFeature);
  }

  /**
   * Determines whether this feature is enabled.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setSimulationMode(Class, Configuration, boolean)
   */
  @Deprecated
  public static Boolean getSimulationMode(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator.getSimulationMode(implementingClass, conf);
  }

}
