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
package org.apache.accumulo.core.client.mapreduce.lib.impl;

import static com.google.common.base.Charsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
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
   * Sets the default table name to use if one emits a null in place of a table name for a given mutation. Table names can only be alpha-numeric and
   * underscores.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param tableName
   *          the table to use when the tablename is null in the write call
   * @since 1.6.0
   */
  public static void setDefaultTableName(Class<?> implementingClass, Configuration conf, String tableName) {
    if (tableName != null)
      conf.set(enumToConfKey(implementingClass, WriteOpts.DEFAULT_TABLE_NAME), tableName);
  }

  /**
   * Gets the default table name from the configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the default table name
   * @since 1.6.0
   * @see #setDefaultTableName(Class, Configuration, String)
   */
  public static String getDefaultTableName(Class<?> implementingClass, Configuration conf) {
    return conf.get(enumToConfKey(implementingClass, WriteOpts.DEFAULT_TABLE_NAME));
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
   * @since 1.6.0
   */
  public static void setBatchWriterOptions(Class<?> implementingClass, Configuration conf, BatchWriterConfig bwConfig) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String serialized;
    try {
      bwConfig.write(new DataOutputStream(baos));
      serialized = new String(baos.toByteArray(), UTF_8);
      baos.close();
    } catch (IOException e) {
      throw new IllegalArgumentException("unable to serialize " + BatchWriterConfig.class.getName());
    }
    conf.set(enumToConfKey(implementingClass, WriteOpts.BATCH_WRITER_CONFIG), serialized);
  }

  /**
   * Gets the {@link BatchWriterConfig} settings.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the configuration object
   * @since 1.6.0
   * @see #setBatchWriterOptions(Class, Configuration, BatchWriterConfig)
   */
  public static BatchWriterConfig getBatchWriterOptions(Class<?> implementingClass, Configuration conf) {
    String serialized = conf.get(enumToConfKey(implementingClass, WriteOpts.BATCH_WRITER_CONFIG));
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    if (serialized == null || serialized.isEmpty()) {
      return bwConfig;
    } else {
      try {
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized.getBytes(UTF_8));
        bwConfig.readFields(new DataInputStream(bais));
        bais.close();
        return bwConfig;
      } catch (IOException e) {
        throw new IllegalArgumentException("unable to serialize " + BatchWriterConfig.class.getName());
      }
    }
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
   * @since 1.6.0
   */
  public static void setCreateTables(Class<?> implementingClass, Configuration conf, boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.CAN_CREATE_TABLES), enableFeature);
  }

  /**
   * Determines whether tables are permitted to be created as needed.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the feature is disabled, false otherwise
   * @since 1.6.0
   * @see #setCreateTables(Class, Configuration, boolean)
   */
  public static Boolean canCreateTables(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.CAN_CREATE_TABLES), false);
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
   * @since 1.6.0
   */
  public static void setSimulationMode(Class<?> implementingClass, Configuration conf, boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.SIMULATION_MODE), enableFeature);
  }

  /**
   * Determines whether this feature is enabled.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @since 1.6.0
   * @see #setSimulationMode(Class, Configuration, boolean)
   */
  public static Boolean getSimulationMode(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.SIMULATION_MODE), false);
  }

}
