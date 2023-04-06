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

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.6.0
 */
public class FileOutputConfigurator extends ConfiguratorBase {

  private static final Logger log = LoggerFactory.getLogger(FileOutputConfigurator.class);

  /**
   * Configuration keys for {@link AccumuloConfiguration}.
   *
   * @since 1.6.0
   */
  public static enum Opts {
    ACCUMULO_PROPERTIES
  }

  /**
   * The supported Accumulo properties we set in this OutputFormat, that change the behavior of the
   * RecordWriter.<br>
   * These properties correspond to the supported public static setter methods available to this
   * class.
   *
   * @param property the Accumulo property to check
   * @since 1.6.0
   */
  protected static Boolean isSupportedAccumuloProperty(Property property) {
    switch (property) {
      case TABLE_FILE_COMPRESSION_TYPE:
      case TABLE_FILE_COMPRESSED_BLOCK_SIZE:
      case TABLE_FILE_BLOCK_SIZE:
      case TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX:
      case TABLE_FILE_REPLICATION:
        return true;
      default:
        return false;
    }
  }

  /**
   * Helper for transforming Accumulo configuration properties into something that can be stored
   * safely inside the Hadoop Job configuration.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param property the supported Accumulo property
   * @param value the value of the property to set
   * @since 1.6.0
   */
  private static <T> void setAccumuloProperty(Class<?> implementingClass, Configuration conf,
      Property property, T value) {
    if (isSupportedAccumuloProperty(property)) {
      String val = String.valueOf(value);
      if (property.getType().isValidFormat(val)) {
        String key =
            enumToConfKey(implementingClass, Opts.ACCUMULO_PROPERTIES) + "." + property.getKey();
        log.debug("Setting accumulo property {} = {} ", key, val);
        conf.set(key, val);
      } else {
        throw new IllegalArgumentException(
            "Value is not appropriate for property type '" + property.getType() + "'");
      }
    } else {
      throw new IllegalArgumentException("Unsupported configuration property " + property.getKey());
    }
  }

  /**
   * This helper method provides an AccumuloConfiguration object constructed from the Accumulo
   * defaults, and overridden with Accumulo properties that have been stored in the Job's
   * configuration.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @since 1.6.0
   */
  public static AccumuloConfiguration getAccumuloConfiguration(Class<?> implementingClass,
      Configuration conf) {
    String prefix = enumToConfKey(implementingClass, Opts.ACCUMULO_PROPERTIES) + ".";
    ConfigurationCopy acuConf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    for (Entry<String,String> entry : conf) {
      if (entry.getKey().startsWith(prefix)) {
        String propString = entry.getKey().substring(prefix.length());
        Property prop = Property.getPropertyByKey(propString);
        if (prop != null) {
          acuConf.set(prop, entry.getValue());
        } else if (Property.isValidTablePropertyKey(propString)) {
          acuConf.set(propString, entry.getValue());
        } else {
          throw new IllegalArgumentException("Unknown accumulo file property " + propString);
        }
      }
    }
    return acuConf;
  }

  /**
   * Sets the compression type to use for data blocks. Specifying a compression may require
   * additional libraries to be available to your Job.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param compressionType one of "none", "gz", "bzip2", "lzo", "lz4", "snappy", or "zstd"
   * @since 1.6.0
   */
  public static void setCompressionType(Class<?> implementingClass, Configuration conf,
      String compressionType) {
    if (compressionType == null || !Arrays
        .asList("none", "gz", "bzip2", "lzo", "lz4", "snappy", "zstd").contains(compressionType)) {
      throw new IllegalArgumentException(
          "Compression type must be one of: none, gz, bzip2, lzo, lz4, snappy, zstd");
    }
    setAccumuloProperty(implementingClass, conf, Property.TABLE_FILE_COMPRESSION_TYPE,
        compressionType);
  }

  /**
   * Sets the size for data blocks within each file.<br>
   * Data blocks are a span of key/value pairs stored in the file that are compressed and indexed as
   * a group.
   *
   * <p>
   * Making this value smaller may increase seek performance, but at the cost of increasing the size
   * of the indexes (which can also affect seek performance).
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param dataBlockSize the block size, in bytes
   * @since 1.6.0
   */
  public static void setDataBlockSize(Class<?> implementingClass, Configuration conf,
      long dataBlockSize) {
    setAccumuloProperty(implementingClass, conf, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE,
        dataBlockSize);
  }

  /**
   * Sets the size for file blocks in the file system; file blocks are managed, and replicated, by
   * the underlying file system.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param fileBlockSize the block size, in bytes
   * @since 1.6.0
   */
  public static void setFileBlockSize(Class<?> implementingClass, Configuration conf,
      long fileBlockSize) {
    setAccumuloProperty(implementingClass, conf, Property.TABLE_FILE_BLOCK_SIZE, fileBlockSize);
  }

  /**
   * Sets the size for index blocks within each file; smaller blocks means a deeper index hierarchy
   * within the file, while larger blocks mean a more shallow index hierarchy within the file. This
   * can affect the performance of queries.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param indexBlockSize the block size, in bytes
   * @since 1.6.0
   */
  public static void setIndexBlockSize(Class<?> implementingClass, Configuration conf,
      long indexBlockSize) {
    setAccumuloProperty(implementingClass, conf, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX,
        indexBlockSize);
  }

  /**
   * Sets the file system replication factor for the resulting file, overriding the file system
   * default.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param replication the number of replicas for produced files
   * @since 1.6.0
   */
  public static void setReplication(Class<?> implementingClass, Configuration conf,
      int replication) {
    setAccumuloProperty(implementingClass, conf, Property.TABLE_FILE_REPLICATION, replication);
  }

  /**
   * @since 1.8.0
   */
  public static void setSampler(Class<?> implementingClass, Configuration conf,
      SamplerConfiguration samplerConfig) {
    Map<String,String> props = new SamplerConfigurationImpl(samplerConfig).toTablePropertiesMap();

    Set<Entry<String,String>> es = props.entrySet();
    for (Entry<String,String> entry : es) {
      conf.set(enumToConfKey(implementingClass, Opts.ACCUMULO_PROPERTIES) + "." + entry.getKey(),
          entry.getValue());
    }
  }

  public static void setSummarizers(Class<?> implementingClass, Configuration conf,
      SummarizerConfiguration[] sumarizerConfigs) {
    Map<String,String> props = SummarizerConfiguration.toTableProperties(sumarizerConfigs);

    for (Entry<String,String> entry : props.entrySet()) {
      conf.set(enumToConfKey(implementingClass, Opts.ACCUMULO_PROPERTIES) + "." + entry.getKey(),
          entry.getValue());
    }
  }

}
