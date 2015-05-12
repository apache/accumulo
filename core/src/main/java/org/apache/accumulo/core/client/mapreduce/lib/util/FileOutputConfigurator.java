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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.hadoop.conf.Configuration;

/**
 * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
 * @since 1.5.0
 */
@Deprecated
public class FileOutputConfigurator extends ConfiguratorBase {

  /**
   * Configuration keys for {@link AccumuloConfiguration}.
   *
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static enum Opts {
    ACCUMULO_PROPERTIES;
  }

  /**
   * The supported Accumulo properties we set in this OutputFormat, that change the behavior of the RecordWriter.<br />
   * These properties correspond to the supported public static setter methods available to this class.
   *
   * @param property
   *          the Accumulo property to check
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
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
   * This helper method provides an AccumuloConfiguration object constructed from the Accumulo defaults, and overridden with Accumulo properties that have been
   * stored in the Job's configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static AccumuloConfiguration getAccumuloConfiguration(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.FileOutputConfigurator.getAccumuloConfiguration(implementingClass, conf);
  }

  /**
   * Sets the compression type to use for data blocks. Specifying a compression may require additional libraries to be available to your Job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param compressionType
   *          one of "none", "gz", "lzo", or "snappy"
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setCompressionType(Class<?> implementingClass, Configuration conf, String compressionType) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.FileOutputConfigurator.setCompressionType(implementingClass, conf, compressionType);
  }

  /**
   * Sets the size for data blocks within each file.<br />
   * Data blocks are a span of key/value pairs stored in the file that are compressed and indexed as a group.
   *
   * <p>
   * Making this value smaller may increase seek performance, but at the cost of increasing the size of the indexes (which can also affect seek performance).
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param dataBlockSize
   *          the block size, in bytes
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setDataBlockSize(Class<?> implementingClass, Configuration conf, long dataBlockSize) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.FileOutputConfigurator.setDataBlockSize(implementingClass, conf, dataBlockSize);
  }

  /**
   * Sets the size for file blocks in the file system; file blocks are managed, and replicated, by the underlying file system.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param fileBlockSize
   *          the block size, in bytes
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setFileBlockSize(Class<?> implementingClass, Configuration conf, long fileBlockSize) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.FileOutputConfigurator.setFileBlockSize(implementingClass, conf, fileBlockSize);
  }

  /**
   * Sets the size for index blocks within each file; smaller blocks means a deeper index hierarchy within the file, while larger blocks mean a more shallow
   * index hierarchy within the file. This can affect the performance of queries.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param indexBlockSize
   *          the block size, in bytes
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setIndexBlockSize(Class<?> implementingClass, Configuration conf, long indexBlockSize) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.FileOutputConfigurator.setIndexBlockSize(implementingClass, conf, indexBlockSize);
  }

  /**
   * Sets the file system replication factor for the resulting file, overriding the file system default.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param replication
   *          the number of replicas for produced files
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setReplication(Class<?> implementingClass, Configuration conf, int replication) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.FileOutputConfigurator.setReplication(implementingClass, conf, replication);
  }

}
