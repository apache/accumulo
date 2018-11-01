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
package org.apache.accumulo.hadoopImpl.mapreduce;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.FileOutputConfigurator;
import org.apache.hadoop.mapreduce.Job;

public class AccumuloFileOutputFormatImpl {

  private static final Class<?> CLASS = AccumuloFileOutputFormat.class;

  /**
   * Sets the compression type to use for data blocks. Specifying a compression may require
   * additional libraries to be available to your Job.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param compressionType
   *          one of "none", "gz", "lzo", or "snappy"
   * @since 1.5.0
   */
  public static void setCompressionType(Job job, String compressionType) {
    FileOutputConfigurator.setCompressionType(CLASS, job.getConfiguration(), compressionType);
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
   * @param job
   *          the Hadoop job instance to be configured
   * @param dataBlockSize
   *          the block size, in bytes
   * @since 1.5.0
   */
  public static void setDataBlockSize(Job job, long dataBlockSize) {
    FileOutputConfigurator.setDataBlockSize(CLASS, job.getConfiguration(), dataBlockSize);
  }

  /**
   * Sets the size for file blocks in the file system; file blocks are managed, and replicated, by
   * the underlying file system.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param fileBlockSize
   *          the block size, in bytes
   * @since 1.5.0
   */
  public static void setFileBlockSize(Job job, long fileBlockSize) {
    FileOutputConfigurator.setFileBlockSize(CLASS, job.getConfiguration(), fileBlockSize);
  }

  /**
   * Sets the size for index blocks within each file; smaller blocks means a deeper index hierarchy
   * within the file, while larger blocks mean a more shallow index hierarchy within the file. This
   * can affect the performance of queries.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param indexBlockSize
   *          the block size, in bytes
   * @since 1.5.0
   */
  public static void setIndexBlockSize(Job job, long indexBlockSize) {
    FileOutputConfigurator.setIndexBlockSize(CLASS, job.getConfiguration(), indexBlockSize);
  }

  /**
   * Sets the file system replication factor for the resulting file, overriding the file system
   * default.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param replication
   *          the number of replicas for produced files
   * @since 1.5.0
   */
  public static void setReplication(Job job, int replication) {
    FileOutputConfigurator.setReplication(CLASS, job.getConfiguration(), replication);
  }

  /**
   * Specify a sampler to be used when writing out data. This will result in the output file having
   * sample data.
   *
   * @param job
   *          The Hadoop job instance to be configured
   * @param samplerConfig
   *          The configuration for creating sample data in the output file.
   * @since 1.8.0
   */

  public static void setSampler(Job job, SamplerConfiguration samplerConfig) {
    FileOutputConfigurator.setSampler(CLASS, job.getConfiguration(), samplerConfig);
  }

  /**
   * Specifies a list of summarizer configurations to create summary data in the output file. Each
   * Key Value written will be passed to the configured {@link Summarizer}'s.
   *
   * @param job
   *          The Hadoop job instance to be configured
   * @param summarizerConfigs
   *          summarizer configurations
   * @since 2.0.0
   */
  public static void setSummarizers(Job job, SummarizerConfiguration... summarizerConfigs) {
    FileOutputConfigurator.setSummarizers(CLASS, job.getConfiguration(), summarizerConfigs);
  }
}
