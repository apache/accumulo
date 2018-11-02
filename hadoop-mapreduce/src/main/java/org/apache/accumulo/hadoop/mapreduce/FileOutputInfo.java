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
package org.apache.accumulo.hadoop.mapreduce;

import java.util.Collection;
import java.util.Optional;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.hadoopImpl.mapreduce.FileOutputInfoImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * Object containing all the information needed for the Map Reduce job. This object is passed to
 * {@link AccumuloFileOutputFormat#setInfo(Job, FileOutputInfo)}. It uses a fluent API like so:
 *
 * <pre>
 * FileOutputInfo.builder()
 *      .outputPath(path)
 *      .fileBlockSize(b)
 *      .compressionType(type)
 *      .summarizers(sc1, sc2).build());
 * </pre>
 *
 * @since 2.0
 */
public interface FileOutputInfo {

  /**
   * @return the output path set using FileOutputInfo.builder()...outputPath(path)
   */
  public Path getOutputPath();

  /**
   * @return the compression if set using FileOutputInfo.builder()...compressionType(type)
   */
  public Optional<String> getCompressionType();

  /**
   * @return the data block size if set using FileOutputInfo.builder()...dataBlockSize(size)
   */
  public Optional<Long> getDataBlockSize();

  /**
   * @return the file block size if set using FileOutputInfo.builder()...fileBlockSize(size)
   */
  public Optional<Long> getFileBlockSize();

  /**
   * @return the index block size if set using FileOutputInfo.builder()...indexBlockSize(size)
   */
  public Optional<Long> getIndexBlockSize();

  /**
   * @return the replication if set using FileOutputInfo.builder()...replication(num)
   */
  public Optional<Integer> getReplication();

  /**
   * @return the SamplerConfiguration if set using FileOutputInfo.builder()...sampler(conf)
   */
  public Optional<SamplerConfiguration> getSampler();

  /**
   * @return the summarizers set using FileOutputInfo.builder()...summarizers(conf1, conf2...)
   */
  public Collection<SummarizerConfiguration> getSummarizers();

  /**
   * @return builder for creating a {@link FileOutputInfo}
   */
  public static FileOutputInfoBuilder.PathParams builder() {
    return new FileOutputInfoImpl.FileOutputInfoBuilderImpl();
  }

  /**
   * Fluent API builder for FileOutputInfo
   *
   * @since 2.0
   */
  interface FileOutputInfoBuilder {

    /**
     * Required params for builder
     *
     * @since 2.0
     */
    interface PathParams {
      /**
       * Set the Path of the output directory for the map-reduce job.
       */
      OutputOptions outputPath(Path path);
    }

    /**
     * Options for builder
     *
     * @since 2.0
     */
    interface OutputOptions {
      /**
       * Sets the compression type to use for data blocks, overriding the default. Specifying a
       * compression may require additional libraries to be available to your Job.
       *
       * @param compressionType
       *          one of "none", "gz", "lzo", or "snappy"
       */
      OutputOptions compressionType(String compressionType);

      /**
       * Sets the size for data blocks within each file.<br>
       * Data blocks are a span of key/value pairs stored in the file that are compressed and
       * indexed as a group.
       *
       * <p>
       * Making this value smaller may increase seek performance, but at the cost of increasing the
       * size of the indexes (which can also affect seek performance).
       *
       * @param dataBlockSize
       *          the block size, in bytes
       */
      OutputOptions dataBlockSize(long dataBlockSize);

      /**
       * Sets the size for file blocks in the file system; file blocks are managed, and replicated,
       * by the underlying file system.
       *
       * @param fileBlockSize
       *          the block size, in bytes
       */
      OutputOptions fileBlockSize(long fileBlockSize);

      /**
       * Sets the size for index blocks within each file; smaller blocks means a deeper index
       * hierarchy within the file, while larger blocks mean a more shallow index hierarchy within
       * the file. This can affect the performance of queries.
       *
       * @param indexBlockSize
       *          the block size, in bytes
       */
      OutputOptions indexBlockSize(long indexBlockSize);

      /**
       * Sets the file system replication factor for the resulting file, overriding the file system
       * default.
       *
       * @param replication
       *          the number of replicas for produced files
       */
      OutputOptions replication(int replication);

      /**
       * Specify a sampler to be used when writing out data. This will result in the output file
       * having sample data.
       *
       * @param samplerConfig
       *          The configuration for creating sample data in the output file.
       */
      OutputOptions sampler(SamplerConfiguration samplerConfig);

      /**
       * Specifies a list of summarizer configurations to create summary data in the output file.
       * Each Key Value written will be passed to the configured
       * {@link org.apache.accumulo.core.client.summary.Summarizer}'s.
       *
       * @param summarizerConfigs
       *          summarizer configurations
       */
      OutputOptions summarizers(SummarizerConfiguration... summarizerConfigs);

      /**
       * @return newly created {@link FileOutputInfo}
       */
      FileOutputInfo build();
    }
  }
}
