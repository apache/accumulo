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

import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.hadoopImpl.mapreduce.OutputInfoImpl;
import org.apache.hadoop.mapreduce.Job;

/**
 * Object containing all the information needed for the Map Reduce job. This object is passed to
 * {@link AccumuloOutputFormat#setInfo(Job, OutputInfo)}. It uses a fluent API like so:
 *
 * <pre>
 * OutputInfo.builder().clientInfo(clientInfo).batchWriterOptions(bwConfig).build();
 * </pre>
 *
 * @since 2.0
 */
public interface OutputInfo {
  /**
   * @return the client info set using OutputInfo.builder().clientInfo(info)
   */
  ClientInfo getClientInfo();

  /**
   * @return the BatchWriterConfig set using OutputInfo.builder()...batchWriterOptions(conf)
   */
  Optional<BatchWriterConfig> getBatchWriterOptions();

  /**
   * @return the default tame name set using OutputInfo.builder()...defaultTableName(name)
   */
  Optional<String> getDefaultTableName();

  /**
   * @return boolean if creating tables or not
   */
  boolean isCreateTables();

  /**
   * @return boolean if running simulation mode or not
   */
  boolean isSimulationMode();

  /**
   * @return builder for creating a {@link OutputInfo}
   */
  public static OutputInfoBuilder.ClientParams builder() {
    return new OutputInfoImpl.OutputInfoBuilderImpl();
  }

  /**
   * Fluent API builder for OutputInfo
   *
   * @since 2.0
   */
  interface OutputInfoBuilder {

    /**
     * Required params for client
     *
     * @since 2.0
     */
    interface ClientParams {
      /**
       * Set the connection information needed to communicate with Accumulo in this job. ClientInfo
       * param can be created using {@link ClientInfo#from(Path)} or
       * {@link ClientInfo#from(Properties)}
       *
       * @param clientInfo
       *          Accumulo connection information
       */
      OutputOptions clientInfo(ClientInfo clientInfo);
    }

    /**
     * Builder options
     *
     * @since 2.0
     */
    interface OutputOptions {
      /**
       * Sets the configuration for for the job's {@link BatchWriter} instances. If not set, a new
       * {@link BatchWriterConfig}, with sensible built-in defaults is used. Setting the
       * configuration multiple times overwrites any previous configuration.
       *
       * @param bwConfig
       *          the configuration for the {@link BatchWriter}
       */
      OutputOptions batchWriterOptions(BatchWriterConfig bwConfig);

      /**
       * Sets the default table name to use if one emits a null in place of a table name for a given
       * mutation. Table names can only be alpha-numeric and underscores.
       *
       * @param tableName
       *          the table to use when the tablename is null in the write call
       */
      OutputOptions defaultTableName(String tableName);

      /**
       * Enables the directive to create new tables, as necessary. Table names can only be
       * alpha-numeric and underscores.
       * <p>
       * By default, this feature is <b>disabled</b>.
       */
      OutputOptions enableCreateTables();

      /**
       * Enables the directive to use simulation mode for this job. In simulation mode, no output is
       * produced. This is useful for testing.
       * <p>
       * By default, this feature is <b>disabled</b>.
       */
      OutputOptions enableSimulationMode();

      /**
       * @return newly created {@link OutputInfo}
       */
      OutputInfo build();
    }
  }
}
