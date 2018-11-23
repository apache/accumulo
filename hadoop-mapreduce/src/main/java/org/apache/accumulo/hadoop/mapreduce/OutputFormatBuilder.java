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

import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

/**
 * Builder for all the information needed for the Map Reduce job. Fluent API used by
 * {@link AccumuloOutputFormat#configure()}
 *
 * @since 2.0
 */
public interface OutputFormatBuilder {

  /**
   * Required params for client
   *
   * @since 2.0
   */
  interface ClientParams {
    /**
     * Set the connection information needed to communicate with Accumulo in this job. ClientInfo
     * param can be created using {@link ClientInfo#from(Properties)}
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
     * {@link BatchWriterConfig}, with sensible built-in defaults is used. Setting the configuration
     * multiple times overwrites any previous configuration.
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
     * Finish configuring, verify and serialize options into the Job
     */
    void store(Job job);

    /**
     * Finish configuring, verify and serialize options into the JobConf
     */
    void store(JobConf jobConf);
  }

}
