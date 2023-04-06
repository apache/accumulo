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
package org.apache.accumulo.hadoop.mapreduce;

import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;

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
  interface ClientParams<T> {
    /**
     * Set the connection information needed to communicate with Accumulo in this job.
     * clientProperties param can be created using {@link Accumulo#newClientProperties()}. Client
     * properties will be serialized into configuration. Therefore it is more secure to use
     * {@link #clientPropertiesPath(String)}
     *
     * @param clientProperties Accumulo connection information
     */
    OutputOptions<T> clientProperties(Properties clientProperties);

    /**
     * Set path to DFS location containing accumulo-client.properties file. This setting is more
     * secure than {@link #clientProperties(Properties)}
     *
     * @param clientPropsPath DFS path to accumulo-client.properties
     */
    OutputOptions<T> clientPropertiesPath(String clientPropsPath);
  }

  /**
   * Builder options
   *
   * @since 2.0
   */
  interface OutputOptions<T> {
    /**
     * Sets the default table name to use if one emits a null in place of a table name for a given
     * mutation. Table names can only be alpha-numeric and underscores.
     *
     * @param tableName the table to use when the tablename is null in the write call
     */
    OutputOptions<T> defaultTable(String tableName);

    /**
     * Enables the directive to create new tables, as necessary. Table names can only be
     * alpha-numeric and underscores.
     * <p>
     * By default, this feature is <b>disabled</b>.
     */
    OutputOptions<T> createTables(boolean value);

    /**
     * Finish configuring, verify and serialize options into the Job or JobConf
     */
    void store(T j);
  }
}
