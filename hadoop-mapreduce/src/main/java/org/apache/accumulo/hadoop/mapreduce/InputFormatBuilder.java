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

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Builder for all the information needed for the Map Reduce job. Fluent API used by
 * {@link AccumuloInputFormat#configure()}
 *
 * @since 2.0
 */
public interface InputFormatBuilder {
  /**
   * Required params for builder
   *
   * @since 2.0
   */
  interface ClientParams<T> {

    /**
     * Set client properties needed to communicate with Accumulo for this job. This information will
     * be serialized into the configuration. Therefore, it is more secure to use
     * {@link #clientPropertiesPath(String)}. Client properties can be created using
     * {@link Accumulo#newClientProperties()}
     *
     * @param clientProperties Accumulo connection information
     */
    TableParams<T> clientProperties(Properties clientProperties);

    /**
     * Set path to DFS location containing accumulo-client.properties file. This setting is more
     * secure than {@link #clientProperties(Properties)}
     *
     * @param clientPropsPath DFS path to accumulo-client.properties
     */
    TableParams<T> clientPropertiesPath(String clientPropsPath);
  }

  /**
   * Required params for builder
   *
   * @since 2.0
   */
  interface TableParams<T> {
    /**
     * Sets the name of the input table, over which this job will scan. At least one table is
     * required before calling store(Job)
     *
     * @param tableName the table to use when the tablename is null in the write call
     */
    InputFormatOptions<T> table(String tableName);

    /**
     * Finish configuring, verify and serialize options into the JobConf or Job
     */
    void store(T j) throws AccumuloException, AccumuloSecurityException;
  }

  /**
   * Optional values to set using fluent API
   *
   * @since 2.0
   */
  interface InputFormatOptions<T> extends TableParams<T> {
    /**
     * Sets the {@link Authorizations} used to scan. Must be a subset of the user's authorizations.
     * By Default, all of the users auths are set.
     *
     * @param auths the user's authorizations
     */
    InputFormatOptions<T> auths(Authorizations auths);

    /**
     * Sets the name of the classloader context on this scanner
     *
     * @param context name of the classloader context
     */
    InputFormatOptions<T> classLoaderContext(String context);

    /**
     * Sets the input ranges to scan for the single input table associated with this job.
     *
     * @param ranges the ranges that will be mapped over
     * @see TableOperations#splitRangeByTablets(String, Range, int)
     */
    InputFormatOptions<T> ranges(Collection<Range> ranges);

    /**
     * Restricts the columns that will be mapped over for this job for the default input table.
     *
     * @param fetchColumns a collection of IteratorSetting.Column objects corresponding to column
     *        family and column qualifier. If the column qualifier is null, the entire column family
     *        is selected. An empty set is the default and is equivalent to scanning all columns.
     */
    InputFormatOptions<T> fetchColumns(Collection<IteratorSetting.Column> fetchColumns);

    /**
     * Encode an iterator on the single input table for this job. It is safe to call this method
     * multiple times. If an iterator is added with the same name, it will be overridden.
     *
     * @param cfg the configuration of the iterator
     */
    InputFormatOptions<T> addIterator(IteratorSetting cfg);

    /**
     * Set these execution hints on scanners created for input splits. See
     * {@link ScannerBase#setExecutionHints(java.util.Map)}
     */
    InputFormatOptions<T> executionHints(Map<String,String> hints);

    /**
     * Causes input format to read sample data. If sample data was created using a different
     * configuration or a tables sampler configuration changes while reading data, then the input
     * format will throw an error.
     *
     * @param samplerConfig The sampler configuration that sample must have been created with
     *        inorder for reading sample data to succeed.
     *
     * @see ScannerBase#setSamplerConfiguration(SamplerConfiguration)
     */
    InputFormatOptions<T> samplerConfiguration(SamplerConfiguration samplerConfig);

    /**
     * Disables the automatic adjustment of ranges for this job. This feature merges overlapping
     * ranges, then splits them to align with tablet boundaries. Disabling this feature will cause
     * exactly one Map task to be created for each specified range. Disabling has no effect for
     * batch scans at it will always automatically adjust ranges.
     * <p>
     * By default, this feature is <b>enabled</b>.
     *
     * @see #ranges(Collection)
     */
    InputFormatOptions<T> autoAdjustRanges(boolean value);

    /**
     * Enables the use of the {@link IsolatedScanner} in this job.
     * <p>
     * By default, this feature is <b>disabled</b>.
     */
    InputFormatOptions<T> scanIsolation(boolean value);

    /**
     * Enables the use of the {@link ClientSideIteratorScanner} in this job. This feature will cause
     * the iterator stack to be constructed within the Map task, rather than within the Accumulo
     * TServer. To use this feature, all classes needed for those iterators must be available on the
     * classpath for the task.
     * <p>
     * By default, this feature is <b>disabled</b>.
     */
    InputFormatOptions<T> localIterators(boolean value);

    /**
     * Enable reading offline tables. By default, this feature is disabled and only online tables
     * are scanned. This will make the map reduce job directly read the table's files. If the table
     * is not offline, then the job will fail. If the table comes online during the map reduce job,
     * it is likely that the job will fail.
     * <p>
     * To use this option, the map reduce user will need access to read the Accumulo directory in
     * HDFS.
     * <p>
     * Reading the offline table will create the scan time iterator stack in the map process. So any
     * iterators that are configured for the table will need to be on the mapper's classpath.
     * <p>
     * One way to use this feature is to clone a table, take the clone offline, and use the clone as
     * the input table for a map reduce job. If you plan to map reduce over the data many times, it
     * may be better to the compact the table, clone it, take it offline, and use the clone for all
     * map reduce jobs. The reason to do this is that compaction will reduce each tablet in the
     * table to one file, and it is faster to read from one file.
     * <p>
     * There are two possible advantages to reading a tables file directly out of HDFS. First, you
     * may see better read performance. Second, it will support speculative execution better. When
     * reading an online table speculative execution can put more load on an already slow tablet
     * server.
     * <p>
     * By default, this feature is <b>disabled</b>.
     */
    InputFormatOptions<T> offlineScan(boolean value);

    /**
     * Enables the use of the {@link org.apache.accumulo.core.client.BatchScanner} in this job.
     * Using this feature will group Ranges by their source tablet, producing an InputSplit per
     * tablet rather than per Range. This batching helps to reduce overhead when querying a large
     * number of small ranges. (ex: when doing quad-tree decomposition for spatial queries)
     * <p>
     * In order to achieve good locality of InputSplits this option always clips the input Ranges to
     * tablet boundaries. This may result in one input Range contributing to several InputSplits.
     * <p>
     * Note: calls to {@link #autoAdjustRanges(boolean)} is ignored when BatchScan is enabled.
     * <p>
     * This configuration is incompatible with:
     * <ul>
     * <li>{@link #offlineScan(boolean)}</li>
     * <li>{@link #localIterators(boolean)}</li>
     * <li>{@link #scanIsolation(boolean)}</li>
     * </ul>
     * <p>
     * By default, this feature is <b>disabled</b>.
     */
    InputFormatOptions<T> batchScan(boolean value);

    /**
     * Enables the user to set the consistency level
     */
    InputFormatOptions<T> consistencyLevel(ConsistencyLevel level);
  }
}
