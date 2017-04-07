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
package org.apache.accumulo.core.client.mapred;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * This abstract {@link InputFormat} class allows MapReduce jobs to use Accumulo as the source of K,V pairs.
 * <p>
 * Subclasses must implement a {@link #getRecordReader(InputSplit, JobConf, Reporter)} to provide a {@link RecordReader} for K,V.
 * <p>
 * A static base class, RecordReaderBase, is provided to retrieve Accumulo {@link Key}/{@link Value} pairs, but one must implement its
 * {@link RecordReaderBase#next(Object, Object)} to transform them to the desired generic types K,V.
 * <p>
 * See {@link AccumuloInputFormat} for an example implementation.
 */
public abstract class InputFormatBase<K,V> extends AbstractInputFormat<K,V> {

  /**
   * Sets the name of the input table, over which this job will scan.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param tableName
   *          the table to use when the tablename is null in the write call
   * @since 1.5.0
   */
  public static void setInputTableName(JobConf job, String tableName) {
    InputConfigurator.setInputTableName(CLASS, job, tableName);
  }

  /**
   * Gets the table name from the configuration.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return the table name
   * @since 1.5.0
   * @see #setInputTableName(JobConf, String)
   */
  protected static String getInputTableName(JobConf job) {
    return InputConfigurator.getInputTableName(CLASS, job);
  }

  /**
   * Sets the input ranges to scan for this job. If not set, the entire table will be scanned.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param ranges
   *          the ranges that will be mapped over
   * @since 1.5.0
   */
  public static void setRanges(JobConf job, Collection<Range> ranges) {
    InputConfigurator.setRanges(CLASS, job, ranges);
  }

  /**
   * Gets the ranges to scan over from a job.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return the ranges
   * @throws IOException
   *           if the ranges have been encoded improperly
   * @since 1.5.0
   * @see #setRanges(JobConf, Collection)
   */
  protected static List<Range> getRanges(JobConf job) throws IOException {
    return InputConfigurator.getRanges(CLASS, job);
  }

  /**
   * Restricts the columns that will be mapped over for this job.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param columnFamilyColumnQualifierPairs
   *          a pair of {@link Text} objects corresponding to column family and column qualifier. If the column qualifier is null, the entire column family is
   *          selected. An empty set is the default and is equivalent to scanning the all columns.
   * @since 1.5.0
   */
  public static void fetchColumns(JobConf job, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    InputConfigurator.fetchColumns(CLASS, job, columnFamilyColumnQualifierPairs);
  }

  /**
   * Gets the columns to be mapped over from this job.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return a set of columns
   * @since 1.5.0
   * @see #fetchColumns(JobConf, Collection)
   */
  protected static Set<Pair<Text,Text>> getFetchedColumns(JobConf job) {
    return InputConfigurator.getFetchedColumns(CLASS, job);
  }

  /**
   * Encode an iterator on the input for this job.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param cfg
   *          the configuration of the iterator
   * @since 1.5.0
   */
  public static void addIterator(JobConf job, IteratorSetting cfg) {
    InputConfigurator.addIterator(CLASS, job, cfg);
  }

  /**
   * Gets a list of the iterator settings (for iterators to apply to a scanner) from this configuration.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return a list of iterators
   * @since 1.5.0
   * @see #addIterator(JobConf, IteratorSetting)
   */
  protected static List<IteratorSetting> getIterators(JobConf job) {
    return InputConfigurator.getIterators(CLASS, job);
  }

  /**
   * Controls the automatic adjustment of ranges for this job. This feature merges overlapping ranges, then splits them to align with tablet boundaries.
   * Disabling this feature will cause exactly one Map task to be created for each specified range. The default setting is enabled. *
   *
   * <p>
   * By default, this feature is <b>enabled</b>.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @see #setRanges(JobConf, Collection)
   * @since 1.5.0
   */
  public static void setAutoAdjustRanges(JobConf job, boolean enableFeature) {
    InputConfigurator.setAutoAdjustRanges(CLASS, job, enableFeature);
  }

  /**
   * Determines whether a configuration has auto-adjust ranges enabled. Must be enabled when {@link #setBatchScan(JobConf, boolean)} is true.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return false if the feature is disabled, true otherwise
   * @since 1.5.0
   * @see #setAutoAdjustRanges(JobConf, boolean)
   */
  protected static boolean getAutoAdjustRanges(JobConf job) {
    return InputConfigurator.getAutoAdjustRanges(CLASS, job);
  }

  /**
   * Controls the use of the {@link IsolatedScanner} in this job.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @since 1.5.0
   */
  public static void setScanIsolation(JobConf job, boolean enableFeature) {
    InputConfigurator.setScanIsolation(CLASS, job, enableFeature);
  }

  /**
   * Determines whether a configuration has isolation enabled.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return true if the feature is enabled, false otherwise
   * @since 1.5.0
   * @see #setScanIsolation(JobConf, boolean)
   */
  protected static boolean isIsolated(JobConf job) {
    return InputConfigurator.isIsolated(CLASS, job);
  }

  /**
   * Controls the use of the {@link ClientSideIteratorScanner} in this job. Enabling this feature will cause the iterator stack to be constructed within the Map
   * task, rather than within the Accumulo TServer. To use this feature, all classes needed for those iterators must be available on the classpath for the task.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @since 1.5.0
   */
  public static void setLocalIterators(JobConf job, boolean enableFeature) {
    InputConfigurator.setLocalIterators(CLASS, job, enableFeature);
  }

  /**
   * Determines whether a configuration uses local iterators.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return true if the feature is enabled, false otherwise
   * @since 1.5.0
   * @see #setLocalIterators(JobConf, boolean)
   */
  protected static boolean usesLocalIterators(JobConf job) {
    return InputConfigurator.usesLocalIterators(CLASS, job);
  }

  /**
   * Enable reading offline tables. By default, this feature is disabled and only online tables are scanned. This will make the map reduce job directly read the
   * table's files. If the table is not offline, then the job will fail. If the table comes online during the map reduce job, it is likely that the job will
   * fail.
   *
   * <p>
   * To use this option, the map reduce user will need access to read the Accumulo directory in HDFS.
   *
   * <p>
   * Reading the offline table will create the scan time iterator stack in the map process. So any iterators that are configured for the table will need to be
   * on the mapper's classpath.
   *
   * <p>
   * One way to use this feature is to clone a table, take the clone offline, and use the clone as the input table for a map reduce job. If you plan to map
   * reduce over the data many times, it may be better to the compact the table, clone it, take it offline, and use the clone for all map reduce jobs. The
   * reason to do this is that compaction will reduce each tablet in the table to one file, and it is faster to read from one file.
   *
   * <p>
   * There are two possible advantages to reading a tables file directly out of HDFS. First, you may see better read performance. Second, it will support
   * speculative execution better. When reading an online table speculative execution can put more load on an already slow tablet server.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @since 1.5.0
   */
  public static void setOfflineTableScan(JobConf job, boolean enableFeature) {
    InputConfigurator.setOfflineTableScan(CLASS, job, enableFeature);
  }

  /**
   * Determines whether a configuration has the offline table scan feature enabled.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return true if the feature is enabled, false otherwise
   * @since 1.5.0
   * @see #setOfflineTableScan(JobConf, boolean)
   */
  protected static boolean isOfflineScan(JobConf job) {
    return InputConfigurator.isOfflineScan(CLASS, job);
  }

  /**
   * Controls the use of the {@link org.apache.accumulo.core.client.BatchScanner} in this job. Using this feature will group Ranges by their source tablet,
   * producing an InputSplit per tablet rather than per Range. This batching helps to reduce overhead when querying a large number of small ranges. (ex: when
   * doing quad-tree decomposition for spatial queries)
   * <p>
   * In order to achieve good locality of InputSplits this option always clips the input Ranges to tablet boundaries. This may result in one input Range
   * contributing to several InputSplits.
   * <p>
   * Note: that the value of {@link #setAutoAdjustRanges(JobConf, boolean)} is ignored and is assumed to be true when BatchScan option is enabled.
   * <p>
   * This configuration is incompatible with:
   * <ul>
   * <li>{@link #setOfflineTableScan(JobConf, boolean)}</li>
   * <li>{@link #setLocalIterators(JobConf, boolean)}</li>
   * <li>{@link #setScanIsolation(JobConf, boolean)}</li>
   * </ul>
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @since 1.7.0
   */
  public static void setBatchScan(JobConf job, boolean enableFeature) {
    InputConfigurator.setBatchScan(CLASS, job, enableFeature);
  }

  /**
   * Determines whether a configuration has the {@link org.apache.accumulo.core.client.BatchScanner} feature enabled.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @since 1.7.0
   * @see #setBatchScan(JobConf, boolean)
   */
  public static boolean isBatchScan(JobConf job) {
    return InputConfigurator.isBatchScan(CLASS, job);
  }

  /**
   * Causes input format to read sample data. If sample data was created using a different configuration or a tables sampler configuration changes while reading
   * data, then the input format will throw an error.
   *
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param samplerConfig
   *          The sampler configuration that sample must have been created with inorder for reading sample data to succeed.
   *
   * @since 1.8.0
   * @see ScannerBase#setSamplerConfiguration(SamplerConfiguration)
   */
  public static void setSamplerConfiguration(JobConf job, SamplerConfiguration samplerConfig) {
    InputConfigurator.setSamplerConfiguration(CLASS, job, samplerConfig);
  }

  protected abstract static class RecordReaderBase<K,V> extends AbstractRecordReader<K,V> {

    @Override
    protected List<IteratorSetting> jobIterators(JobConf job, String tableName) {
      return getIterators(job);
    }

    /**
     * Apply the configured iterators to the scanner.
     *
     * @param iterators
     *          the iterators to set
     * @param scanner
     *          the scanner to configure
     * @deprecated since 1.7.0; Use {@link #jobIterators} instead.
     */
    @Deprecated
    protected void setupIterators(List<IteratorSetting> iterators, Scanner scanner) {
      for (IteratorSetting iterator : iterators) {
        scanner.addScanIterator(iterator);
      }
    }

    /**
     * Apply the configured iterators from the configuration to the scanner.
     *
     * @param job
     *          the job configuration
     * @param scanner
     *          the scanner to configure
     */
    @Deprecated
    protected void setupIterators(JobConf job, Scanner scanner) {
      setupIterators(getIterators(job), scanner);
    }
  }

  /**
   * @deprecated since 1.5.2; Use {@link org.apache.accumulo.core.client.mapred.RangeInputSplit} instead.
   * @see org.apache.accumulo.core.client.mapred.RangeInputSplit
   */
  @Deprecated
  public static class RangeInputSplit extends org.apache.accumulo.core.client.mapred.RangeInputSplit {
    public RangeInputSplit() {
      super();
    }

    public RangeInputSplit(RangeInputSplit other) throws IOException {
      super(other);
    }

    public RangeInputSplit(String table, String tableId, Range range, String[] locations) {
      super(table, tableId, range, locations);
    }

    protected RangeInputSplit(String table, Range range, String[] locations) {
      super(table, "", range, locations);
    }
  }
}
