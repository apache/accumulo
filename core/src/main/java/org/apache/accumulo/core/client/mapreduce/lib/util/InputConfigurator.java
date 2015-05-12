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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
 * @since 1.5.0
 */
@Deprecated
public class InputConfigurator extends ConfiguratorBase {

  /**
   * Configuration keys for {@link Scanner}.
   *
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static enum ScanOpts {
    TABLE_NAME, AUTHORIZATIONS, RANGES, COLUMNS, ITERATORS
  }

  /**
   * Configuration keys for various features.
   *
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static enum Features {
    AUTO_ADJUST_RANGES, SCAN_ISOLATION, USE_LOCAL_ITERATORS, SCAN_OFFLINE
  }

  /**
   * Sets the name of the input table, over which this job will scan.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param tableName
   *          the table to use when the tablename is null in the write call
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setInputTableName(Class<?> implementingClass, Configuration conf, String tableName) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.setInputTableName(implementingClass, conf, tableName);
  }

  /**
   * Gets the table name from the configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the table name
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setInputTableName(Class, Configuration, String)
   */
  @Deprecated
  public static String getInputTableName(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.getInputTableName(implementingClass, conf);
  }

  /**
   * Sets the {@link Authorizations} used to scan. Must be a subset of the user's authorization. Defaults to the empty set.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param auths
   *          the user's authorizations
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setScanAuthorizations(Class<?> implementingClass, Configuration conf, Authorizations auths) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.setScanAuthorizations(implementingClass, conf, auths);
  }

  /**
   * Gets the authorizations to set for the scans from the configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the Accumulo scan authorizations
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setScanAuthorizations(Class, Configuration, Authorizations)
   */
  @Deprecated
  public static Authorizations getScanAuthorizations(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.getScanAuthorizations(implementingClass, conf);
  }

  /**
   * Sets the input ranges to scan for this job. If not set, the entire table will be scanned.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param ranges
   *          the ranges that will be mapped over
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setRanges(Class<?> implementingClass, Configuration conf, Collection<Range> ranges) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.setRanges(implementingClass, conf, ranges);
  }

  /**
   * Gets the ranges to scan over from a job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the ranges
   * @throws IOException
   *           if the ranges have been encoded improperly
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setRanges(Class, Configuration, Collection)
   */
  @Deprecated
  public static List<Range> getRanges(Class<?> implementingClass, Configuration conf) throws IOException {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.getRanges(implementingClass, conf);
  }

  /**
   * Restricts the columns that will be mapped over for this job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param columnFamilyColumnQualifierPairs
   *          a pair of {@link Text} objects corresponding to column family and column qualifier. If the column qualifier is null, the entire column family is
   *          selected. An empty set is the default and is equivalent to scanning the all columns.
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void fetchColumns(Class<?> implementingClass, Configuration conf, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.fetchColumns(implementingClass, conf, columnFamilyColumnQualifierPairs);
  }

  /**
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   */
  @Deprecated
  public static String[] serializeColumns(Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.serializeColumns(columnFamilyColumnQualifierPairs);
  }

  /**
   * Gets the columns to be mapped over from this job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return a set of columns
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #fetchColumns(Class, Configuration, Collection)
   */
  @Deprecated
  public static Set<Pair<Text,Text>> getFetchedColumns(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.getFetchedColumns(implementingClass, conf);
  }

  /**
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   */
  @Deprecated
  public static Set<Pair<Text,Text>> deserializeFetchedColumns(Collection<String> serialized) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.deserializeFetchedColumns(serialized);
  }

  /**
   * Encode an iterator on the input for this job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param cfg
   *          the configuration of the iterator
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void addIterator(Class<?> implementingClass, Configuration conf, IteratorSetting cfg) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.addIterator(implementingClass, conf, cfg);
  }

  /**
   * Gets a list of the iterator settings (for iterators to apply to a scanner) from this configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return a list of iterators
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #addIterator(Class, Configuration, IteratorSetting)
   */
  @Deprecated
  public static List<IteratorSetting> getIterators(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.getIterators(implementingClass, conf);
  }

  /**
   * Controls the automatic adjustment of ranges for this job. This feature merges overlapping ranges, then splits them to align with tablet boundaries.
   * Disabling this feature will cause exactly one Map task to be created for each specified range. The default setting is enabled. *
   *
   * <p>
   * By default, this feature is <b>enabled</b>.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @see #setRanges(Class, Configuration, Collection)
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setAutoAdjustRanges(Class<?> implementingClass, Configuration conf, boolean enableFeature) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.setAutoAdjustRanges(implementingClass, conf, enableFeature);
  }

  /**
   * Determines whether a configuration has auto-adjust ranges enabled.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return false if the feature is disabled, true otherwise
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setAutoAdjustRanges(Class, Configuration, boolean)
   */
  @Deprecated
  public static Boolean getAutoAdjustRanges(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.getAutoAdjustRanges(implementingClass, conf);
  }

  /**
   * Controls the use of the {@link IsolatedScanner} in this job.
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
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setScanIsolation(Class<?> implementingClass, Configuration conf, boolean enableFeature) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.setScanIsolation(implementingClass, conf, enableFeature);
  }

  /**
   * Determines whether a configuration has isolation enabled.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setScanIsolation(Class, Configuration, boolean)
   */
  @Deprecated
  public static Boolean isIsolated(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.isIsolated(implementingClass, conf);
  }

  /**
   * Controls the use of the {@link ClientSideIteratorScanner} in this job. Enabling this feature will cause the iterator stack to be constructed within the Map
   * task, rather than within the Accumulo TServer. To use this feature, all classes needed for those iterators must be available on the classpath for the task.
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
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setLocalIterators(Class<?> implementingClass, Configuration conf, boolean enableFeature) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.setLocalIterators(implementingClass, conf, enableFeature);
  }

  /**
   * Determines whether a configuration uses local iterators.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setLocalIterators(Class, Configuration, boolean)
   */
  @Deprecated
  public static Boolean usesLocalIterators(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.usesLocalIterators(implementingClass, conf);
  }

  /**
   * <p>
   * Enable reading offline tables. By default, this feature is disabled and only online tables are scanned. This will make the map reduce job directly read the
   * table's files. If the table is not offline, then the job will fail. If the table comes online during the map reduce job, it is likely that the job will
   * fail.
   *
   * <p>
   * To use this option, the map reduce user will need access to read the Accumulo directory in HDFS.
   *
   * <p>
   * Reading the offline table will create the scan time iterator stack in the map process. So any iterators that are configured for the table will need to be
   * on the mapper's classpath. The accumulo-site.xml may need to be on the mapper's classpath if HDFS or the Accumulo directory in HDFS are non-standard.
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
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void setOfflineTableScan(Class<?> implementingClass, Configuration conf, boolean enableFeature) {
    org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.setOfflineTableScan(implementingClass, conf, enableFeature);
  }

  /**
   * Determines whether a configuration has the offline table scan feature enabled.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   * @see #setOfflineTableScan(Class, Configuration, boolean)
   */
  @Deprecated
  public static Boolean isOfflineScan(Class<?> implementingClass, Configuration conf) {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.isOfflineScan(implementingClass, conf);
  }

  /**
   * Initializes an Accumulo {@link TabletLocator} based on the configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return an Accumulo tablet locator
   * @throws TableNotFoundException
   *           if the table name set on the configuration doesn't exist
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static TabletLocator getTabletLocator(Class<?> implementingClass, Configuration conf) throws TableNotFoundException {
    return org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.getTabletLocator(implementingClass, conf,
        Tables.getTableId(getInstance(implementingClass, conf), getInputTableName(implementingClass, conf)));
  }

  // InputFormat doesn't have the equivalent of OutputFormat's checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @throws IOException
   *           if the context is improperly configured
   * @deprecated since 1.6.0; Configure your job with the appropriate InputFormat or OutputFormat.
   * @since 1.5.0
   */
  @Deprecated
  public static void validateOptions(Class<?> implementingClass, Configuration conf) throws IOException {
    org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator.validateOptions(implementingClass, conf);
  }

}
