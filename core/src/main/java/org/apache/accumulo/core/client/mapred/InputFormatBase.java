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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.OfflineScanner;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mapreduce.BatchScanConfig;
import org.apache.accumulo.core.client.mapreduce.lib.util.InputConfigurator;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.AuthenticationTokenSerializer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

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
public abstract class InputFormatBase<K,V> implements InputFormat<K,V> {
  
  private static final Class<?> CLASS = AccumuloInputFormat.class;
  protected static final Logger log = Logger.getLogger(CLASS);
  
  /**
   * Sets the connector information needed to communicate with Accumulo in this job.
   * 
   * <p>
   * <b>WARNING:</b> The serialized token is stored in the configuration and shared with all MapReduce tasks. It is BASE64 encoded to provide a charset safe
   * conversion to a string, and is not intended to be secure.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param principal
   *          a valid Accumulo user name (user must have Table.CREATE permission)
   * @param token
   *          the user's password
   * @throws AccumuloSecurityException
   * @since 1.5.0
   */
  public static void setConnectorInfo(JobConf job, String principal, AuthenticationToken token) throws AccumuloSecurityException {
    InputConfigurator.setConnectorInfo(CLASS, job, principal, token);
  }
  
  /**
   * Sets the connector information needed to communicate with Accumulo in this job.
   * 
   * <p>
   * Stores the password in a file in HDFS and pulls that into the Distributed Cache in an attempt to be more secure than storing it in the Configuration.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param principal
   *          a valid Accumulo user name (user must have Table.CREATE permission)
   * @param tokenFile
   *          the path to the token file
   * @throws AccumuloSecurityException
   * @since 1.6.0
   */
  public static void setConnectorInfo(JobConf job, String principal, String tokenFile) throws AccumuloSecurityException {
    InputConfigurator.setConnectorInfo(CLASS, job, principal, tokenFile);
  }
  
  /**
   * Determines if the connector has been configured.
   * 
   * @param job
   *          the Hadoop context for the configured job
   * @return true if the connector has been configured, false otherwise
   * @since 1.5.0
   * @see #setConnectorInfo(JobConf, String, AuthenticationToken)
   */
  protected static Boolean isConnectorInfoSet(JobConf job) {
    return InputConfigurator.isConnectorInfoSet(CLASS, job);
  }
  
  /**
   * Gets the user name from the configuration.
   * 
   * @param job
   *          the Hadoop context for the configured job
   * @return the user name
   * @since 1.5.0
   * @see #setConnectorInfo(JobConf, String, AuthenticationToken)
   */
  protected static String getPrincipal(JobConf job) {
    return InputConfigurator.getPrincipal(CLASS, job);
  }
  
  /**
   * Gets the serialized token class from either the configuration or the token file.
   * 
   * @since 1.5.0
   * @deprecated since 1.6.0; Use {@link #getAuthenticationToken(JobConf)} instead.
   */
  @Deprecated
  protected static String getTokenClass(JobConf job) {
    return getAuthenticationToken(job).getClass().getName();
  }
  
  /**
   * Gets the serialized token from either the configuration or the token file.
   * 
   * @since 1.5.0
   * @deprecated since 1.6.0; Use {@link #getAuthenticationToken(JobConf)} instead.
   */
  @Deprecated
  protected static byte[] getToken(JobConf job) {
    return AuthenticationTokenSerializer.serialize(getAuthenticationToken(job));
  }
  
  /**
   * Gets the authenticated token from either the specified token file or directly from the configuration, whichever was used when the job was configured.
   * 
   * @param job
   *          the Hadoop context for the configured job
   * @return the principal's authentication token
   * @since 1.6.0
   * @see #setConnectorInfo(JobConf, String, AuthenticationToken)
   * @see #setConnectorInfo(JobConf, String, String)
   */
  protected static AuthenticationToken getAuthenticationToken(JobConf job) {
    return InputConfigurator.getAuthenticationToken(CLASS, job);
  }
  
  /**
   * Configures a {@link ZooKeeperInstance} for this job.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param instanceName
   *          the Accumulo instance name
   * @param zooKeepers
   *          a comma-separated list of zookeeper servers
   * @since 1.5.0
   */
  public static void setZooKeeperInstance(JobConf job, String instanceName, String zooKeepers) {
    InputConfigurator.setZooKeeperInstance(CLASS, job, instanceName, zooKeepers);
  }
  
  /**
   * Configures a {@link MockInstance} for this job.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param instanceName
   *          the Accumulo instance name
   * @since 1.5.0
   */
  public static void setMockInstance(JobConf job, String instanceName) {
    InputConfigurator.setMockInstance(CLASS, job, instanceName);
  }
  
  /**
   * Initializes an Accumulo {@link Instance} based on the configuration.
   * 
   * @param job
   *          the Hadoop context for the configured job
   * @return an Accumulo instance
   * @since 1.5.0
   * @see #setZooKeeperInstance(JobConf, String, String)
   * @see #setMockInstance(JobConf, String)
   */
  protected static Instance getInstance(JobConf job) {
    return InputConfigurator.getInstance(CLASS, job);
  }
  
  /**
   * Sets the log level for this job.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param level
   *          the logging level
   * @since 1.5.0
   */
  public static void setLogLevel(JobConf job, Level level) {
    InputConfigurator.setLogLevel(CLASS, job, level);
  }
  
  /**
   * Gets the log level from this configuration.
   * 
   * @param job
   *          the Hadoop context for the configured job
   * @return the log level
   * @since 1.5.0
   * @see #setLogLevel(JobConf, Level)
   */
  protected static Level getLogLevel(JobConf job) {
    return InputConfigurator.getLogLevel(CLASS, job);
  }
  
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
   * Sets the {@link Authorizations} used to scan. Must be a subset of the user's authorization. Defaults to the empty set.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param auths
   *          the user's authorizations
   * @since 1.5.0
   */
  public static void setScanAuthorizations(JobConf job, Authorizations auths) {
    InputConfigurator.setScanAuthorizations(CLASS, job, auths);
  }
  
  /**
   * Gets the authorizations to set for the scans from the configuration.
   * 
   * @param job
   *          the Hadoop context for the configured job
   * @return the Accumulo scan authorizations
   * @since 1.5.0
   * @see #setScanAuthorizations(JobConf, Authorizations)
   */
  protected static Authorizations getScanAuthorizations(JobConf job) {
    return InputConfigurator.getScanAuthorizations(CLASS, job);
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
   * Determines whether a configuration has auto-adjust ranges enabled.
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
   * Sets the {@link org.apache.accumulo.core.client.mapreduce.BatchScanConfig} objects on the given Hadoop configuration
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param configs
   *          the table query configs to be set on the configuration.
   * @since 1.6.0
   */
  public static void setTableQueryConfigs(JobConf job, BatchScanConfig... configs) {
    checkNotNull(configs);
    InputConfigurator.setTableQueryConfigs(CLASS, job, configs);
  }
  
  /**
   * Fetches all {@link org.apache.accumulo.core.client.mapreduce.BatchScanConfig}s that have been set on the given Hadoop configuration.
   * 
   * <p>
   * Note this also returns the {@link org.apache.accumulo.core.client.mapreduce.BatchScanConfig} representing the table configurations set through the single table input methods (
   * {@link #setInputTableName(JobConf, String)}, {@link #setRanges(JobConf, java.util.Collection)}, {@link #fetchColumns(JobConf, java.util.Collection)},
   * {@link #addIterator(JobConf, IteratorSetting)}, etc...)
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @return
   * @since 1.6.0
   */
  public static List<BatchScanConfig> getTableQueryConfigs(JobConf job) {
    return InputConfigurator.getTableQueryConfigs(CLASS, job);
  }
  
  /**
   * Fetches a {@link org.apache.accumulo.core.client.mapreduce.BatchScanConfig} that has been set on the configuration for a specific table.
   * 
   * <p>
   * null is returned in the event that the table doesn't exist.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param tableName
   *          the table name for which to grab the config object
   * @return the {@link org.apache.accumulo.core.client.mapreduce.BatchScanConfig} for the given table
   * @since 1.6.0
   */
  public static BatchScanConfig getTableQueryConfig(JobConf job, String tableName) {
    return InputConfigurator.getTableQueryConfig(CLASS, job, tableName);
  }
  
  /**
   * Initializes an Accumulo {@link TabletLocator} based on the configuration.
   * 
   * @param job
   *          the Hadoop context for the configured job
   * @return an Accumulo tablet locator
   * @throws TableNotFoundException
   *           if the table name set on the configuration doesn't exist
   * @since 1.5.0
   */
  protected static TabletLocator getTabletLocator(JobConf job, String tableName) throws TableNotFoundException {
    return InputConfigurator.getTabletLocator(CLASS, job, tableName);
  }
  
  // InputFormat doesn't have the equivalent of OutputFormat's checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo {@link InputFormat}.
   * 
   * @param job
   *          the Hadoop context for the configured job
   * @throws IOException
   *           if the context is improperly configured
   * @since 1.5.0
   */
  protected static void validateOptions(JobConf job) throws IOException {
    InputConfigurator.validateOptions(CLASS, job);
  }
  
  /**
   * An abstract base class to be used to create {@link RecordReader} instances that convert from Accumulo {@link Key}/{@link Value} pairs to the user's K/V
   * types.
   * 
   * Subclasses must implement {@link #next(Object, Object)} to update key and value, and also to update the following variables:
   * <ul>
   * <li>Key {@link #currentKey} (used for progress reporting)</li>
   * <li>int {@link #numKeysRead} (used for progress reporting)</li>
   * </ul>
   */
  protected abstract static class RecordReaderBase<K,V> implements RecordReader<K,V> {
    protected long numKeysRead;
    protected Iterator<Entry<Key,Value>> scannerIterator;
    protected RangeInputSplit split;
    
    /**
     * Apply the configured iterators from the configuration to the scanner.
     * 
     * @param job
     *          the Hadoop context for the configured job
     * @param scanner
     *          the scanner to configure
     */
    protected void setupIterators(JobConf job, Scanner scanner, String tableName) {
      BatchScanConfig config = getTableQueryConfig(job, tableName);
      List<IteratorSetting> iterators = config.getIterators();
      for (IteratorSetting iterator : iterators)
        scanner.addScanIterator(iterator);
    }
    
    /**
     * Initialize a scanner over the given input split using this task attempt configuration.
     */
    public void initialize(InputSplit inSplit, JobConf job) throws IOException {
      Scanner scanner;
      split = (RangeInputSplit) inSplit;
      log.debug("Initializing input split: " + split.getRange());
      Instance instance = getInstance(job);
      String user = getPrincipal(job);
      AuthenticationToken token = getAuthenticationToken(job);
      Authorizations authorizations = getScanAuthorizations(job);
      
      BatchScanConfig tableConfig = getTableQueryConfig(job, split.getTableName());
      
      // in case the table name changed, we can still use the previous name for terms of configuration,
      // but for the scanner, we'll need to reference the new table name.
      String actualNameForId = split.getTableName();
      if (!(instance instanceof MockInstance)) {
        try {
          actualNameForId = Tables.getTableName(instance, split.getTableId());
          if (!actualNameForId.equals(split.getTableName()))
            log.debug("Table name changed from " + split.getTableName() + " to " + actualNameForId);
        } catch (TableNotFoundException e) {
          throw new IOException("The specified table was not found for id=" + split.getTableId());
        }
      }
      
      try {
        log.debug("Creating connector with user: " + user);
        Connector conn = instance.getConnector(user, token);
        log.debug("Creating scanner for table: " + getInputTableName(job));
        log.debug("Authorizations are: " + authorizations);
        if (tableConfig.isOfflineScan()) {
          scanner = new OfflineScanner(instance, new Credentials(user, token), split.getTableId(), authorizations);
        } else {
          scanner = conn.createScanner(actualNameForId, authorizations);
        }
        if (tableConfig.shouldUseIsolatedScanners()) {
          log.info("Creating isolated scanner");
          scanner = new IsolatedScanner(scanner);
        }
        if (tableConfig.shouldUseLocalIterators()) {
          log.info("Using local iterators");
          scanner = new ClientSideIteratorScanner(scanner);
        }
        setupIterators(job, scanner, split.getTableName());
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      // setup a scanner within the bounds of this split
      for (Pair<Text,Text> c : tableConfig.getFetchedColumns()) {
        if (c.getSecond() != null) {
          log.debug("Fetching column " + c.getFirst() + ":" + c.getSecond());
          scanner.fetchColumn(c.getFirst(), c.getSecond());
        } else {
          log.debug("Fetching column family " + c.getFirst());
          scanner.fetchColumnFamily(c.getFirst());
        }
      }
      
      scanner.setRange(split.getRange());
      
      numKeysRead = 0;
      
      // do this last after setting all scanner options
      scannerIterator = scanner.iterator();
    }
    
    @Override
    public void close() {}
    
    @Override
    public long getPos() throws IOException {
      return numKeysRead;
    }
    
    @Override
    public float getProgress() throws IOException {
      if (numKeysRead > 0 && currentKey == null)
        return 1.0f;
      return split.getProgress(currentKey);
    }
    
    protected Key currentKey = null;
    
  }
  
  Map<String,Map<KeyExtent,List<Range>>> binOfflineTable(JobConf job, String tableName, List<Range> ranges) throws TableNotFoundException, AccumuloException,
      AccumuloSecurityException {
    
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    
    Instance instance = getInstance(job);
    Connector conn = instance.getConnector(getPrincipal(job), getAuthenticationToken(job));
    String tableId = Tables.getTableId(instance, tableName);
    
    if (Tables.getTableState(instance, tableId) != TableState.OFFLINE) {
      Tables.clearCache(instance);
      if (Tables.getTableState(instance, tableId) != TableState.OFFLINE) {
        throw new AccumuloException("Table is online " + tableName + "(" + tableId + ") cannot scan table in offline mode ");
      }
    }
    
    for (Range range : ranges) {
      Text startRow;
      
      if (range.getStartKey() != null)
        startRow = range.getStartKey().getRow();
      else
        startRow = new Text();
      
      Range metadataRange = new Range(new KeyExtent(new Text(tableId), startRow, null).getMetadataEntry(), true, null, false);
      Scanner scanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(TabletsSection.LastLocationColumnFamily.NAME);
      scanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
      scanner.fetchColumnFamily(TabletsSection.FutureLocationColumnFamily.NAME);
      scanner.setRange(metadataRange);
      
      RowIterator rowIter = new RowIterator(scanner);
      
      KeyExtent lastExtent = null;
      
      while (rowIter.hasNext()) {
        Iterator<Entry<Key,Value>> row = rowIter.next();
        String last = "";
        KeyExtent extent = null;
        String location = null;
        
        while (row.hasNext()) {
          Entry<Key,Value> entry = row.next();
          Key key = entry.getKey();
          
          if (key.getColumnFamily().equals(TabletsSection.LastLocationColumnFamily.NAME)) {
            last = entry.getValue().toString();
          }
          
          if (key.getColumnFamily().equals(TabletsSection.CurrentLocationColumnFamily.NAME)
              || key.getColumnFamily().equals(TabletsSection.FutureLocationColumnFamily.NAME)) {
            location = entry.getValue().toString();
          }
          
          if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)) {
            extent = new KeyExtent(key.getRow(), entry.getValue());
          }
          
        }
        
        if (location != null)
          return null;
        
        if (!extent.getTableId().toString().equals(tableId)) {
          throw new AccumuloException("Saw unexpected table Id " + tableId + " " + extent);
        }
        
        if (lastExtent != null && !extent.isPreviousExtent(lastExtent)) {
          throw new AccumuloException(" " + lastExtent + " is not previous extent " + extent);
        }
        
        Map<KeyExtent,List<Range>> tabletRanges = binnedRanges.get(last);
        if (tabletRanges == null) {
          tabletRanges = new HashMap<KeyExtent,List<Range>>();
          binnedRanges.put(last, tabletRanges);
        }
        
        List<Range> rangeList = tabletRanges.get(extent);
        if (rangeList == null) {
          rangeList = new ArrayList<Range>();
          tabletRanges.put(extent, rangeList);
        }
        
        rangeList.add(range);
        
        if (extent.getEndRow() == null || range.afterEndKey(new Key(extent.getEndRow()).followingKey(PartialKey.ROW))) {
          break;
        }
        
        lastExtent = extent;
      }
      
    }
    
    return binnedRanges;
  }
  
  /**
   * Read the metadata table to get tablets and match up ranges to them.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    log.setLevel(getLogLevel(job));
    validateOptions(job);
    
    LinkedList<InputSplit> splits = new LinkedList<InputSplit>();
    List<BatchScanConfig> tableConfigs = getTableQueryConfigs(job);
    for (BatchScanConfig tableConfig : tableConfigs) {
      
      boolean autoAdjust = tableConfig.shouldAutoAdjustRanges();
      String tableId = null;
      List<Range> ranges = autoAdjust ? Range.mergeOverlapping(tableConfig.getRanges()) : tableConfig.getRanges();
      if (ranges.isEmpty()) {
        ranges = new ArrayList<Range>(1);
        ranges.add(new Range());
      }
      
      // get the metadata information for these ranges
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
      TabletLocator tl;
      try {
        if (tableConfig.isOfflineScan()) {
          binnedRanges = binOfflineTable(job, tableConfig.getTableName(), ranges);
          while (binnedRanges == null) {
            // Some tablets were still online, try again
            UtilWaitThread.sleep(100 + (int) (Math.random() * 100)); // sleep randomly between 100 and 200 ms
            binnedRanges = binOfflineTable(job, tableConfig.getTableName(), ranges);
          }
        } else {
          Instance instance = getInstance(job);
          tl = getTabletLocator(job, tableConfig.getTableName());
          // its possible that the cache could contain complete, but old information about a tables tablets... so clear it
          tl.invalidateCache();
          Credentials creds = new Credentials(getPrincipal(job), getAuthenticationToken(job));
          
          while (!tl.binRanges(creds, ranges, binnedRanges).isEmpty()) {
            if (!(instance instanceof MockInstance)) {
              if (!Tables.exists(instance, tableId))
                throw new TableDeletedException(tableId);
              if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
                throw new TableOfflineException(instance, tableId);
              tableId = Tables.getTableId(instance, tableConfig.getTableName());
            }
            binnedRanges.clear();
            log.warn("Unable to locate bins for specified ranges. Retrying.");
            UtilWaitThread.sleep(100 + (int) (Math.random() * 100)); // sleep randomly between 100 and 200 ms
            tl.invalidateCache();
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      HashMap<Range,ArrayList<String>> splitsToAdd = null;
      
      if (!autoAdjust)
        splitsToAdd = new HashMap<Range,ArrayList<String>>();
      
      HashMap<String,String> hostNameCache = new HashMap<String,String>();
      for (Entry<String,Map<KeyExtent,List<Range>>> tserverBin : binnedRanges.entrySet()) {
        String ip = tserverBin.getKey().split(":", 2)[0];
        String location = hostNameCache.get(ip);
        if (location == null) {
          InetAddress inetAddress = InetAddress.getByName(ip);
          location = inetAddress.getHostName();
          hostNameCache.put(ip, location);
        }
        for (Entry<KeyExtent,List<Range>> extentRanges : tserverBin.getValue().entrySet()) {
          Range ke = extentRanges.getKey().toDataRange();
          for (Range r : extentRanges.getValue()) {
            if (autoAdjust) {
              // divide ranges into smaller ranges, based on the tablets
              splits.add(new RangeInputSplit(tableConfig.getTableName(), tableId, ke.clip(r), new String[] {location}));
            } else {
              // don't divide ranges
              ArrayList<String> locations = splitsToAdd.get(r);
              if (locations == null)
                locations = new ArrayList<String>(1);
              locations.add(location);
              splitsToAdd.put(r, locations);
            }
          }
        }
      }
      
      if (!autoAdjust)
        for (Entry<Range,ArrayList<String>> entry : splitsToAdd.entrySet())
          splits.add(new RangeInputSplit(tableConfig.getTableName(), tableId, entry.getKey(), entry.getValue().toArray(new String[0])));
    }
    
    return splits.toArray(new InputSplit[splits.size()]);
  }
  
  /**
   * The Class RangeInputSplit. Encapsulates an Accumulo range for use in Map Reduce jobs.
   */
  public static class RangeInputSplit extends org.apache.accumulo.core.client.mapreduce.InputFormatBase.RangeInputSplit implements InputSplit {
    
    public RangeInputSplit() {
      super();
    }
    
    public RangeInputSplit(RangeInputSplit split) throws IOException {
      super(split);
    }
    
    protected RangeInputSplit(String table, String tableId, Range range, String[] locations) {
      super(table, tableId, range, locations);
    }
  }
  
}
