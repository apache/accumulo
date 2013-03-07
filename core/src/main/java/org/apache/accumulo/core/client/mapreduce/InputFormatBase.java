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
package org.apache.accumulo.core.client.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.accumulo.core.Constants;
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
import org.apache.accumulo.core.client.mapreduce.lib.util.InputConfigurator;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This abstract {@link InputFormat} class allows MapReduce jobs to use Accumulo as the source of K,V pairs.
 * <p>
 * Subclasses must implement a {@link #createRecordReader(InputSplit, TaskAttemptContext)} to provide a {@link RecordReader} for K,V.
 * <p>
 * A static base class, RecordReaderBase, is provided to retrieve Accumulo {@link Key}/{@link Value} pairs, but one must implement its
 * {@link RecordReaderBase#nextKeyValue()} to transform them to the desired generic types K,V.
 * <p>
 * See {@link AccumuloInputFormat} for an example implementation.
 */
public abstract class InputFormatBase<K,V> extends InputFormat<K,V> {
  
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
  public static void setConnectorInfo(Job job, String principal, AuthenticationToken token) throws AccumuloSecurityException {
    InputConfigurator.setConnectorInfo(CLASS, job.getConfiguration(), principal, token);
  }
  
  /**
   * Sets the connector information needed to communicate with Accumulo in this job. The authentication information will be read from the specified file when
   * the job runs. This prevents the user's token from being exposed on the Job Tracker web page. The specified path will be placed in the
   * {@link DistributedCache}, for better performance during job execution. Users can create the contents of this file using
   * {@link CredentialHelper#asBase64String(TCredentials)}.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param path
   *          the path to a file in the configured file system, containing the serialized, base-64 encoded {@link AuthenticationToken} with the user's
   *          authentication
   * @since 1.5.0
   */
  public static void setConnectorInfo(Job job, Path path) {
    InputConfigurator.setConnectorInfo(CLASS, job.getConfiguration(), path);
  }
  
  /**
   * Determines if the connector has been configured.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return true if the connector has been configured, false otherwise
   * @since 1.5.0
   * @see #setConnectorInfo(Job, String, AuthenticationToken)
   * @see #setConnectorInfo(Job, Path)
   */
  protected static Boolean isConnectorInfoSet(JobContext context) {
    return InputConfigurator.isConnectorInfoSet(CLASS, context.getConfiguration());
  }
  
  /**
   * Gets the user name from the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the user name
   * @since 1.5.0
   * @see #setConnectorInfo(Job, String, AuthenticationToken)
   * @see #setConnectorInfo(Job, Path)
   */
  protected static String getPrincipal(JobContext context) {
    return InputConfigurator.getPrincipal(CLASS, context.getConfiguration());
  }
  
  /**
   * Gets the serialized token class from the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the user name
   * @since 1.5.0
   * @see #setConnectorInfo(Job, String, AuthenticationToken)
   * @see #setConnectorInfo(Job, Path)
   */
  protected static String getTokenClass(JobContext context) {
    return InputConfigurator.getTokenClass(CLASS, context.getConfiguration());
  }
  
  /**
   * Gets the password from the configuration. WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to
   * provide a charset safe conversion to a string, and is not intended to be secure.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the decoded user password
   * @since 1.5.0
   * @see #setConnectorInfo(Job, String, AuthenticationToken)
   */
  protected static byte[] getToken(JobContext context) {
    return InputConfigurator.getToken(CLASS, context.getConfiguration());
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
  public static void setZooKeeperInstance(Job job, String instanceName, String zooKeepers) {
    InputConfigurator.setZooKeeperInstance(CLASS, job.getConfiguration(), instanceName, zooKeepers);
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
  public static void setMockInstance(Job job, String instanceName) {
    InputConfigurator.setMockInstance(CLASS, job.getConfiguration(), instanceName);
  }
  
  /**
   * Initializes an Accumulo {@link Instance} based on the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return an Accumulo instance
   * @since 1.5.0
   * @see #setZooKeeperInstance(Job, String, String)
   * @see #setMockInstance(Job, String)
   */
  protected static Instance getInstance(JobContext context) {
    return InputConfigurator.getInstance(CLASS, context.getConfiguration());
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
  public static void setLogLevel(Job job, Level level) {
    InputConfigurator.setLogLevel(CLASS, job.getConfiguration(), level);
  }
  
  /**
   * Gets the log level from this configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the log level
   * @since 1.5.0
   * @see #setLogLevel(Job, Level)
   */
  protected static Level getLogLevel(JobContext context) {
    return InputConfigurator.getLogLevel(CLASS, context.getConfiguration());
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
  public static void setInputTableName(Job job, String tableName) {
    InputConfigurator.setInputTableName(CLASS, job.getConfiguration(), tableName);
  }
  
  /**
   * Gets the table name from the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the table name
   * @since 1.5.0
   * @see #setInputTableName(Job, String)
   */
  protected static String getInputTableName(JobContext context) {
    return InputConfigurator.getInputTableName(CLASS, context.getConfiguration());
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
  public static void setScanAuthorizations(Job job, Authorizations auths) {
    InputConfigurator.setScanAuthorizations(CLASS, job.getConfiguration(), auths);
  }
  
  /**
   * Gets the authorizations to set for the scans from the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the Accumulo scan authorizations
   * @since 1.5.0
   * @see #setScanAuthorizations(Job, Authorizations)
   */
  protected static Authorizations getScanAuthorizations(JobContext context) {
    return InputConfigurator.getScanAuthorizations(CLASS, context.getConfiguration());
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
  public static void setRanges(Job job, Collection<Range> ranges) {
    InputConfigurator.setRanges(CLASS, job.getConfiguration(), ranges);
  }
  
  /**
   * Gets the ranges to scan over from a job.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the ranges
   * @throws IOException
   *           if the ranges have been encoded improperly
   * @since 1.5.0
   * @see #setRanges(Job, Collection)
   */
  protected static List<Range> getRanges(JobContext context) throws IOException {
    return InputConfigurator.getRanges(CLASS, context.getConfiguration());
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
  public static void fetchColumns(Job job, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    InputConfigurator.fetchColumns(CLASS, job.getConfiguration(), columnFamilyColumnQualifierPairs);
  }
  
  /**
   * Gets the columns to be mapped over from this job.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return a set of columns
   * @since 1.5.0
   * @see #fetchColumns(Job, Collection)
   */
  protected static Set<Pair<Text,Text>> getFetchedColumns(JobContext context) {
    return InputConfigurator.getFetchedColumns(CLASS, context.getConfiguration());
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
  public static void addIterator(Job job, IteratorSetting cfg) {
    InputConfigurator.addIterator(CLASS, job.getConfiguration(), cfg);
  }
  
  /**
   * Gets a list of the iterator settings (for iterators to apply to a scanner) from this configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return a list of iterators
   * @since 1.5.0
   * @see #addIterator(Job, IteratorSetting)
   */
  protected static List<IteratorSetting> getIterators(JobContext context) {
    return InputConfigurator.getIterators(CLASS, context.getConfiguration());
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
   * @see #setRanges(Job, Collection)
   * @since 1.5.0
   */
  public static void setAutoAdjustRanges(Job job, boolean enableFeature) {
    InputConfigurator.setAutoAdjustRanges(CLASS, job.getConfiguration(), enableFeature);
  }
  
  /**
   * Determines whether a configuration has auto-adjust ranges enabled.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return false if the feature is disabled, true otherwise
   * @since 1.5.0
   * @see #setAutoAdjustRanges(Job, boolean)
   */
  protected static boolean getAutoAdjustRanges(JobContext context) {
    return InputConfigurator.getAutoAdjustRanges(CLASS, context.getConfiguration());
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
  public static void setScanIsolation(Job job, boolean enableFeature) {
    InputConfigurator.setScanIsolation(CLASS, job.getConfiguration(), enableFeature);
  }
  
  /**
   * Determines whether a configuration has isolation enabled.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return true if the feature is enabled, false otherwise
   * @since 1.5.0
   * @see #setScanIsolation(Job, boolean)
   */
  protected static boolean isIsolated(JobContext context) {
    return InputConfigurator.isIsolated(CLASS, context.getConfiguration());
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
  public static void setLocalIterators(Job job, boolean enableFeature) {
    InputConfigurator.setLocalIterators(CLASS, job.getConfiguration(), enableFeature);
  }
  
  /**
   * Determines whether a configuration uses local iterators.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return true if the feature is enabled, false otherwise
   * @since 1.5.0
   * @see #setLocalIterators(Job, boolean)
   */
  protected static boolean usesLocalIterators(JobContext context) {
    return InputConfigurator.usesLocalIterators(CLASS, context.getConfiguration());
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
  public static void setOfflineTableScan(Job job, boolean enableFeature) {
    InputConfigurator.setOfflineTableScan(CLASS, job.getConfiguration(), enableFeature);
  }
  
  /**
   * Determines whether a configuration has the offline table scan feature enabled.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return true if the feature is enabled, false otherwise
   * @since 1.5.0
   * @see #setOfflineTableScan(Job, boolean)
   */
  protected static boolean isOfflineScan(JobContext context) {
    return InputConfigurator.isOfflineScan(CLASS, context.getConfiguration());
  }
  
  /**
   * Initializes an Accumulo {@link TabletLocator} based on the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return an Accumulo tablet locator
   * @throws TableNotFoundException
   *           if the table name set on the configuration doesn't exist
   * @since 1.5.0
   */
  protected static TabletLocator getTabletLocator(JobContext context) throws TableNotFoundException {
    return InputConfigurator.getTabletLocator(CLASS, context.getConfiguration());
  }
  
  // InputFormat doesn't have the equivalent of OutputFormat's checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @throws IOException
   *           if the context is improperly configured
   * @since 1.5.0
   */
  protected static void validateOptions(JobContext context) throws IOException {
    InputConfigurator.validateOptions(CLASS, context.getConfiguration());
  }
  
  /**
   * An abstract base class to be used to create {@link RecordReader} instances that convert from Accumulo {@link Key}/{@link Value} pairs to the user's K/V
   * types.
   * 
   * Subclasses must implement {@link #nextKeyValue()} and use it to update the following variables:
   * <ul>
   * <li>K {@link #currentK}</li>
   * <li>V {@link #currentV}</li>
   * <li>Key {@link #currentKey} (used for progress reporting)</li>
   * <li>int {@link #numKeysRead} (used for progress reporting)</li>
   * </ul>
   */
  protected abstract static class RecordReaderBase<K,V> extends RecordReader<K,V> {
    protected long numKeysRead;
    protected Iterator<Entry<Key,Value>> scannerIterator;
    protected RangeInputSplit split;
    
    /**
     * Apply the configured iterators from the configuration to the scanner.
     * 
     * @param context
     *          the Hadoop context for the configured job
     * @param scanner
     *          the scanner to configure
     */
    protected void setupIterators(TaskAttemptContext context, Scanner scanner) {
      List<IteratorSetting> iterators = getIterators(context);
      for (IteratorSetting iterator : iterators) {
        scanner.addScanIterator(iterator);
      }
    }
    
    /**
     * Initialize a scanner over the given input split using this task attempt configuration.
     */
    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
      Scanner scanner;
      split = (RangeInputSplit) inSplit;
      log.debug("Initializing input split: " + split.range);
      Instance instance = getInstance(attempt);
      String principal = getPrincipal(attempt);
      String tokenClass = getTokenClass(attempt);
      byte[] token = getToken(attempt);
      Authorizations authorizations = getScanAuthorizations(attempt);
      
      try {
        log.debug("Creating connector with user: " + principal);
        Connector conn = instance.getConnector(principal, CredentialHelper.extractToken(tokenClass, token));
        log.debug("Creating scanner for table: " + getInputTableName(attempt));
        log.debug("Authorizations are: " + authorizations);
        if (isOfflineScan(attempt)) {
          scanner = new OfflineScanner(instance, new TCredentials(principal, tokenClass, ByteBuffer.wrap(token), instance.getInstanceID()), Tables.getTableId(
              instance, getInputTableName(attempt)), authorizations);
        } else {
          scanner = conn.createScanner(getInputTableName(attempt), authorizations);
        }
        if (isIsolated(attempt)) {
          log.info("Creating isolated scanner");
          scanner = new IsolatedScanner(scanner);
        }
        if (usesLocalIterators(attempt)) {
          log.info("Using local iterators");
          scanner = new ClientSideIteratorScanner(scanner);
        }
        setupIterators(attempt, scanner);
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      // setup a scanner within the bounds of this split
      for (Pair<Text,Text> c : getFetchedColumns(attempt)) {
        if (c.getSecond() != null) {
          log.debug("Fetching column " + c.getFirst() + ":" + c.getSecond());
          scanner.fetchColumn(c.getFirst(), c.getSecond());
        } else {
          log.debug("Fetching column family " + c.getFirst());
          scanner.fetchColumnFamily(c.getFirst());
        }
      }
      
      scanner.setRange(split.range);
      
      numKeysRead = 0;
      
      // do this last after setting all scanner options
      scannerIterator = scanner.iterator();
    }
    
    @Override
    public void close() {}
    
    @Override
    public float getProgress() throws IOException {
      if (numKeysRead > 0 && currentKey == null)
        return 1.0f;
      return split.getProgress(currentKey);
    }
    
    protected K currentK = null;
    protected V currentV = null;
    protected Key currentKey = null;
    protected Value currentValue = null;
    
    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
      return currentK;
    }
    
    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
      return currentV;
    }
  }
  
  Map<String,Map<KeyExtent,List<Range>>> binOfflineTable(JobContext context, String tableName, List<Range> ranges) throws TableNotFoundException,
      AccumuloException, AccumuloSecurityException {
    
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    
    Instance instance = getInstance(context);
    Connector conn = instance.getConnector(getPrincipal(context), CredentialHelper.extractToken(getTokenClass(context), getToken(context)));
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
      Scanner scanner = conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
      Constants.METADATA_PREV_ROW_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(Constants.METADATA_LAST_LOCATION_COLUMN_FAMILY);
      scanner.fetchColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY);
      scanner.fetchColumnFamily(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY);
      scanner.setRange(metadataRange);
      
      RowIterator rowIter = new RowIterator(scanner);
      
      // TODO check that extents match prev extent
      
      KeyExtent lastExtent = null;
      
      while (rowIter.hasNext()) {
        Iterator<Entry<Key,Value>> row = rowIter.next();
        String last = "";
        KeyExtent extent = null;
        String location = null;
        
        while (row.hasNext()) {
          Entry<Key,Value> entry = row.next();
          Key key = entry.getKey();
          
          if (key.getColumnFamily().equals(Constants.METADATA_LAST_LOCATION_COLUMN_FAMILY)) {
            last = entry.getValue().toString();
          }
          
          if (key.getColumnFamily().equals(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY)
              || key.getColumnFamily().equals(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY)) {
            location = entry.getValue().toString();
          }
          
          if (Constants.METADATA_PREV_ROW_COLUMN.hasColumns(key)) {
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
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    log.setLevel(getLogLevel(context));
    validateOptions(context);
    
    String tableName = getInputTableName(context);
    boolean autoAdjust = getAutoAdjustRanges(context);
    List<Range> ranges = autoAdjust ? Range.mergeOverlapping(getRanges(context)) : getRanges(context);
    
    if (ranges.isEmpty()) {
      ranges = new ArrayList<Range>(1);
      ranges.add(new Range());
    }
    
    // get the metadata information for these ranges
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    TabletLocator tl;
    try {
      if (isOfflineScan(context)) {
        binnedRanges = binOfflineTable(context, tableName, ranges);
        while (binnedRanges == null) {
          // Some tablets were still online, try again
          UtilWaitThread.sleep(100 + (int) (Math.random() * 100)); // sleep randomly between 100 and 200 ms
          binnedRanges = binOfflineTable(context, tableName, ranges);
        }
      } else {
        Instance instance = getInstance(context);
        String tableId = null;
        tl = getTabletLocator(context);
        // its possible that the cache could contain complete, but old information about a tables tablets... so clear it
        tl.invalidateCache();
        while (!tl.binRanges(ranges, binnedRanges,
            new TCredentials(getPrincipal(context), getTokenClass(context), ByteBuffer.wrap(getToken(context)), getInstance(context).getInstanceID())).isEmpty()) {
          if (!(instance instanceof MockInstance)) {
            if (tableId == null)
              tableId = Tables.getTableId(instance, tableName);
            if (!Tables.exists(instance, tableId))
              throw new TableDeletedException(tableId);
            if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
              throw new TableOfflineException(instance, tableId);
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
    
    ArrayList<InputSplit> splits = new ArrayList<InputSplit>(ranges.size());
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
            splits.add(new RangeInputSplit(tableName, ke.clip(r), new String[] {location}));
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
        splits.add(new RangeInputSplit(tableName, entry.getKey(), entry.getValue().toArray(new String[0])));
    return splits;
  }
  
  /**
   * The Class RangeInputSplit. Encapsulates an Accumulo range for use in Map Reduce jobs.
   */
  public static class RangeInputSplit extends InputSplit implements Writable {
    private Range range;
    private String[] locations;
    
    public RangeInputSplit() {
      range = new Range();
      locations = new String[0];
    }
    
    public RangeInputSplit(RangeInputSplit split) throws IOException {
      this.setRange(split.getRange());
      this.setLocations(split.getLocations());
    }
    
    protected RangeInputSplit(String table, Range range, String[] locations) {
      this.range = range;
      this.locations = locations;
    }
    
    public Range getRange() {
      return range;
    }
    
    public void setRange(Range range) {
      this.range = range;
    }
    
    private static byte[] extractBytes(ByteSequence seq, int numBytes) {
      byte[] bytes = new byte[numBytes + 1];
      bytes[0] = 0;
      for (int i = 0; i < numBytes; i++) {
        if (i >= seq.length())
          bytes[i + 1] = 0;
        else
          bytes[i + 1] = seq.byteAt(i);
      }
      return bytes;
    }
    
    public static float getProgress(ByteSequence start, ByteSequence end, ByteSequence position) {
      int maxDepth = Math.min(Math.max(end.length(), start.length()), position.length());
      BigInteger startBI = new BigInteger(extractBytes(start, maxDepth));
      BigInteger endBI = new BigInteger(extractBytes(end, maxDepth));
      BigInteger positionBI = new BigInteger(extractBytes(position, maxDepth));
      return (float) (positionBI.subtract(startBI).doubleValue() / endBI.subtract(startBI).doubleValue());
    }
    
    public float getProgress(Key currentKey) {
      if (currentKey == null)
        return 0f;
      if (range.getStartKey() != null && range.getEndKey() != null) {
        if (!range.getStartKey().equals(range.getEndKey(), PartialKey.ROW)) {
          // just look at the row progress
          return getProgress(range.getStartKey().getRowData(), range.getEndKey().getRowData(), currentKey.getRowData());
        } else if (!range.getStartKey().equals(range.getEndKey(), PartialKey.ROW_COLFAM)) {
          // just look at the column family progress
          return getProgress(range.getStartKey().getColumnFamilyData(), range.getEndKey().getColumnFamilyData(), currentKey.getColumnFamilyData());
        } else if (!range.getStartKey().equals(range.getEndKey(), PartialKey.ROW_COLFAM_COLQUAL)) {
          // just look at the column qualifier progress
          return getProgress(range.getStartKey().getColumnQualifierData(), range.getEndKey().getColumnQualifierData(), currentKey.getColumnQualifierData());
        }
      }
      // if we can't figure it out, then claim no progress
      return 0f;
    }
    
    /**
     * This implementation of length is only an estimate, it does not provide exact values. Do not have your code rely on this return value.
     */
    @Override
    public long getLength() throws IOException {
      Text startRow = range.isInfiniteStartKey() ? new Text(new byte[] {Byte.MIN_VALUE}) : range.getStartKey().getRow();
      Text stopRow = range.isInfiniteStopKey() ? new Text(new byte[] {Byte.MAX_VALUE}) : range.getEndKey().getRow();
      int maxCommon = Math.min(7, Math.min(startRow.getLength(), stopRow.getLength()));
      long diff = 0;
      
      byte[] start = startRow.getBytes();
      byte[] stop = stopRow.getBytes();
      for (int i = 0; i < maxCommon; ++i) {
        diff |= 0xff & (start[i] ^ stop[i]);
        diff <<= Byte.SIZE;
      }
      
      if (startRow.getLength() != stopRow.getLength())
        diff |= 0xff;
      
      return diff + 1;
    }
    
    @Override
    public String[] getLocations() throws IOException {
      return locations;
    }
    
    public void setLocations(String[] locations) {
      this.locations = locations;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      range.readFields(in);
      int numLocs = in.readInt();
      locations = new String[numLocs];
      for (int i = 0; i < numLocs; ++i)
        locations[i] = in.readUTF();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      range.write(out);
      out.writeInt(locations.length);
      for (int i = 0; i < locations.length; ++i)
        out.writeUTF(locations[i]);
    }
  }
  
  // ----------------------------------------------------------------------------------------------------
  // Everything below this line is deprecated and should go away in future versions
  // ----------------------------------------------------------------------------------------------------
  
  /**
   * @deprecated since 1.5.0; Use {@link #setScanIsolation(Job, boolean)} instead.
   */
  @Deprecated
  public static void setIsolated(Configuration conf, boolean enable) {
    InputConfigurator.setScanIsolation(CLASS, conf, enable);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setLocalIterators(Job, boolean)} instead.
   */
  @Deprecated
  public static void setLocalIterators(Configuration conf, boolean enable) {
    InputConfigurator.setLocalIterators(CLASS, conf, enable);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setConnectorInfo(Job, String, AuthenticationToken)}, {@link #setInputTableName(Job, String)}, and
   *             {@link #setScanAuthorizations(Job, Authorizations)} instead.
   */
  @Deprecated
  public static void setInputInfo(Configuration conf, String user, byte[] passwd, String table, Authorizations auths) {
    try {
      InputConfigurator.setConnectorInfo(CLASS, conf, user, new PasswordToken(passwd));
    } catch (AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
    InputConfigurator.setInputTableName(CLASS, conf, table);
    InputConfigurator.setScanAuthorizations(CLASS, conf, auths);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setZooKeeperInstance(Job, String, String)} instead.
   */
  @Deprecated
  public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
    InputConfigurator.setZooKeeperInstance(CLASS, conf, instanceName, zooKeepers);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setMockInstance(Job, String)} instead.
   */
  @Deprecated
  public static void setMockInstance(Configuration conf, String instanceName) {
    InputConfigurator.setMockInstance(CLASS, conf, instanceName);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setRanges(Job, Collection)} instead.
   */
  @Deprecated
  public static void setRanges(Configuration conf, Collection<Range> ranges) {
    InputConfigurator.setRanges(CLASS, conf, ranges);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setAutoAdjustRanges(Job, boolean)} instead.
   */
  @Deprecated
  public static void disableAutoAdjustRanges(Configuration conf) {
    InputConfigurator.setAutoAdjustRanges(CLASS, conf, false);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #addIterator(Job, IteratorSetting)} to add the {@link VersioningIterator} instead.
   */
  @Deprecated
  public static void setMaxVersions(Configuration conf, int maxVersions) throws IOException {
    IteratorSetting vers = new IteratorSetting(1, "vers", VersioningIterator.class);
    try {
      VersioningIterator.setMaxVersions(vers, maxVersions);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    }
    InputConfigurator.addIterator(CLASS, conf, vers);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setOfflineTableScan(Job, boolean)} instead.
   */
  @Deprecated
  public static void setScanOffline(Configuration conf, boolean scanOff) {
    InputConfigurator.setOfflineTableScan(CLASS, conf, scanOff);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #fetchColumns(Job, Collection)} instead.
   */
  @Deprecated
  public static void fetchColumns(Configuration conf, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    InputConfigurator.fetchColumns(CLASS, conf, columnFamilyColumnQualifierPairs);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setLogLevel(Job, Level)} instead.
   */
  @Deprecated
  public static void setLogLevel(Configuration conf, Level level) {
    InputConfigurator.setLogLevel(CLASS, conf, level);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #addIterator(Job, IteratorSetting)} instead.
   */
  @Deprecated
  public static void addIterator(Configuration conf, IteratorSetting cfg) {
    InputConfigurator.addIterator(CLASS, conf, cfg);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #isIsolated(JobContext)} instead.
   */
  @Deprecated
  protected static boolean isIsolated(Configuration conf) {
    return InputConfigurator.isIsolated(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #usesLocalIterators(JobContext)} instead.
   */
  @Deprecated
  protected static boolean usesLocalIterators(Configuration conf) {
    return InputConfigurator.usesLocalIterators(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getPrincipal(JobContext)} instead.
   */
  @Deprecated
  protected static String getPrincipal(Configuration conf) {
    return InputConfigurator.getPrincipal(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getToken(JobContext)} instead.
   */
  @Deprecated
  protected static byte[] getToken(Configuration conf) {
    return InputConfigurator.getToken(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getInputTableName(JobContext)} instead.
   */
  @Deprecated
  protected static String getTablename(Configuration conf) {
    return InputConfigurator.getInputTableName(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getScanAuthorizations(JobContext)} instead.
   */
  @Deprecated
  protected static Authorizations getAuthorizations(Configuration conf) {
    return InputConfigurator.getScanAuthorizations(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getInstance(JobContext)} instead.
   */
  @Deprecated
  protected static Instance getInstance(Configuration conf) {
    return InputConfigurator.getInstance(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getTabletLocator(JobContext)} instead.
   */
  @Deprecated
  protected static TabletLocator getTabletLocator(Configuration conf) throws TableNotFoundException {
    return InputConfigurator.getTabletLocator(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getRanges(JobContext)} instead.
   */
  @Deprecated
  protected static List<Range> getRanges(Configuration conf) throws IOException {
    return InputConfigurator.getRanges(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getFetchedColumns(JobContext)} instead.
   */
  @Deprecated
  protected static Set<Pair<Text,Text>> getFetchedColumns(Configuration conf) {
    return InputConfigurator.getFetchedColumns(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getAutoAdjustRanges(JobContext)} instead.
   */
  @Deprecated
  protected static boolean getAutoAdjustRanges(Configuration conf) {
    return InputConfigurator.getAutoAdjustRanges(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getLogLevel(JobContext)} instead.
   */
  @Deprecated
  protected static Level getLogLevel(Configuration conf) {
    return InputConfigurator.getLogLevel(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #validateOptions(JobContext)} instead.
   */
  @Deprecated
  protected static void validateOptions(Configuration conf) throws IOException {
    InputConfigurator.validateOptions(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #addIterator(Job, IteratorSetting)} to add the {@link VersioningIterator} instead.
   */
  @Deprecated
  protected static int getMaxVersions(Configuration conf) {
    // This is so convoluted, because the only reason to get the number of maxVersions is to construct the same type of IteratorSetting object we have to
    // deconstruct to get at this option in the first place, but to preserve correct behavior, this appears necessary.
    List<IteratorSetting> iteratorSettings = InputConfigurator.getIterators(CLASS, conf);
    for (IteratorSetting setting : iteratorSettings) {
      if ("vers".equals(setting.getName()) && 1 == setting.getPriority() && VersioningIterator.class.getName().equals(setting.getIteratorClass())) {
        if (setting.getOptions().containsKey("maxVersions"))
          return Integer.parseInt(setting.getOptions().get("maxVersions"));
        else
          return -1;
      }
    }
    return -1;
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #isOfflineScan(JobContext)} instead.
   */
  @Deprecated
  protected static boolean isOfflineScan(Configuration conf) {
    return InputConfigurator.isOfflineScan(CLASS, conf);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getIterators(JobContext)} instead.
   */
  @Deprecated
  protected static List<AccumuloIterator> getIterators(Configuration conf) {
    List<IteratorSetting> iteratorSettings = InputConfigurator.getIterators(CLASS, conf);
    List<AccumuloIterator> deprecatedIterators = new ArrayList<AccumuloIterator>(iteratorSettings.size());
    for (IteratorSetting setting : iteratorSettings) {
      AccumuloIterator deprecatedIter = new AccumuloIterator(new String(setting.getPriority() + AccumuloIterator.FIELD_SEP + setting.getIteratorClass()
          + AccumuloIterator.FIELD_SEP + setting.getName()));
      deprecatedIterators.add(deprecatedIter);
    }
    return deprecatedIterators;
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getIterators(JobContext)} instead.
   */
  @Deprecated
  protected static List<AccumuloIteratorOption> getIteratorOptions(Configuration conf) {
    List<IteratorSetting> iteratorSettings = InputConfigurator.getIterators(CLASS, conf);
    List<AccumuloIteratorOption> deprecatedIteratorOptions = new ArrayList<AccumuloIteratorOption>(iteratorSettings.size());
    for (IteratorSetting setting : iteratorSettings) {
      for (Entry<String,String> opt : setting.getOptions().entrySet()) {
        String deprecatedOption;
        try {
          deprecatedOption = new String(setting.getName() + AccumuloIteratorOption.FIELD_SEP + URLEncoder.encode(opt.getKey(), "UTF-8")
              + AccumuloIteratorOption.FIELD_SEP + URLEncoder.encode(opt.getValue(), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
        deprecatedIteratorOptions.add(new AccumuloIteratorOption(deprecatedOption));
      }
    }
    return deprecatedIteratorOptions;
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link IteratorSetting} instead.
   */
  @Deprecated
  static class AccumuloIterator {
    
    private static final String FIELD_SEP = ":";
    
    private int priority;
    private String iteratorClass;
    private String iteratorName;
    
    public AccumuloIterator(int priority, String iteratorClass, String iteratorName) {
      this.priority = priority;
      this.iteratorClass = iteratorClass;
      this.iteratorName = iteratorName;
    }
    
    // Parses out a setting given an string supplied from an earlier toString() call
    public AccumuloIterator(String iteratorSetting) {
      // Parse the string to expand the iterator
      StringTokenizer tokenizer = new StringTokenizer(iteratorSetting, FIELD_SEP);
      priority = Integer.parseInt(tokenizer.nextToken());
      iteratorClass = tokenizer.nextToken();
      iteratorName = tokenizer.nextToken();
    }
    
    public int getPriority() {
      return priority;
    }
    
    public String getIteratorClass() {
      return iteratorClass;
    }
    
    public String getIteratorName() {
      return iteratorName;
    }
    
    @Override
    public String toString() {
      return new String(priority + FIELD_SEP + iteratorClass + FIELD_SEP + iteratorName);
    }
    
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link IteratorSetting} instead.
   */
  @Deprecated
  static class AccumuloIteratorOption {
    private static final String FIELD_SEP = ":";
    
    private String iteratorName;
    private String key;
    private String value;
    
    public AccumuloIteratorOption(String iteratorName, String key, String value) {
      this.iteratorName = iteratorName;
      this.key = key;
      this.value = value;
    }
    
    // Parses out an option given a string supplied from an earlier toString() call
    public AccumuloIteratorOption(String iteratorOption) {
      StringTokenizer tokenizer = new StringTokenizer(iteratorOption, FIELD_SEP);
      this.iteratorName = tokenizer.nextToken();
      try {
        this.key = URLDecoder.decode(tokenizer.nextToken(), "UTF-8");
        this.value = URLDecoder.decode(tokenizer.nextToken(), "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
    
    public String getIteratorName() {
      return iteratorName;
    }
    
    public String getKey() {
      return key;
    }
    
    public String getValue() {
      return value;
    }
    
    @Override
    public String toString() {
      try {
        return new String(iteratorName + FIELD_SEP + URLEncoder.encode(key, "UTF-8") + FIELD_SEP + URLEncoder.encode(value, "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
    
  }
  
}
