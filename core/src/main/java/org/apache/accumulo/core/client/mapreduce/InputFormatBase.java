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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
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
import java.util.HashSet;
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
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.mock.MockTabletLocator;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
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
 * RecordReaderBase.nextKeyValue() to transform them to the desired generic types K,V.
 * <p>
 * See {@link AccumuloInputFormat} for an example implementation.
 */

public abstract class InputFormatBase<K,V> extends InputFormat<K,V> {
  protected static final Logger log = Logger.getLogger(InputFormatBase.class);
  
  /**
   * Prefix shared by all job configuration property names for this class
   */
  private static final String PREFIX = AccumuloInputFormat.class.getSimpleName();
  
  /**
   * Used to limit the times a job can be configured with input information to 1
   * 
   * @see #setInputInfo(Job, String, byte[], String, Authorizations)
   * @see #getUsername(JobContext)
   * @see #getPassword(JobContext)
   * @see #getTablename(JobContext)
   * @see #getAuthorizations(JobContext)
   */
  private static final String INPUT_INFO_HAS_BEEN_SET = PREFIX + ".configured";
  
  /**
   * Used to limit the times a job can be configured with instance information to 1
   * 
   * @see #setZooKeeperInstance(Job, String, String)
   * @see #setMockInstance(Job, String)
   * @see #getInstance(JobContext)
   */
  private static final String INSTANCE_HAS_BEEN_SET = PREFIX + ".instanceConfigured";
  
  /**
   * Key for storing the Accumulo user's name
   * 
   * @see #setInputInfo(Job, String, byte[], String, Authorizations)
   * @see #getUsername(JobContext)
   */
  private static final String USERNAME = PREFIX + ".username";
  
  /**
   * Key for storing the Accumulo user's password
   * 
   * @see #setInputInfo(Job, String, byte[], String, Authorizations)
   * @see #getPassword(JobContext)
   */
  private static final String PASSWORD = PREFIX + ".password";
  
  /**
   * Key for storing the source table's name
   * 
   * @see #setInputInfo(Job, String, byte[], String, Authorizations)
   * @see #getTablename(JobContext)
   */
  private static final String TABLE_NAME = PREFIX + ".tablename";
  
  /**
   * Key for storing the authorizations to use to scan
   * 
   * @see #setInputInfo(Job, String, byte[], String, Authorizations)
   * @see #getAuthorizations(JobContext)
   */
  private static final String AUTHORIZATIONS = PREFIX + ".authorizations";
  
  /**
   * Key for storing the Accumulo instance name to connect to
   * 
   * @see #setZooKeeperInstance(Job, String, String)
   * @see #setMockInstance(Job, String)
   * @see #getInstance(JobContext)
   */
  private static final String INSTANCE_NAME = PREFIX + ".instanceName";
  
  /**
   * Key for storing the set of Accumulo zookeeper servers to communicate with
   * 
   * @see #setZooKeeperInstance(Job, String, String)
   * @see #getInstance(JobContext)
   */
  private static final String ZOOKEEPERS = PREFIX + ".zooKeepers";
  
  /**
   * Key for storing the directive to use the mock instance type
   * 
   * @see #setMockInstance(Job, String)
   * @see #getInstance(JobContext)
   */
  private static final String MOCK = PREFIX + ".useMockInstance";
  
  /**
   * Key for storing the set of ranges over which to query
   * 
   * @see #setRanges(Job, Collection)
   * @see #getRanges(JobContext)
   */
  private static final String RANGES = PREFIX + ".ranges";
  
  /**
   * Key for storing whether to auto adjust ranges by merging overlapping ranges and splitting them on tablet boundaries
   * 
   * @see #setAutoAdjustRanges(Job, boolean)
   * @see #getAutoAdjustRanges(JobContext)
   */
  private static final String AUTO_ADJUST_RANGES = PREFIX + ".ranges.autoAdjust";
  
  /**
   * Key for storing the set of columns to query
   * 
   * @see #fetchColumns(Job, Collection)
   * @see #getFetchedColumns(JobContext)
   */
  private static final String COLUMNS = PREFIX + ".columns";
  
  /**
   * Key for storing the desired logging level
   * 
   * @see #setLogLevel(Job, Level)
   * @see #getLogLevel(JobContext)
   */
  private static final String LOGLEVEL = PREFIX + ".loglevel";
  
  /**
   * Key for storing whether the scanner to use is {@link IsolatedScanner}
   * 
   * @see #setScanIsolation(Job, boolean)
   * @see #isIsolated(JobContext)
   */
  private static final String ISOLATED = PREFIX + ".isolated";
  
  /**
   * Key for storing whether iterators are local
   * 
   * @see #setLocalIterators(Job, boolean)
   * @see #usesLocalIterators(JobContext)
   */
  private static final String LOCAL_ITERATORS = PREFIX + ".localiters";
  
  /**
   * Key for storing the scan-time iterators to use
   * 
   * @see #addIterator(Job, IteratorSetting)
   * @see #getIterators(JobContext)
   */
  private static final String ITERATORS = PREFIX + ".iterators";
  
  /**
   * Constant for separating serialized {@link IteratorSetting} configuration in the job
   * 
   * @see #addIterator(Job, IteratorSetting)
   * @see #getIterators(JobContext)
   */
  private static final String ITERATORS_DELIM = ",";
  
  /**
   * Key for storing whether to read a table's files while it is offline
   * 
   * @see #setOfflineTableScan(Job, boolean)
   * @see #isOfflineScan(JobContext)
   */
  private static final String READ_OFFLINE = PREFIX + ".read.offline";
  
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
    job.getConfiguration().setBoolean(ISOLATED, enableFeature);
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
    job.getConfiguration().setBoolean(LOCAL_ITERATORS, enableFeature);
  }
  
  /**
   * Sets the minimum information needed to query Accumulo in this job.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param user
   *          a valid Accumulo user name
   * @param passwd
   *          the user's password
   * @param table
   *          the table from which to read
   * @param auths
   *          the authorizations used to restrict data read
   * @since 1.5.0
   */
  public static void setInputInfo(Job job, String user, byte[] passwd, String table, Authorizations auths) {
    if (job.getConfiguration().getBoolean(INPUT_INFO_HAS_BEEN_SET, false))
      throw new IllegalStateException("Input info can only be set once per job");
    job.getConfiguration().setBoolean(INPUT_INFO_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(user, passwd, table);
    job.getConfiguration().set(USERNAME, user);
    job.getConfiguration().set(PASSWORD, new String(Base64.encodeBase64(passwd)));
    job.getConfiguration().set(TABLE_NAME, table);
    if (auths != null && !auths.isEmpty())
      job.getConfiguration().set(AUTHORIZATIONS, auths.serialize());
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
    if (job.getConfiguration().getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IllegalStateException("Instance info can only be set once per job");
    job.getConfiguration().setBoolean(INSTANCE_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(instanceName, zooKeepers);
    job.getConfiguration().set(INSTANCE_NAME, instanceName);
    job.getConfiguration().set(ZOOKEEPERS, zooKeepers);
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
    job.getConfiguration().setBoolean(INSTANCE_HAS_BEEN_SET, true);
    job.getConfiguration().setBoolean(MOCK, true);
    job.getConfiguration().set(INSTANCE_NAME, instanceName);
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
    ArgumentChecker.notNull(ranges);
    ArrayList<String> rangeStrings = new ArrayList<String>(ranges.size());
    try {
      for (Range r : ranges) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        r.write(new DataOutputStream(baos));
        rangeStrings.add(new String(Base64.encodeBase64(baos.toByteArray())));
      }
    } catch (IOException ex) {
      throw new IllegalArgumentException("Unable to encode ranges to Base64", ex);
    }
    job.getConfiguration().setStrings(RANGES, rangeStrings.toArray(new String[0]));
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
    job.getConfiguration().setBoolean(AUTO_ADJUST_RANGES, enableFeature);
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
    job.getConfiguration().setBoolean(READ_OFFLINE, enableFeature);
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
    ArgumentChecker.notNull(columnFamilyColumnQualifierPairs);
    ArrayList<String> columnStrings = new ArrayList<String>(columnFamilyColumnQualifierPairs.size());
    for (Pair<Text,Text> column : columnFamilyColumnQualifierPairs) {
      if (column.getFirst() == null)
        throw new IllegalArgumentException("Column family can not be null");
      
      String col = new String(Base64.encodeBase64(TextUtil.getBytes(column.getFirst())));
      if (column.getSecond() != null)
        col += ":" + new String(Base64.encodeBase64(TextUtil.getBytes(column.getSecond())));
      columnStrings.add(col);
    }
    job.getConfiguration().setStrings(COLUMNS, columnStrings.toArray(new String[0]));
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
    ArgumentChecker.notNull(level);
    log.setLevel(level);
    job.getConfiguration().setInt(LOGLEVEL, level.toInt());
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
    // First check to see if anything has been set already
    String iterators = job.getConfiguration().get(ITERATORS);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String newIter;
    try {
      cfg.write(new DataOutputStream(baos));
      newIter = new String(Base64.encodeBase64(baos.toByteArray()));
      baos.close();
    } catch (IOException e) {
      throw new IllegalArgumentException("unable to serialize IteratorSetting");
    }
    
    // No iterators specified yet, create a new string
    if (iterators == null || iterators.isEmpty()) {
      iterators = newIter;
    } else {
      // append the next iterator & reset
      iterators = iterators.concat(ITERATORS_DELIM + newIter);
    }
    // Store the iterators w/ the job
    job.getConfiguration().set(ITERATORS, iterators);
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
    return context.getConfiguration().getBoolean(ISOLATED, false);
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
    return context.getConfiguration().getBoolean(LOCAL_ITERATORS, false);
  }
  
  /**
   * Gets the user name from the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the user name
   * @since 1.5.0
   * @see #setInputInfo(Job, String, byte[], String, Authorizations)
   */
  protected static String getUsername(JobContext context) {
    return context.getConfiguration().get(USERNAME);
  }
  
  /**
   * Gets the password from the configuration. WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to
   * provide a charset safe conversion to a string, and is not intended to be secure.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the decoded user password
   * @since 1.5.0
   * @see #setInputInfo(Job, String, byte[], String, Authorizations)
   */
  protected static byte[] getPassword(JobContext context) {
    return Base64.decodeBase64(context.getConfiguration().get(PASSWORD, "").getBytes());
  }
  
  /**
   * Gets the table name from the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the table name
   * @since 1.5.0
   * @see #setInputInfo(Job, String, byte[], String, Authorizations)
   */
  protected static String getTablename(JobContext context) {
    return context.getConfiguration().get(TABLE_NAME);
  }
  
  /**
   * Gets the authorizations to set for the scans from the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the Accumulo scan authorizations
   * @since 1.5.0
   * @see #setInputInfo(Job, String, byte[], String, Authorizations)
   */
  protected static Authorizations getAuthorizations(JobContext context) {
    String authString = context.getConfiguration().get(AUTHORIZATIONS);
    return authString == null ? Constants.NO_AUTHS : new Authorizations(authString.getBytes());
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
    if (context.getConfiguration().getBoolean(MOCK, false))
      return new MockInstance(context.getConfiguration().get(INSTANCE_NAME));
    return new ZooKeeperInstance(context.getConfiguration().get(INSTANCE_NAME), context.getConfiguration().get(ZOOKEEPERS));
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
    if (context.getConfiguration().getBoolean(MOCK, false))
      return new MockTabletLocator();
    Instance instance = getInstance(context);
    String username = getUsername(context);
    byte[] password = getPassword(context);
    String tableName = getTablename(context);
    return TabletLocator.getInstance(instance, new AuthInfo(username, ByteBuffer.wrap(password), instance.getInstanceID()),
        new Text(Tables.getTableId(instance, tableName)));
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
    ArrayList<Range> ranges = new ArrayList<Range>();
    for (String rangeString : context.getConfiguration().getStringCollection(RANGES)) {
      ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(rangeString.getBytes()));
      Range range = new Range();
      range.readFields(new DataInputStream(bais));
      ranges.add(range);
    }
    return ranges;
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
    Set<Pair<Text,Text>> columns = new HashSet<Pair<Text,Text>>();
    for (String col : context.getConfiguration().getStringCollection(COLUMNS)) {
      int idx = col.indexOf(":");
      Text cf = new Text(idx < 0 ? Base64.decodeBase64(col.getBytes()) : Base64.decodeBase64(col.substring(0, idx).getBytes()));
      Text cq = idx < 0 ? null : new Text(Base64.decodeBase64(col.substring(idx + 1).getBytes()));
      columns.add(new Pair<Text,Text>(cf, cq));
    }
    return columns;
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
    return context.getConfiguration().getBoolean(AUTO_ADJUST_RANGES, true);
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
    return Level.toLevel(context.getConfiguration().getInt(LOGLEVEL, Level.INFO.toInt()));
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
    if (!context.getConfiguration().getBoolean(INPUT_INFO_HAS_BEEN_SET, false))
      throw new IOException("Input info has not been set.");
    if (!context.getConfiguration().getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IOException("Instance info has not been set.");
    // validate that we can connect as configured
    try {
      Connector c = getInstance(context).getConnector(getUsername(context), getPassword(context));
      if (!c.securityOperations().authenticateUser(getUsername(context), getPassword(context)))
        throw new IOException("Unable to authenticate user");
      if (!c.securityOperations().hasTablePermission(getUsername(context), getTablename(context), TablePermission.READ))
        throw new IOException("Unable to access table");
      
      if (!usesLocalIterators(context)) {
        // validate that any scan-time iterators can be loaded by the the tablet servers
        for (IteratorSetting iter : getIterators(context)) {
          if (!c.instanceOperations().testClassLoad(iter.getIteratorClass(), SortedKeyValueIterator.class.getName()))
            throw new AccumuloException("Servers are unable to load " + iter.getIteratorClass() + " as a " + SortedKeyValueIterator.class.getName());
        }
      }
      
    } catch (AccumuloException e) {
      throw new IOException(e);
    } catch (AccumuloSecurityException e) {
      throw new IOException(e);
    }
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
    return context.getConfiguration().getBoolean(READ_OFFLINE, false);
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
    
    String iterators = context.getConfiguration().get(ITERATORS);
    
    // If no iterators are present, return an empty list
    if (iterators == null || iterators.isEmpty())
      return new ArrayList<IteratorSetting>();
    
    // Compose the set of iterators encoded in the job configuration
    StringTokenizer tokens = new StringTokenizer(context.getConfiguration().get(ITERATORS), ITERATORS_DELIM);
    List<IteratorSetting> list = new ArrayList<IteratorSetting>();
    try {
      while (tokens.hasMoreTokens()) {
        String itstring = tokens.nextToken();
        ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(itstring.getBytes()));
        list.add(new IteratorSetting(new DataInputStream(bais)));
        bais.close();
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("couldn't decode iterator settings");
    }
    return list;
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
      String user = getUsername(attempt);
      byte[] password = getPassword(attempt);
      Authorizations authorizations = getAuthorizations(attempt);
      
      try {
        log.debug("Creating connector with user: " + user);
        Connector conn = instance.getConnector(user, password);
        log.debug("Creating scanner for table: " + getTablename(attempt));
        log.debug("Authorizations are: " + authorizations);
        if (isOfflineScan(attempt)) {
          scanner = new OfflineScanner(instance, new AuthInfo(user, ByteBuffer.wrap(password), instance.getInstanceID()), Tables.getTableId(instance,
              getTablename(attempt)), authorizations);
        } else {
          scanner = conn.createScanner(getTablename(attempt), authorizations);
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
  
  Map<String,Map<KeyExtent,List<Range>>> binOfflineTable(Configuration conf, String tableName, List<Range> ranges) throws TableNotFoundException,
      AccumuloException, AccumuloSecurityException {
    
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    
    Instance instance = getInstance(conf);
    Connector conn = instance.getConnector(getUsername(conf), getPassword(conf));
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
    
    String tableName = getTablename(context);
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
        binnedRanges = binOfflineTable(context.getConfiguration(), tableName, ranges);
        while (binnedRanges == null) {
          // Some tablets were still online, try again
          UtilWaitThread.sleep(100 + (int) (Math.random() * 100)); // sleep randomly between 100 and 200 ms
          binnedRanges = binOfflineTable(context.getConfiguration(), tableName, ranges);
        }
      } else {
        Instance instance = getInstance(context);
        String tableId = null;
        tl = getTabletLocator(context);
        // its possible that the cache could contain complete, but old information about a tables tablets... so clear it
        tl.invalidateCache();
        while (!tl.binRanges(ranges, binnedRanges).isEmpty()) {
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
    
    RangeInputSplit(String table, Range range, String[] locations) {
      this.range = range;
      this.locations = locations;
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
   * @deprecated since 1.5.0;
   * @see #addIterator(Configuration, IteratorSetting)
   * @see #getIteratorOptions(Configuration)
   */
  @Deprecated
  private static final String ITERATORS_OPTIONS = PREFIX + ".iterators.options";
  
  /**
   * @deprecated since 1.5.0; Use {@link #setScanIsolation(Job, boolean)} instead.
   */
  @Deprecated
  public static void setIsolated(Configuration conf, boolean enable) {
    conf.setBoolean(ISOLATED, enable);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setLocalIterators(Job, boolean)} instead.
   */
  @Deprecated
  public static void setLocalIterators(Configuration conf, boolean enable) {
    conf.setBoolean(LOCAL_ITERATORS, enable);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setInputInfo(Job, String, byte[], String, Authorizations)} instead.
   */
  @Deprecated
  public static void setInputInfo(Configuration conf, String user, byte[] passwd, String table, Authorizations auths) {
    if (conf.getBoolean(INPUT_INFO_HAS_BEEN_SET, false))
      throw new IllegalStateException("Input info can only be set once per job");
    conf.setBoolean(INPUT_INFO_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(user, passwd, table);
    conf.set(USERNAME, user);
    conf.set(PASSWORD, new String(Base64.encodeBase64(passwd)));
    conf.set(TABLE_NAME, table);
    if (auths != null && !auths.isEmpty())
      conf.set(AUTHORIZATIONS, auths.serialize());
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setZooKeeperInstance(Job, String, String)} instead.
   */
  @Deprecated
  public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
    if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IllegalStateException("Instance info can only be set once per job");
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(instanceName, zooKeepers);
    conf.set(INSTANCE_NAME, instanceName);
    conf.set(ZOOKEEPERS, zooKeepers);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setMockInstance(Job, String)} instead.
   */
  @Deprecated
  public static void setMockInstance(Configuration conf, String instanceName) {
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    conf.setBoolean(MOCK, true);
    conf.set(INSTANCE_NAME, instanceName);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setRanges(Job, Collection)} instead.
   */
  @Deprecated
  public static void setRanges(Configuration conf, Collection<Range> ranges) {
    ArgumentChecker.notNull(ranges);
    ArrayList<String> rangeStrings = new ArrayList<String>(ranges.size());
    try {
      for (Range r : ranges) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        r.write(new DataOutputStream(baos));
        rangeStrings.add(new String(Base64.encodeBase64(baos.toByteArray())));
      }
    } catch (IOException ex) {
      throw new IllegalArgumentException("Unable to encode ranges to Base64", ex);
    }
    conf.setStrings(RANGES, rangeStrings.toArray(new String[0]));
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setAutoAdjustRanges(Job, boolean)} instead.
   */
  @Deprecated
  public static void disableAutoAdjustRanges(Configuration conf) {
    conf.setBoolean(AUTO_ADJUST_RANGES, false);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #addIterator(Job, IteratorSetting)} to add the {@link VersioningIterator} instead.
   */
  @Deprecated
  public static void setMaxVersions(Configuration conf, int maxVersions) throws IOException {
    if (maxVersions < 1)
      throw new IOException("Invalid maxVersions: " + maxVersions + ".  Must be >= 1");
    // Check to make sure its a legit value
    if (maxVersions >= 1) {
      IteratorSetting vers = new IteratorSetting(0, "vers", VersioningIterator.class);
      VersioningIterator.setMaxVersions(vers, maxVersions);
      addIterator(conf, vers);
    }
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setOfflineTableScan(Job, boolean)} instead.
   */
  @Deprecated
  public static void setScanOffline(Configuration conf, boolean scanOff) {
    conf.setBoolean(READ_OFFLINE, scanOff);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #fetchColumns(Job, Collection)} instead.
   */
  @Deprecated
  public static void fetchColumns(Configuration conf, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    ArgumentChecker.notNull(columnFamilyColumnQualifierPairs);
    ArrayList<String> columnStrings = new ArrayList<String>(columnFamilyColumnQualifierPairs.size());
    for (Pair<Text,Text> column : columnFamilyColumnQualifierPairs) {
      if (column.getFirst() == null)
        throw new IllegalArgumentException("Column family can not be null");
      
      String col = new String(Base64.encodeBase64(TextUtil.getBytes(column.getFirst())));
      if (column.getSecond() != null)
        col += ":" + new String(Base64.encodeBase64(TextUtil.getBytes(column.getSecond())));
      columnStrings.add(col);
    }
    conf.setStrings(COLUMNS, columnStrings.toArray(new String[0]));
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #setLogLevel(Job, Level)} instead.
   */
  @Deprecated
  public static void setLogLevel(Configuration conf, Level level) {
    ArgumentChecker.notNull(level);
    log.setLevel(level);
    conf.setInt(LOGLEVEL, level.toInt());
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #addIterator(Job, IteratorSetting)} instead.
   */
  @Deprecated
  public static void addIterator(Configuration conf, IteratorSetting cfg) {
    // First check to see if anything has been set already
    String iterators = conf.get(ITERATORS);
    
    // No iterators specified yet, create a new string
    if (iterators == null || iterators.isEmpty()) {
      iterators = new AccumuloIterator(cfg.getPriority(), cfg.getIteratorClass(), cfg.getName()).toString();
    } else {
      // append the next iterator & reset
      iterators = iterators.concat(ITERATORS_DELIM + new AccumuloIterator(cfg.getPriority(), cfg.getIteratorClass(), cfg.getName()).toString());
    }
    // Store the iterators w/ the job
    conf.set(ITERATORS, iterators);
    for (Entry<String,String> entry : cfg.getOptions().entrySet()) {
      if (entry.getValue() == null)
        continue;
      
      String iteratorOptions = conf.get(ITERATORS_OPTIONS);
      
      // No options specified yet, create a new string
      if (iteratorOptions == null || iteratorOptions.isEmpty()) {
        iteratorOptions = new AccumuloIteratorOption(cfg.getName(), entry.getKey(), entry.getValue()).toString();
      } else {
        // append the next option & reset
        iteratorOptions = iteratorOptions.concat(ITERATORS_DELIM + new AccumuloIteratorOption(cfg.getName(), entry.getKey(), entry.getValue()));
      }
      
      // Store the options w/ the job
      conf.set(ITERATORS_OPTIONS, iteratorOptions);
    }
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #isIsolated(JobContext)} instead.
   */
  @Deprecated
  protected static boolean isIsolated(Configuration conf) {
    return conf.getBoolean(ISOLATED, false);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #usesLocalIterators(JobContext)} instead.
   */
  @Deprecated
  protected static boolean usesLocalIterators(Configuration conf) {
    return conf.getBoolean(LOCAL_ITERATORS, false);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getUsername(JobContext)} instead.
   */
  @Deprecated
  protected static String getUsername(Configuration conf) {
    return conf.get(USERNAME);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getPassword(JobContext)} instead.
   */
  @Deprecated
  protected static byte[] getPassword(Configuration conf) {
    return Base64.decodeBase64(conf.get(PASSWORD, "").getBytes());
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getTablename(JobContext)} instead.
   */
  @Deprecated
  protected static String getTablename(Configuration conf) {
    return conf.get(TABLE_NAME);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getAuthorizations(JobContext)} instead.
   */
  @Deprecated
  protected static Authorizations getAuthorizations(Configuration conf) {
    String authString = conf.get(AUTHORIZATIONS);
    return authString == null ? Constants.NO_AUTHS : new Authorizations(authString.split(","));
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getInstance(JobContext)} instead.
   */
  @Deprecated
  protected static Instance getInstance(Configuration conf) {
    if (conf.getBoolean(MOCK, false))
      return new MockInstance(conf.get(INSTANCE_NAME));
    return new ZooKeeperInstance(conf.get(INSTANCE_NAME), conf.get(ZOOKEEPERS));
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getTabletLocator(JobContext)} instead.
   */
  @Deprecated
  protected static TabletLocator getTabletLocator(Configuration conf) throws TableNotFoundException {
    if (conf.getBoolean(MOCK, false))
      return new MockTabletLocator();
    Instance instance = getInstance(conf);
    String username = getUsername(conf);
    byte[] password = getPassword(conf);
    String tableName = getTablename(conf);
    return TabletLocator.getInstance(instance, new AuthInfo(username, ByteBuffer.wrap(password), instance.getInstanceID()),
        new Text(Tables.getTableId(instance, tableName)));
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getRanges(JobContext)} instead.
   */
  @Deprecated
  protected static List<Range> getRanges(Configuration conf) throws IOException {
    ArrayList<Range> ranges = new ArrayList<Range>();
    for (String rangeString : conf.getStringCollection(RANGES)) {
      ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(rangeString.getBytes()));
      Range range = new Range();
      range.readFields(new DataInputStream(bais));
      ranges.add(range);
    }
    return ranges;
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getFetchedColumns(JobContext)} instead.
   */
  @Deprecated
  protected static Set<Pair<Text,Text>> getFetchedColumns(Configuration conf) {
    Set<Pair<Text,Text>> columns = new HashSet<Pair<Text,Text>>();
    for (String col : conf.getStringCollection(COLUMNS)) {
      int idx = col.indexOf(":");
      Text cf = new Text(idx < 0 ? Base64.decodeBase64(col.getBytes()) : Base64.decodeBase64(col.substring(0, idx).getBytes()));
      Text cq = idx < 0 ? null : new Text(Base64.decodeBase64(col.substring(idx + 1).getBytes()));
      columns.add(new Pair<Text,Text>(cf, cq));
    }
    return columns;
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getAutoAdjustRanges(JobContext)} instead.
   */
  @Deprecated
  protected static boolean getAutoAdjustRanges(Configuration conf) {
    return conf.getBoolean(AUTO_ADJUST_RANGES, true);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getLogLevel(JobContext)} instead.
   */
  @Deprecated
  protected static Level getLogLevel(Configuration conf) {
    return Level.toLevel(conf.getInt(LOGLEVEL, Level.INFO.toInt()));
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #validateOptions(JobContext)} instead.
   */
  @Deprecated
  protected static void validateOptions(Configuration conf) throws IOException {
    if (!conf.getBoolean(INPUT_INFO_HAS_BEEN_SET, false))
      throw new IOException("Input info has not been set.");
    if (!conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IOException("Instance info has not been set.");
    // validate that we can connect as configured
    try {
      Connector c = getInstance(conf).getConnector(getUsername(conf), getPassword(conf));
      if (!c.securityOperations().authenticateUser(getUsername(conf), getPassword(conf)))
        throw new IOException("Unable to authenticate user");
      if (!c.securityOperations().hasTablePermission(getUsername(conf), getTablename(conf), TablePermission.READ))
        throw new IOException("Unable to access table");
      
      if (!usesLocalIterators(conf)) {
        // validate that any scan-time iterators can be loaded by the the tablet servers
        for (AccumuloIterator iter : getIterators(conf)) {
          if (!c.instanceOperations().testClassLoad(iter.getIteratorClass(), SortedKeyValueIterator.class.getName()))
            throw new AccumuloException("Servers are unable to load " + iter.getIteratorClass() + " as a " + SortedKeyValueIterator.class.getName());
        }
      }
      
    } catch (AccumuloException e) {
      throw new IOException(e);
    } catch (AccumuloSecurityException e) {
      throw new IOException(e);
    }
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #addIterator(Job, IteratorSetting)} to add the {@link VersioningIterator} instead.
   */
  @Deprecated
  protected static int getMaxVersions(Configuration conf) {
    return -1;
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #isOfflineScan(JobContext)} instead.
   */
  @Deprecated
  protected static boolean isOfflineScan(Configuration conf) {
    return conf.getBoolean(READ_OFFLINE, false);
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getIterators(JobContext)} instead.
   */
  @Deprecated
  protected static List<AccumuloIterator> getIterators(Configuration conf) {
    
    String iterators = conf.get(ITERATORS);
    
    // If no iterators are present, return an empty list
    if (iterators == null || iterators.isEmpty())
      return new ArrayList<AccumuloIterator>();
    
    // Compose the set of iterators encoded in the job configuration
    StringTokenizer tokens = new StringTokenizer(conf.get(ITERATORS), ITERATORS_DELIM);
    List<AccumuloIterator> list = new ArrayList<AccumuloIterator>();
    while (tokens.hasMoreTokens()) {
      String itstring = tokens.nextToken();
      list.add(new AccumuloIterator(itstring));
    }
    return list;
  }
  
  /**
   * @deprecated since 1.5.0; Use {@link #getIterators(JobContext)} instead.
   */
  @Deprecated
  protected static List<AccumuloIteratorOption> getIteratorOptions(Configuration conf) {
    String iteratorOptions = conf.get(ITERATORS_OPTIONS);
    
    // If no options are present, return an empty list
    if (iteratorOptions == null || iteratorOptions.isEmpty())
      return new ArrayList<AccumuloIteratorOption>();
    
    // Compose the set of options encoded in the job configuration
    StringTokenizer tokens = new StringTokenizer(conf.get(ITERATORS_OPTIONS), ITERATORS_DELIM);
    List<AccumuloIteratorOption> list = new ArrayList<AccumuloIteratorOption>();
    while (tokens.hasMoreTokens()) {
      String optionString = tokens.nextToken();
      list.add(new AccumuloIteratorOption(optionString));
    }
    return list;
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
