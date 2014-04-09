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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
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
import java.util.NoSuchElementException;
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
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.ContextFactory;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This class allows MapReduce jobs to use Accumulo as the source of data. This input format provides keys and values of type K and V to the Map() and Reduce()
 * functions.
 * 
 * Subclasses must implement the following method: public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws
 * IOException, InterruptedException
 * 
 * This class includes a static class that can be used to create a RecordReader: protected abstract static class RecordReaderBase<K,V> extends RecordReader<K,V>
 * 
 * Subclasses of RecordReaderBase must implement the following method: public boolean nextKeyValue() throws IOException, InterruptedException This method should
 * set the following variables: K currentK V currentV Key currentKey (used for progress reporting) int numKeysRead (used for progress reporting)
 * 
 * See AccumuloInputFormat for an example implementation.
 * 
 * Other static methods are optional
 */

public abstract class InputFormatBase<K,V> extends InputFormat<K,V> {
  protected static final Logger log = Logger.getLogger(InputFormatBase.class);

  private static final String PREFIX = AccumuloInputFormat.class.getSimpleName();
  private static final String INPUT_INFO_HAS_BEEN_SET = PREFIX + ".configured";
  private static final String INSTANCE_HAS_BEEN_SET = PREFIX + ".instanceConfigured";
  private static final String USERNAME = PREFIX + ".username";
  private static final String PASSWORD = PREFIX + ".password";
  private static final String TABLE_NAME = PREFIX + ".tablename";
  private static final String AUTHORIZATIONS = PREFIX + ".authorizations";

  private static final String INSTANCE_NAME = PREFIX + ".instanceName";
  private static final String ZOOKEEPERS = PREFIX + ".zooKeepers";
  private static final String MOCK = ".useMockInstance";

  private static final String RANGES = PREFIX + ".ranges";
  private static final String AUTO_ADJUST_RANGES = PREFIX + ".ranges.autoAdjust";

  private static final String ROW_REGEX = PREFIX + ".regex.row";
  private static final String COLUMN_FAMILY_REGEX = PREFIX + ".regex.cf";
  private static final String COLUMN_QUALIFIER_REGEX = PREFIX + ".regex.cq";
  private static final String VALUE_REGEX = PREFIX + ".regex.value";

  private static final String COLUMNS = PREFIX + ".columns";
  private static final String LOGLEVEL = PREFIX + ".loglevel";

  private static final String ISOLATED = PREFIX + ".isolated";

  private static final String LOCAL_ITERATORS = PREFIX + ".localiters";

  // Used to specify the maximum # of versions of an Accumulo cell value to return
  private static final String MAX_VERSIONS = PREFIX + ".maxVersions";

  // Used for specifying the iterators to be applied
  private static final String ITERATORS = PREFIX + ".iterators";
  private static final String ITERATORS_OPTIONS = PREFIX + ".iterators.options";
  private static final String ITERATORS_DELIM = ",";

  private static final String READ_OFFLINE = PREFIX + ".read.offline";

  /**
   * @deprecated Use {@link #setIsolated(Configuration,boolean)} instead
   */
  @Deprecated
  public static void setIsolated(JobContext job, boolean enable) {
    setIsolated(InputFormatBase.getConfiguration(job), enable);
  }

  /**
   * Enable or disable use of the {@link IsolatedScanner} in this configuration object. By default it is not enabled.
   * 
   * @param conf
   *          The Hadoop configuration object
   * @param enable
   *          if true, enable usage of the IsolatedScanner. Otherwise, disable.
   */
  public static void setIsolated(Configuration conf, boolean enable) {
    conf.setBoolean(ISOLATED, enable);
  }

  /**
   * @deprecated Use {@link #setLocalIterators(Configuration,boolean)} instead
   */
  @Deprecated
  public static void setLocalIterators(JobContext job, boolean enable) {
    setLocalIterators(InputFormatBase.getConfiguration(job), enable);
  }

  /**
   * Enable or disable use of the {@link ClientSideIteratorScanner} in this Configuration object. By default it is not enabled.
   * 
   * @param conf
   *          The Hadoop configuration object
   * @param enable
   *          if true, enable usage of the ClientSideInteratorScanner. Otherwise, disable.
   */
  public static void setLocalIterators(Configuration conf, boolean enable) {
    conf.setBoolean(LOCAL_ITERATORS, enable);
  }

  /**
   * @deprecated Use {@link #setInputInfo(Configuration,String,byte[],String,Authorizations)} instead
   */
  @Deprecated
  public static void setInputInfo(JobContext job, String user, byte[] passwd, String table, Authorizations auths) {
    setInputInfo(InputFormatBase.getConfiguration(job), user, passwd, table, auths);
  }

  /**
   * Initialize the user, table, and authorization information for the configuration object that will be used with an Accumulo InputFormat.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param user
   *          a valid accumulo user
   * @param passwd
   *          the user's password
   * @param table
   *          the table to read
   * @param auths
   *          the authorizations used to restrict data read
   */
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
   * @deprecated Use {@link #setZooKeeperInstance(Configuration,String,String)} instead
   */
  @Deprecated
  public static void setZooKeeperInstance(JobContext job, String instanceName, String zooKeepers) {
    setZooKeeperInstance(InputFormatBase.getConfiguration(job), instanceName, zooKeepers);
  }

  /**
   * Configure a {@link ZooKeeperInstance} for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param instanceName
   *          the accumulo instance name
   * @param zooKeepers
   *          a comma-separated list of zookeeper servers
   */
  public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
    if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IllegalStateException("Instance info can only be set once per job");
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);

    ArgumentChecker.notNull(instanceName, zooKeepers);
    conf.set(INSTANCE_NAME, instanceName);
    conf.set(ZOOKEEPERS, zooKeepers);
  }

  /**
   * @deprecated Use {@link #setMockInstance(Configuration,String)} instead
   */
  @Deprecated
  public static void setMockInstance(JobContext job, String instanceName) {
    setMockInstance(InputFormatBase.getConfiguration(job), instanceName);
  }

  /**
   * Configure a {@link MockInstance} for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param instanceName
   *          the accumulo instance name
   */
  public static void setMockInstance(Configuration conf, String instanceName) {
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    conf.setBoolean(MOCK, true);
    conf.set(INSTANCE_NAME, instanceName);
  }

  /**
   * @deprecated Use {@link #setRanges(Configuration,Collection)} instead
   */
  @Deprecated
  public static void setRanges(JobContext job, Collection<Range> ranges) {
    setRanges(InputFormatBase.getConfiguration(job), ranges);
  }

  /**
   * Set the ranges to map over for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param ranges
   *          the ranges that will be mapped over
   */
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
   * @deprecated Use {@link #disableAutoAdjustRanges(Configuration)} instead
   */
  @Deprecated
  public static void disableAutoAdjustRanges(JobContext job) {
    disableAutoAdjustRanges(InputFormatBase.getConfiguration(job));
  }

  /**
   * Disables the adjustment of ranges for this configuration object. By default, overlapping ranges will be merged and ranges will be fit to existing tablet
   * boundaries. Disabling this adjustment will cause there to be exactly one mapper per range set using {@link #setRanges(Configuration, Collection)}.
   * 
   * @param conf
   *          the Hadoop configuration object
   */
  public static void disableAutoAdjustRanges(Configuration conf) {
    conf.setBoolean(AUTO_ADJUST_RANGES, false);
  }

  /**
   * @deprecated since 1.4 use {@link org.apache.accumulo.core.iterators.user.RegExFilter} and {@link #addIterator(Configuration, IteratorSetting)}
   */
  @Deprecated
  public static enum RegexType {
    ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VALUE
  }

  /**
   * @deprecated since 1.4 use {@link #addIterator(Configuration, IteratorSetting)}
   * @see org.apache.accumulo.core.iterators.user.RegExFilter#setRegexs(IteratorSetting, String, String, String, String, boolean)
   */
  @Deprecated
  public static void setRegex(JobContext job, RegexType type, String regex) {
    ArgumentChecker.notNull(type, regex);
    String key = null;
    switch (type) {
      case ROW:
        key = ROW_REGEX;
        break;
      case COLUMN_FAMILY:
        key = COLUMN_FAMILY_REGEX;
        break;
      case COLUMN_QUALIFIER:
        key = COLUMN_QUALIFIER_REGEX;
        break;
      case VALUE:
        key = VALUE_REGEX;
        break;
      default:
        throw new NoSuchElementException();
    }
    try {
      InputFormatBase.getConfiguration(job).set(key, URLEncoder.encode(regex, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      log.error("Failedd to encode regular expression", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * @deprecated Use {@link #setMaxVersions(Configuration,int)} instead
   */
  @Deprecated
  public static void setMaxVersions(JobContext job, int maxVersions) throws IOException {
    setMaxVersions(InputFormatBase.getConfiguration(job), maxVersions);
  }

  /**
   * Sets the max # of values that may be returned for an individual Accumulo cell. By default, applied before all other Accumulo iterators (highest priority)
   * leveraged in the scan by the record reader. To adjust priority use setIterator() & setIteratorOptions() w/ the VersioningIterator type explicitly.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param maxVersions
   *          the max number of versions per accumulo cell
   * @throws IOException
   *           if maxVersions is < 1
   */
  public static void setMaxVersions(Configuration conf, int maxVersions) throws IOException {
    if (maxVersions < 1)
      throw new IOException("Invalid maxVersions: " + maxVersions + ".  Must be >= 1");
    conf.setInt(MAX_VERSIONS, maxVersions);
  }

  /**
   * <p>
   * Enable reading offline tables. This will make the map reduce job directly read the tables files. If the table is not offline, then the job will fail. If
   * the table comes online during the map reduce job, its likely that the job will fail.
   * 
   * <p>
   * To use this option, the map reduce user will need access to read the accumulo directory in HDFS.
   * 
   * <p>
   * Reading the offline table will create the scan time iterator stack in the map process. So any iterators that are configured for the table will need to be
   * on the mappers classpath.
   * 
   * <p>
   * One way to use this feature is to clone a table, take the clone offline, and use the clone as the input table for a map reduce job. If you plan to map
   * reduce over the data many times, it may be better to the compact the table, clone it, take it offline, and use the clone for all map reduce jobs. The
   * reason to do this is that compaction will reduce each tablet in the table to one file, and its faster to read from one file.
   * 
   * <p>
   * There are two possible advantages to reading a tables file directly out of HDFS. First, you may see better read performance. Second, it will support
   * speculative execution better. When reading an online table speculative execution can put more load on an already slow tablet server.
   * 
   * @param conf
   *          the job
   * @param scanOff
   *          pass true to read offline tables
   */

  public static void setScanOffline(Configuration conf, boolean scanOff) {
    conf.setBoolean(READ_OFFLINE, scanOff);
  }

  /**
   * @deprecated Use {@link #fetchColumns(Configuration,Collection)} instead
   */
  @Deprecated
  public static void fetchColumns(JobContext job, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    fetchColumns(InputFormatBase.getConfiguration(job), columnFamilyColumnQualifierPairs);
  }

  /**
   * Restricts the columns that will be mapped over for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param columnFamilyColumnQualifierPairs
   *          A pair of {@link Text} objects corresponding to column family and column qualifier. If the column qualifier is null, the entire column family is
   *          selected. An empty set is the default and is equivalent to scanning the all columns.
   */
  public static void fetchColumns(Configuration conf, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    String[] columnStrings = serializeColumns(columnFamilyColumnQualifierPairs);
    conf.setStrings(COLUMNS, columnStrings);
  }

  public static String[] serializeColumns(Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
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

    return columnStrings.toArray(new String[0]);
  }

  /**
   * @deprecated Use {@link #setLogLevel(Configuration,Level)} instead
   */
  @Deprecated
  public static void setLogLevel(JobContext job, Level level) {
    setLogLevel(InputFormatBase.getConfiguration(job), level);
  }

  /**
   * Sets the log level for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param level
   *          the logging level
   */
  public static void setLogLevel(Configuration conf, Level level) {
    ArgumentChecker.notNull(level);
    log.setLevel(level);
    conf.setInt(LOGLEVEL, level.toInt());
  }

  /**
   * @deprecated Use {@link #addIterator(Configuration,IteratorSetting)} instead
   */
  @Deprecated
  public static void addIterator(JobContext job, IteratorSetting cfg) {
    addIterator(InputFormatBase.getConfiguration(job), cfg);
  }

  /**
   * Encode an iterator on the input for this configuration object.
   * 
   * @param conf
   *          The Hadoop configuration in which to save the iterator configuration
   * @param cfg
   *          The configuration of the iterator
   */
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
   * Specify an Accumulo iterator type to manage the behavior of the underlying table scan this InputFormat's RecordReader will conduct, w/ priority dictating
   * the order in which specified iterators are applied. Repeat calls to specify multiple iterators are allowed.
   * 
   * @param job
   *          the job
   * @param priority
   *          the priority
   * @param iteratorClass
   *          the iterator class
   * @param iteratorName
   *          the iterator name
   * 
   * @deprecated since 1.4, see {@link #addIterator(Configuration, IteratorSetting)}
   */
  @Deprecated
  public static void setIterator(JobContext job, int priority, String iteratorClass, String iteratorName) {
    // First check to see if anything has been set already
    String iterators = InputFormatBase.getConfiguration(job).get(ITERATORS);

    // No iterators specified yet, create a new string
    if (iterators == null || iterators.isEmpty()) {
      iterators = new AccumuloIterator(priority, iteratorClass, iteratorName).toString();
    } else {
      // append the next iterator & reset
      iterators = iterators.concat(ITERATORS_DELIM + new AccumuloIterator(priority, iteratorClass, iteratorName).toString());
    }
    // Store the iterators w/ the job
    InputFormatBase.getConfiguration(job).set(ITERATORS, iterators);

  }

  /**
   * Specify an option for a named Accumulo iterator, further specifying that iterator's behavior.
   * 
   * @param job
   *          the job
   * @param iteratorName
   *          the iterator name. Should correspond to an iterator set w/ a prior setIterator call.
   * @param key
   *          the key
   * @param value
   *          the value
   * 
   * @deprecated since 1.4, see {@link #addIterator(Configuration, IteratorSetting)}
   */
  @Deprecated
  public static void setIteratorOption(JobContext job, String iteratorName, String key, String value) {
    if (iteratorName == null || key == null || value == null)
      return;

    String iteratorOptions = InputFormatBase.getConfiguration(job).get(ITERATORS_OPTIONS);

    // No options specified yet, create a new string
    if (iteratorOptions == null || iteratorOptions.isEmpty()) {
      iteratorOptions = new AccumuloIteratorOption(iteratorName, key, value).toString();
    } else {
      // append the next option & reset
      iteratorOptions = iteratorOptions.concat(ITERATORS_DELIM + new AccumuloIteratorOption(iteratorName, key, value));
    }

    // Store the options w/ the job
    InputFormatBase.getConfiguration(job).set(ITERATORS_OPTIONS, iteratorOptions);
  }

  /**
   * @deprecated Use {@link #isIsolated(Configuration)} instead
   */
  @Deprecated
  protected static boolean isIsolated(JobContext job) {
    return isIsolated(InputFormatBase.getConfiguration(job));
  }

  /**
   * Determines whether a configuration has isolation enabled.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return true if isolation is enabled, false otherwise
   * @see #setIsolated(Configuration, boolean)
   */
  protected static boolean isIsolated(Configuration conf) {
    return conf.getBoolean(ISOLATED, false);
  }

  /**
   * @deprecated Use {@link #usesLocalIterators(Configuration)} instead
   */
  @Deprecated
  protected static boolean usesLocalIterators(JobContext job) {
    return usesLocalIterators(InputFormatBase.getConfiguration(job));
  }

  /**
   * Determines whether a configuration uses local iterators.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return true if uses local iterators, false otherwise
   * @see #setLocalIterators(Configuration, boolean)
   */
  protected static boolean usesLocalIterators(Configuration conf) {
    return conf.getBoolean(LOCAL_ITERATORS, false);
  }

  /**
   * @deprecated Use {@link #getUsername(Configuration)} instead
   */
  @Deprecated
  protected static String getUsername(JobContext job) {
    return getUsername(InputFormatBase.getConfiguration(job));
  }

  /**
   * Gets the user name from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the user name
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static String getUsername(Configuration conf) {
    return conf.get(USERNAME);
  }

  /**
   * WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to provide a charset safe conversion to a
   * string, and is not intended to be secure.
   * 
   * @deprecated Use {@link #getPassword(Configuration)} instead
   */
  @Deprecated
  protected static byte[] getPassword(JobContext job) {
    return getPassword(InputFormatBase.getConfiguration(job));
  }

  /**
   * Gets the password from the configuration. WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to
   * provide a charset safe conversion to a string, and is not intended to be secure.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the BASE64-encoded password
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static byte[] getPassword(Configuration conf) {
    return Base64.decodeBase64(conf.get(PASSWORD, "").getBytes());
  }

  /**
   * @deprecated Use {@link #getTablename(Configuration)} instead
   */
  @Deprecated
  protected static String getTablename(JobContext job) {
    return getTablename(InputFormatBase.getConfiguration(job));
  }

  /**
   * Gets the table name from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the table name
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static String getTablename(Configuration conf) {
    return conf.get(TABLE_NAME);
  }

  /**
   * @deprecated Use {@link #getAuthorizations(Configuration)} instead
   */
  @Deprecated
  protected static Authorizations getAuthorizations(JobContext job) {
    return getAuthorizations(InputFormatBase.getConfiguration(job));
  }

  /**
   * Gets the authorizations to set for the scans from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the accumulo scan authorizations
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static Authorizations getAuthorizations(Configuration conf) {
    String authString = conf.get(AUTHORIZATIONS);
    return authString == null ? Constants.NO_AUTHS : new Authorizations(authString.split(","));
  }

  /**
   * @deprecated Use {@link #getInstance(Configuration)} instead
   */
  @Deprecated
  protected static Instance getInstance(JobContext job) {
    return getInstance(InputFormatBase.getConfiguration(job));
  }

  /**
   * Initializes an Accumulo {@link Instance} based on the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return an accumulo instance
   * @see #setZooKeeperInstance(Configuration, String, String)
   * @see #setMockInstance(Configuration, String)
   */
  protected static Instance getInstance(Configuration conf) {
    if (conf.getBoolean(MOCK, false))
      return new MockInstance(conf.get(INSTANCE_NAME));
    return new ZooKeeperInstance(conf.get(INSTANCE_NAME), conf.get(ZOOKEEPERS));
  }

  /**
   * @deprecated Use {@link #getTabletLocator(Configuration)} instead
   */
  @Deprecated
  protected static TabletLocator getTabletLocator(JobContext job) throws TableNotFoundException {
    return getTabletLocator(InputFormatBase.getConfiguration(job));
  }

  /**
   * Initializes an Accumulo {@link TabletLocator} based on the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return an accumulo tablet locator
   * @throws TableNotFoundException
   *           if the table name set on the configuration doesn't exist
   */
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
   * @deprecated Use {@link #getRanges(Configuration)} instead
   */
  @Deprecated
  protected static List<Range> getRanges(JobContext job) throws IOException {
    return getRanges(InputFormatBase.getConfiguration(job));
  }

  /**
   * Gets the ranges to scan over from a configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the ranges
   * @throws IOException
   *           if the ranges have been encoded improperly
   * @see #setRanges(Configuration, Collection)
   */
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
   * @deprecated since 1.4 use {@link org.apache.accumulo.core.iterators.user.RegExFilter} and {@link #addIterator(Configuration, IteratorSetting)}
   * @see #setRegex(JobContext, RegexType, String)
   */
  @Deprecated
  protected static String getRegex(JobContext job, RegexType type) {
    String key = null;
    switch (type) {
      case ROW:
        key = ROW_REGEX;
        break;
      case COLUMN_FAMILY:
        key = COLUMN_FAMILY_REGEX;
        break;
      case COLUMN_QUALIFIER:
        key = COLUMN_QUALIFIER_REGEX;
        break;
      case VALUE:
        key = VALUE_REGEX;
        break;
      default:
        throw new NoSuchElementException();
    }
    try {
      String s = InputFormatBase.getConfiguration(job).get(key);
      if (s == null)
        return null;
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error("Failed to decode regular expression", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * @deprecated Use {@link #getFetchedColumns(Configuration)} instead
   */
  @Deprecated
  protected static Set<Pair<Text,Text>> getFetchedColumns(JobContext job) {
    return getFetchedColumns(InputFormatBase.getConfiguration(job));
  }

  /**
   * Gets the columns to be mapped over from this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return a set of columns
   * @see #fetchColumns(Configuration, Collection)
   */
  protected static Set<Pair<Text,Text>> getFetchedColumns(Configuration conf) {
    ArgumentChecker.notNull(conf);

    return deserializeFetchedColumns(conf.getStrings(COLUMNS));
  }

  public static Set<Pair<Text,Text>> deserializeFetchedColumns(String[] serialized) {
    Set<Pair<Text,Text>> columns = new HashSet<Pair<Text,Text>>();

    if (null == serialized) {
      return columns;
    }

    for (String col : serialized) {
      int idx = col.indexOf(":");
      Text cf = new Text(idx < 0 ? Base64.decodeBase64(col.getBytes()) : Base64.decodeBase64(col.substring(0, idx).getBytes()));
      Text cq = idx < 0 ? null : new Text(Base64.decodeBase64(col.substring(idx + 1).getBytes()));
      columns.add(new Pair<Text,Text>(cf, cq));
    }
    return columns;
  }

  /**
   * @deprecated Use {@link #getAutoAdjustRanges(Configuration)} instead
   */
  @Deprecated
  protected static boolean getAutoAdjustRanges(JobContext job) {
    return getAutoAdjustRanges(InputFormatBase.getConfiguration(job));
  }

  /**
   * Determines whether a configuration has auto-adjust ranges enabled.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return true if auto-adjust is enabled, false otherwise
   * @see #disableAutoAdjustRanges(Configuration)
   */
  protected static boolean getAutoAdjustRanges(Configuration conf) {
    return conf.getBoolean(AUTO_ADJUST_RANGES, true);
  }

  /**
   * @deprecated Use {@link #getLogLevel(Configuration)} instead
   */
  @Deprecated
  protected static Level getLogLevel(JobContext job) {
    return getLogLevel(InputFormatBase.getConfiguration(job));
  }

  /**
   * Gets the log level from this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the log level
   * @see #setLogLevel(Configuration, Level)
   */
  protected static Level getLogLevel(Configuration conf) {
    return Level.toLevel(conf.getInt(LOGLEVEL, Level.INFO.toInt()));
  }

  // InputFormat doesn't have the equivalent of OutputFormat's
  // checkOutputSpecs(JobContext job)
  /**
   * @deprecated Use {@link #validateOptions(Configuration)} instead
   */
  @Deprecated
  protected static void validateOptions(JobContext job) throws IOException {
    validateOptions(InputFormatBase.getConfiguration(job));
  }

  // InputFormat doesn't have the equivalent of OutputFormat's
  // checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @throws IOException
   *           if the configuration is improperly configured
   */
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
   * @deprecated Use {@link #getMaxVersions(Configuration)} instead
   */
  @Deprecated
  protected static int getMaxVersions(JobContext job) {
    return getMaxVersions(InputFormatBase.getConfiguration(job));
  }

  /**
   * Gets the maxVersions to use for the {@link VersioningIterator} from this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the max versions, -1 if not configured
   * @see #setMaxVersions(Configuration, int)
   */
  protected static int getMaxVersions(Configuration conf) {
    return conf.getInt(MAX_VERSIONS, -1);
  }

  protected static boolean isOfflineScan(Configuration conf) {
    return conf.getBoolean(READ_OFFLINE, false);
  }

  // Return a list of the iterator settings (for iterators to apply to a scanner)

  /**
   * @deprecated Use {@link #getIterators(Configuration)} instead
   */
  @Deprecated
  protected static List<AccumuloIterator> getIterators(JobContext job) {
    return getIterators(InputFormatBase.getConfiguration(job));
  }

  /**
   * Gets a list of the iterator settings (for iterators to apply to a scanner) from this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return a list of iterators
   * @see #addIterator(Configuration, IteratorSetting)
   */
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
   * @deprecated Use {@link #getIteratorOptions(Configuration)} instead
   */
  @Deprecated
  protected static List<AccumuloIteratorOption> getIteratorOptions(JobContext job) {
    return getIteratorOptions(InputFormatBase.getConfiguration(job));
  }

  /**
   * Gets a list of the iterator options specified on this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return a list of iterator options
   * @see #addIterator(Configuration, IteratorSetting)
   */
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

  protected abstract static class RecordReaderBase<K,V> extends RecordReader<K,V> {
    protected long numKeysRead;
    protected Iterator<Entry<Key,Value>> scannerIterator;
    private boolean scannerRegexEnabled = false;
    protected RangeInputSplit split;

    /**
     * @deprecated since 1.4, configure {@link org.apache.accumulo.core.iterators.user.RegExFilter} instead.
     */
    @Deprecated
    private void checkAndEnableRegex(String regex, Scanner scanner, String methodName) throws IllegalArgumentException, SecurityException,
        IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException {
      if (regex != null) {
        if (scannerRegexEnabled == false) {
          scanner.setupRegex(PREFIX + ".regex.iterator", 50);
          scannerRegexEnabled = true;
        }
        scanner.getClass().getMethod(methodName, String.class).invoke(scanner, regex);
        log.info("Setting " + methodName + " to " + regex);
      }
    }

    /**
     * @deprecated since 1.4, configure {@link org.apache.accumulo.core.iterators.user.RegExFilter} instead.
     */
    @Deprecated
    protected boolean setupRegex(TaskAttemptContext attempt, Scanner scanner) throws AccumuloException {
      try {
        checkAndEnableRegex(getRegex(attempt, RegexType.ROW), scanner, "setRowRegex");
        checkAndEnableRegex(getRegex(attempt, RegexType.COLUMN_FAMILY), scanner, "setColumnFamilyRegex");
        checkAndEnableRegex(getRegex(attempt, RegexType.COLUMN_QUALIFIER), scanner, "setColumnQualifierRegex");
        checkAndEnableRegex(getRegex(attempt, RegexType.VALUE), scanner, "setValueRegex");
        return true;
      } catch (Exception e) {
        throw new AccumuloException("Can't set up regex for scanner");
      }
    }

    /**
     * Apply the configured iterators from the configuration to the scanner.
     * 
     * @param scanner
     *          the scanner to configure
     * @param iterators
     *          the iterators to configure on the scanner
     * @param options
     *          options for each configured iterator
     */
    protected void setupIterators(Scanner scanner, List<AccumuloIterator> iterators, List<AccumuloIteratorOption> options)
        throws AccumuloException {

      Map<String,IteratorSetting> scanIterators = new HashMap<String,IteratorSetting>();
      for (AccumuloIterator iterator : iterators) {
        scanIterators.put(iterator.getIteratorName(), new IteratorSetting(iterator.getPriority(), iterator.getIteratorName(), iterator.getIteratorClass()));
      }
      for (AccumuloIteratorOption option : options) {
        scanIterators.get(option.iteratorName).addOption(option.getKey(), option.getValue());
      }
      for (AccumuloIterator iterator : iterators) {
        scanner.addScanIterator(scanIterators.get(iterator.getIteratorName()));
      }
    }

    /**
     * If maxVersions has been set, configure a {@link VersioningIterator} at priority 0 for this scanner.
     * 
     * @param scanner
     *          the scanner to configure
     * @param maxVersions
     *          the number of versions to return
     */
    protected void setupMaxVersions(Scanner scanner, int maxVersions) {
      // Check to make sure its a legit value
      if (maxVersions >= 1) {
        IteratorSetting vers = new IteratorSetting(0, "vers", VersioningIterator.class);
        VersioningIterator.setMaxVersions(vers, maxVersions);
        scanner.addScanIterator(vers);
      }
    }

    /**
     * Initialize a scanner over the given input split using this task attempt configuration.
     */
    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
      Scanner scanner;
      split = (RangeInputSplit) inSplit;
      log.debug("Initializing input split: " + split.getRange());
      Configuration conf = InputFormatBase.getConfiguration(attempt);

      Instance instance = split.getInstance();
      if (null == instance) {
        instance = getInstance(conf);
      }

      String user = split.getUsername();
      if (null == user) {
        user = getUsername(conf);
      }

      byte[] password = split.getPassword();
      if (null == password) {
        password = getPassword(conf);
      }

      Authorizations authorizations = split.getAuths();
      if (null == authorizations) {
        authorizations = getAuthorizations(conf);
      }

      String table = split.getTable();
      if (null == table) {
        table = getTablename(conf);
      }
      
      Boolean isOffline = split.isOffline();
      if (null == isOffline) {
        isOffline = isOfflineScan(conf);
      }

      Boolean isIsolated = split.isIsolatedScan();
      if (null == isIsolated) {
        isIsolated = isIsolated(conf);
      }

      Boolean usesLocalIterators = split.usesLocalIterators();
      if (null == usesLocalIterators) {
        usesLocalIterators = usesLocalIterators(conf);
      }

      String rowRegex = split.getRowRegex();
      if (null == rowRegex) {
        rowRegex = conf.get(ROW_REGEX);
      }

      String colfRegex = split.getColfamRegex();
      if (null == colfRegex) {
        colfRegex = conf.get(COLUMN_FAMILY_REGEX);
      }

      String colqRegex = split.getColqualRegex();
      if (null == colqRegex) {
        colqRegex = conf.get(COLUMN_QUALIFIER_REGEX);
      }

      String valueRegex = split.getValueRegex();
      if (null == valueRegex) {
        valueRegex = conf.get(VALUE_REGEX);
      }

      Integer maxVersions = split.getMaxVersions();
      if (null == maxVersions) {
        maxVersions = getMaxVersions(conf);
      }
      
      List<AccumuloIterator> iterators = split.getIterators();
      if (null == iterators) {
        iterators = getIterators(conf);
      }
      
      List<AccumuloIteratorOption> options = split.getOptions();
      if (null == options) {
        options = getIteratorOptions(conf);
      }
      
      Set<Pair<Text,Text>> columns = split.getFetchedColumns();
      if (null == columns) {
        columns = getFetchedColumns(conf);
      }

      try {
        log.debug("Creating connector with user: " + user);
        Connector conn = instance.getConnector(user, password);
        log.debug("Creating scanner for table: " + table);
        log.debug("Authorizations are: " + authorizations);
        if (isOffline) {
          scanner = new OfflineScanner(instance, new AuthInfo(user, ByteBuffer.wrap(password), instance.getInstanceID()), Tables.getTableId(instance, table),
              authorizations);
        } else {
          scanner = conn.createScanner(table, authorizations);
        }
        if (isIsolated) {
          log.info("Creating isolated scanner");
          scanner = new IsolatedScanner(scanner);
        }
        if (usesLocalIterators) {
          log.info("Using local iterators");
          scanner = new ClientSideIteratorScanner(scanner);
        }
        setupMaxVersions(scanner, maxVersions);
        if (rowRegex != null || colfRegex != null || colqRegex != null || valueRegex != null) {
          IteratorSetting is = new IteratorSetting(50, RegExFilter.class);
          RegExFilter.setRegexs(is, rowRegex, colfRegex, colqRegex, valueRegex, false);
          scanner.addScanIterator(is);
        }
        setupIterators(scanner, iterators, options);
      } catch (Exception e) {
        throw new IOException(e);
      }

      // setup a scanner within the bounds of this split
      for (Pair<Text,Text> c : columns) {
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

  Map<String,Map<KeyExtent,List<Range>>> binOfflineTable(JobContext job, String tableName, List<Range> ranges) throws TableNotFoundException,
      AccumuloException, AccumuloSecurityException {

    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();

    Instance instance = getInstance(InputFormatBase.getConfiguration(job));
    Connector conn = instance.getConnector(getUsername(InputFormatBase.getConfiguration(job)), getPassword(InputFormatBase.getConfiguration(job)));
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
      ColumnFQ.fetch(scanner, Constants.METADATA_PREV_ROW_COLUMN);
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
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    Configuration conf = InputFormatBase.getConfiguration(job);

    log.setLevel(getLogLevel(conf));
    validateOptions(conf);

    String tableName = getTablename(conf);
    boolean autoAdjust = getAutoAdjustRanges(conf);
    List<Range> ranges = autoAdjust ? Range.mergeOverlapping(getRanges(conf)) : getRanges(conf);
    boolean offline = isOfflineScan(conf);
    boolean isolated = isIsolated(conf);
    boolean localIterators = usesLocalIterators(conf);
    boolean mockInstance = conf.getBoolean(MOCK, false);
    int maxVersions = getMaxVersions(conf);
    String rowRegex = conf.get(ROW_REGEX), colfamRegex = conf.get(COLUMN_FAMILY_REGEX), colqualRegex = conf.get(COLUMN_QUALIFIER_REGEX), valueRegex = conf
        .get(VALUE_REGEX);
    Set<Pair<Text,Text>> fetchedColumns = getFetchedColumns(conf);
    Authorizations auths = getAuthorizations(conf);
    byte[] password = getPassword(conf);
    String username = getUsername(conf);
    Instance instance = getInstance(conf);
    List<AccumuloIterator> iterators = getIterators(conf);
    List<AccumuloIteratorOption> options = getIteratorOptions(conf);
    Level logLevel = getLogLevel(conf);

    if (ranges.isEmpty()) {
      ranges = new ArrayList<Range>(1);
      ranges.add(new Range());
    }

    // get the metadata information for these ranges
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    TabletLocator tl;
    try {
      if (isOfflineScan(InputFormatBase.getConfiguration(job))) {
        binnedRanges = binOfflineTable(job, tableName, ranges);
        while (binnedRanges == null) {
          // Some tablets were still online, try again
          UtilWaitThread.sleep(100 + (int) (Math.random() * 100)); // sleep randomly between 100 and 200 ms
          binnedRanges = binOfflineTable(job, tableName, ranges);
        }
      } else {
        String tableId = null;
        tl = getTabletLocator(InputFormatBase.getConfiguration(job));
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
            // divide ranges into smaller ranges, based on the
            // tablets
            splits.add(new RangeInputSplit(ke.clip(r), new String[] {location}));
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
        splits.add(new RangeInputSplit(entry.getKey(), entry.getValue().toArray(new String[0])));

    for (InputSplit inputSplit : splits) {
      RangeInputSplit split = (RangeInputSplit) inputSplit;

      split.setTable(tableName);
      split.setOffline(offline);
      split.setIsolatedScan(isolated);
      split.setUsesLocalIterators(localIterators);
      split.setMockInstance(mockInstance);
      split.setMaxVersions(maxVersions);
      split.setRowRegex(rowRegex);
      split.setColfamRegex(colfamRegex);
      split.setColqualRegex(colqualRegex);
      split.setValueRegex(valueRegex);
      split.setFetchedColumns(fetchedColumns);
      split.setUsername(username);
      split.setPassword(password);
      split.setInstanceName(instance.getInstanceName());
      split.setZooKeepers(instance.getZooKeepers());
      split.setAuths(auths);
      split.setIterators(iterators);
      split.setOptions(options);
      split.setLogLevel(logLevel);
    }

    return splits;
  }

  /**
   * The Class IteratorSetting. Encapsulates specifics for an Accumulo iterator's name & priority.
   */
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
   * The Class AccumuloIteratorOption. Encapsulates specifics for an Accumulo iterator's optional configuration details - associated via the iteratorName.
   */
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

  public static Configuration getConfiguration(JobContext context) {
    return ContextFactory.getConfiguration(context);
  }
}
