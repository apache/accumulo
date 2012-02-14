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
import java.lang.reflect.InvocationTargetException;
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
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
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
  
  /**
   * Enable or disable use of the {@link IsolatedScanner}. By default it is not enabled.
   * 
   * @param job
   * @param enable
   * @deprecated Use {@link #setIsolated(Configuration,boolean)} instead
   */
  public static void setIsolated(JobContext job, boolean enable) {
    setIsolated(job.getConfiguration(), enable);
  }

  /**
   * Enable or disable use of the {@link IsolatedScanner}. By default it is not enabled.
   * 
   * @param conf
   * @param enable
   */
  public static void setIsolated(Configuration conf, boolean enable) {
    conf.setBoolean(ISOLATED, enable);
  }
  
  /**
   * Enable or disable use of the {@link ClientSideIteratorScanner}. By default it is not enabled.
   * 
   * @param job
   * @param enable
   * @deprecated Use {@link #setLocalIterators(Configuration,boolean)} instead
   */
  public static void setLocalIterators(JobContext job, boolean enable) {
    setLocalIterators(job.getConfiguration(), enable);
  }

  /**
   * Enable or disable use of the {@link ClientSideIteratorScanner}. By default it is not enabled.
   * 
   * @param job
   * @param enable
   */
  public static void setLocalIterators(Configuration conf, boolean enable) {
    conf.setBoolean(LOCAL_ITERATORS, enable);
  }
  
  /**
   * @deprecated Use {@link #setInputInfo(Configuration,String,byte[],String,Authorizations)} instead
   */
  public static void setInputInfo(JobContext job, String user, byte[] passwd, String table, Authorizations auths) {
    setInputInfo(job.getConfiguration(), user, passwd, table, auths);
  }

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
  public static void setZooKeeperInstance(JobContext job, String instanceName, String zooKeepers) {
    setZooKeeperInstance(job.getConfiguration(), instanceName, zooKeepers);
  }

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
  public static void setMockInstance(JobContext job, String instanceName) {
    setMockInstance(job.getConfiguration(), instanceName);
  }

  public static void setMockInstance(Configuration conf, String instanceName) {
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    conf.setBoolean(MOCK, true);
    conf.set(INSTANCE_NAME, instanceName);
  }
  
  /**
   * @deprecated Use {@link #setRanges(Configuration,Collection<Range>)} instead
   */
  public static void setRanges(JobContext job, Collection<Range> ranges) {
    setRanges(job.getConfiguration(), ranges);
  }

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
  public static void disableAutoAdjustRanges(JobContext job) {
    disableAutoAdjustRanges(job.getConfiguration());
  }

  public static void disableAutoAdjustRanges(Configuration conf) {
    conf.setBoolean(AUTO_ADJUST_RANGES, false);
  }
  
  public static enum RegexType {
    ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VALUE
  }
  
  /**
   * @deprecated since 1.4 {@link #addIterator(Configuration, IteratorSetting)}
   * @see org.apache.accumulo.core.iterators.user.RegExFilter#setRegexs(IteratorSetting, String, String, String, String, boolean)
   * @param job
   * @param type
   * @param regex
   */
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
      job.getConfiguration().set(key, URLEncoder.encode(regex, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      log.error("Failedd to encode regular expression", e);
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Sets the max # of values that may be returned for an individual Accumulo cell. By default, applied before all other Accumulo iterators (highest priority)
   * leveraged in the scan by the record reader. To adjust priority use setIterator() & setIteratorOptions() w/ the VersioningIterator type explicitly.
   * 
   * @param job
   *          the job
   * @param maxVersions
   *          the max versions
   * @throws IOException
   * @deprecated Use {@link #setMaxVersions(Configuration,int)} instead
   */
  public static void setMaxVersions(JobContext job, int maxVersions) throws IOException {
    setMaxVersions(job.getConfiguration(), maxVersions);
  }

  /**
   * Sets the max # of values that may be returned for an individual Accumulo cell. By default, applied before all other Accumulo iterators (highest priority)
   * leveraged in the scan by the record reader. To adjust priority use setIterator() & setIteratorOptions() w/ the VersioningIterator type explicitly.
   * 
   * @param conf
   *          the job
   * @param maxVersions
   *          the max versions
   * @throws IOException
   */
  public static void setMaxVersions(Configuration conf, int maxVersions) throws IOException {
    if (maxVersions < 1)
      throw new IOException("Invalid maxVersions: " + maxVersions + ".  Must be >= 1");
    conf.setInt(MAX_VERSIONS, maxVersions);
  }
  
  /**
   * 
   * @param columnFamilyColumnQualifierPairs
   *          A pair of {@link Text} objects corresponding to column family and column qualifier. If the column qualifier is null, the entire column family is
   *          selected. An empty set is the default and is equivalent to scanning the all columns.
   * @deprecated Use {@link #fetchColumns(Configuration,Collection<Pair<Text, Text>>)} instead
   */
  public static void fetchColumns(JobContext job, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    fetchColumns(job.getConfiguration(), columnFamilyColumnQualifierPairs);
  }
  
  /**
   * 
   * @param columnFamilyColumnQualifierPairs
   *          A pair of {@link Text} objects corresponding to column family and column qualifier. If the column qualifier is null, the entire column family is
   *          selected. An empty set is the default and is equivalent to scanning the all columns.
   */
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
   * @deprecated Use {@link #setLogLevel(Configuration,Level)} instead
   */
  public static void setLogLevel(JobContext job, Level level) {
    setLogLevel(job.getConfiguration(), level);
  }
  
  public static void setLogLevel(Configuration conf, Level level) {
    ArgumentChecker.notNull(level);
    log.setLevel(level);
    conf.setInt(LOGLEVEL, level.toInt());
  }
  
  /**
   * Encode an iterator on the input.
   * 
   * @param job
   *          The job in which to save the iterator configuration
   * @param priority
   *          The priority of the iterator
   * @param cfg
   *          The configuration of the iterator
   * @deprecated Use {@link #addIterator(Configuration,IteratorSetting)} instead
   */
  public static void addIterator(JobContext job, IteratorSetting cfg) {
    addIterator(job.getConfiguration(), cfg);
  }

  /**
   * Encode an iterator on the input.
   * 
   * @param conf
   *          The job in which to save the iterator configuration
   * @param priority
   *          The priority of the iterator
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
    for (Entry<String,String> entry : cfg.getProperties().entrySet()) {
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
   * Specify an Accumulo iterator type to manage the behavior of the underlying table scan this InputFormat's Record Reader will conduct, w/ priority dictating
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
  public static void setIterator(JobContext job, int priority, String iteratorClass, String iteratorName) {
    // First check to see if anything has been set already
    String iterators = job.getConfiguration().get(ITERATORS);
    
    // No iterators specified yet, create a new string
    if (iterators == null || iterators.isEmpty()) {
      iterators = new AccumuloIterator(priority, iteratorClass, iteratorName).toString();
    } else {
      // append the next iterator & reset
      iterators = iterators.concat(ITERATORS_DELIM + new AccumuloIterator(priority, iteratorClass, iteratorName).toString());
    }
    // Store the iterators w/ the job
    job.getConfiguration().set(ITERATORS, iterators);
    
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
  public static void setIteratorOption(JobContext job, String iteratorName, String key, String value) {
    if (iteratorName == null || key == null || value == null)
      return;
    
    String iteratorOptions = job.getConfiguration().get(ITERATORS_OPTIONS);
    
    // No options specified yet, create a new string
    if (iteratorOptions == null || iteratorOptions.isEmpty()) {
      iteratorOptions = new AccumuloIteratorOption(iteratorName, key, value).toString();
    } else {
      // append the next option & reset
      iteratorOptions = iteratorOptions.concat(ITERATORS_DELIM + new AccumuloIteratorOption(iteratorName, key, value));
    }
    
    // Store the options w/ the job
    job.getConfiguration().set(ITERATORS_OPTIONS, iteratorOptions);
  }
  
  /**
   * @deprecated Use {@link #isIsolated(Configuration)} instead
   */
  protected static boolean isIsolated(JobContext job) {
    return isIsolated(job.getConfiguration());
  }

  protected static boolean isIsolated(Configuration conf) {
    return conf.getBoolean(ISOLATED, false);
  }
  
  /**
   * @deprecated Use {@link #usesLocalIterators(Configuration)} instead
   */
  protected static boolean usesLocalIterators(JobContext job) {
    return usesLocalIterators(job.getConfiguration());
  }

  protected static boolean usesLocalIterators(Configuration conf) {
    return conf.getBoolean(LOCAL_ITERATORS, false);
  }
  
  /**
   * @deprecated Use {@link #getUsername(Configuration)} instead
   */
  protected static String getUsername(JobContext job) {
    return getUsername(job.getConfiguration());
  }

  protected static String getUsername(Configuration conf) {
    return conf.get(USERNAME);
  }
  
  /**
   * WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to provide a charset safe conversion to a
   * string, and is not intended to be secure.
   * @deprecated Use {@link #getPassword(Configuration)} instead
   */
  protected static byte[] getPassword(JobContext job) {
    return getPassword(job.getConfiguration());
  }

  /**
   * WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to provide a charset safe conversion to a
   * string, and is not intended to be secure.
   */
  protected static byte[] getPassword(Configuration conf) {
    return Base64.decodeBase64(conf.get(PASSWORD, "").getBytes());
  }
  
  /**
   * @deprecated Use {@link #getTablename(Configuration)} instead
   */
  protected static String getTablename(JobContext job) {
    return getTablename(job.getConfiguration());
  }

  protected static String getTablename(Configuration conf) {
    return conf.get(TABLE_NAME);
  }
  
  /**
   * @deprecated Use {@link #getAuthorizations(Configuration)} instead
   */
  protected static Authorizations getAuthorizations(JobContext job) {
    return getAuthorizations(job.getConfiguration());
  }

  protected static Authorizations getAuthorizations(Configuration conf) {
    String authString = conf.get(AUTHORIZATIONS);
    return authString == null ? Constants.NO_AUTHS : new Authorizations(authString.split(","));
  }
  
  /**
   * @deprecated Use {@link #getInstance(Configuration)} instead
   */
  protected static Instance getInstance(JobContext job) {
    return getInstance(job.getConfiguration());
  }

  protected static Instance getInstance(Configuration conf) {
    if (conf.getBoolean(MOCK, false))
      return new MockInstance(conf.get(INSTANCE_NAME));
    return new ZooKeeperInstance(conf.get(INSTANCE_NAME), conf.get(ZOOKEEPERS));
  }
  
  /**
   * @deprecated Use {@link #getTabletLocator(Configuration)} instead
   */
  protected static TabletLocator getTabletLocator(JobContext job) throws TableNotFoundException {
    return getTabletLocator(job.getConfiguration());
  }

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
  protected static List<Range> getRanges(JobContext job) throws IOException {
    return getRanges(job.getConfiguration());
  }

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
   * @deprecated Use {@link #getRegex(Configuration,RegexType)} instead
   */
  protected static String getRegex(JobContext job, RegexType type) {
    return getRegex(job.getConfiguration(), type);
  }

  protected static String getRegex(Configuration conf, RegexType type) {
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
      String s = conf.get(key);
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
  protected static Set<Pair<Text,Text>> getFetchedColumns(JobContext job) {
    return getFetchedColumns(job.getConfiguration());
  }

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
   * @deprecated Use {@link #getAutoAdjustRanges(Configuration)} instead
   */
  protected static boolean getAutoAdjustRanges(JobContext job) {
    return getAutoAdjustRanges(job.getConfiguration());
  }

  protected static boolean getAutoAdjustRanges(Configuration conf) {
    return conf.getBoolean(AUTO_ADJUST_RANGES, true);
  }
  
  /**
   * @deprecated Use {@link #getLogLevel(Configuration)} instead
   */
  protected static Level getLogLevel(JobContext job) {
    return getLogLevel(job.getConfiguration());
  }

  protected static Level getLogLevel(Configuration conf) {
    return Level.toLevel(conf.getInt(LOGLEVEL, Level.INFO.toInt()));
  }
  
  // InputFormat doesn't have the equivalent of OutputFormat's
  // checkOutputSpecs(JobContext job)
  /**
   * @deprecated Use {@link #validateOptions(Configuration)} instead
   */
  protected static void validateOptions(JobContext job) throws IOException {
    validateOptions(job.getConfiguration());
  }

  // InputFormat doesn't have the equivalent of OutputFormat's
  // checkOutputSpecs(JobContext job)
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
  
  // Get the maxVersions the VersionsIterator should be configured with. Return -1 if none.
  /**
   * @deprecated Use {@link #getMaxVersions(Configuration)} instead
   */
  protected static int getMaxVersions(JobContext job) {
    return getMaxVersions(job.getConfiguration());
  }

  // Get the maxVersions the VersionsIterator should be configured with. Return -1 if none.
  protected static int getMaxVersions(Configuration conf) {
    return conf.getInt(MAX_VERSIONS, -1);
  }
  
  // Return a list of the iterator settings (for iterators to apply to a scanner)
  /**
   * @deprecated Use {@link #getIterators(Configuration)} instead
   */
  protected static List<AccumuloIterator> getIterators(JobContext job) {
    return getIterators(job.getConfiguration());
  }

  // Return a list of the iterator settings (for iterators to apply to a scanner)
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
  
  // Return a list of the iterator options specified
  /**
   * @deprecated Use {@link #getIteratorOptions(Configuration)} instead
   */
  protected static List<AccumuloIteratorOption> getIteratorOptions(JobContext job) {
    return getIteratorOptions(job.getConfiguration());
  }

  // Return a list of the iterator options specified
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
    
    @SuppressWarnings("deprecation")
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
     * @deprecated Use {@link #setupRegex(Configuration,Scanner)} instead
     */
    protected boolean setupRegex(TaskAttemptContext attempt, Scanner scanner) throws AccumuloException {
      return setupRegex(attempt.getConfiguration(), scanner);
    }

    protected boolean setupRegex(Configuration conf, Scanner scanner) throws AccumuloException {
      try {
        checkAndEnableRegex(getRegex(conf, RegexType.ROW), scanner, "setRowRegex");
        checkAndEnableRegex(getRegex(conf, RegexType.COLUMN_FAMILY), scanner, "setColumnFamilyRegex");
        checkAndEnableRegex(getRegex(conf, RegexType.COLUMN_QUALIFIER), scanner, "setColumnQualifierRegex");
        checkAndEnableRegex(getRegex(conf, RegexType.VALUE), scanner, "setValueRegex");
        return true;
      } catch (Exception e) {
        throw new AccumuloException("Can't set up regex for scanner");
      }
    }
    
    // Apply the configured iterators from the job to the scanner
    /**
     * @deprecated Use {@link #setupIterators(Configuration,Scanner)} instead
     */
    protected void setupIterators(TaskAttemptContext attempt, Scanner scanner) throws AccumuloException {
      setupIterators(attempt.getConfiguration(), scanner);
    }

    // Apply the configured iterators from the job to the scanner
    protected void setupIterators(Configuration conf, Scanner scanner) throws AccumuloException {
      List<AccumuloIterator> iterators = getIterators(conf);
      List<AccumuloIteratorOption> options = getIteratorOptions(conf);
      
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
    
    // Apply the VersioningIterator at priority 0 based on the job config
    /**
     * @deprecated Use {@link #setupMaxVersions(Configuration,Scanner)} instead
     */
    protected void setupMaxVersions(TaskAttemptContext attempt, Scanner scanner) {
      setupMaxVersions(attempt.getConfiguration(), scanner);
    }

    // Apply the VersioningIterator at priority 0 based on the job config
    protected void setupMaxVersions(Configuration conf, Scanner scanner) {
      int maxVersions = getMaxVersions(conf);
      // Check to make sure its a legit value
      if (maxVersions >= 1) {
        IteratorSetting vers = new IteratorSetting(0, "vers", VersioningIterator.class);
        VersioningIterator.setMaxVersions(vers, maxVersions);
        scanner.addScanIterator(vers);
      }
    }
    
    public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
      Scanner scanner;
      split = (RangeInputSplit) inSplit;
      log.debug("Initializing input split: " + split.range);
      Instance instance = getInstance(attempt.getConfiguration());
      String user = getUsername(attempt.getConfiguration());
      byte[] password = getPassword(attempt.getConfiguration());
      Authorizations authorizations = getAuthorizations(attempt.getConfiguration());
      
      try {
        log.debug("Creating connector with user: " + user);
        Connector conn = instance.getConnector(user, password);
        log.debug("Creating scanner for table: " + getTablename(attempt.getConfiguration()));
        log.debug("Authorizations are: " + authorizations);
        scanner = conn.createScanner(getTablename(attempt.getConfiguration()), authorizations);
        if (isIsolated(attempt.getConfiguration())) {
          log.info("Creating isolated scanner");
          scanner = new IsolatedScanner(scanner);
        }
        if (usesLocalIterators(attempt.getConfiguration())) {
          log.info("Using local iterators");
          scanner = new ClientSideIteratorScanner(scanner);
        }
        setupMaxVersions(attempt.getConfiguration(), scanner);
        setupRegex(attempt.getConfiguration(), scanner);
        setupIterators(attempt.getConfiguration(), scanner);
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      // setup a scanner within the bounds of this split
      for (Pair<Text,Text> c : getFetchedColumns(attempt.getConfiguration())) {
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
    
    public void close() {}
    
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
  
  /**
   * read the metadata table to get tablets of interest these each become a split
   */
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    log.setLevel(getLogLevel(job.getConfiguration()));
    validateOptions(job.getConfiguration());
    
    String tableName = getTablename(job.getConfiguration());
    boolean autoAdjust = getAutoAdjustRanges(job.getConfiguration());
    List<Range> ranges = autoAdjust ? Range.mergeOverlapping(getRanges(job.getConfiguration())) : getRanges(job.getConfiguration());
    
    if (ranges.isEmpty()) {
      ranges = new ArrayList<Range>(1);
      ranges.add(new Range());
    }
    
    // get the metadata information for these ranges
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    TabletLocator tl;
    try {
      tl = getTabletLocator(job.getConfiguration());
      while (!tl.binRanges(ranges, binnedRanges).isEmpty()) {
        log.warn("Unable to locate bins for specified ranges. Retrying.");
        UtilWaitThread.sleep(100 + (int) (Math.random() * 100)); // sleep randomly between 100 and 200 ms
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
    
    public Range getRange() {
      return range;
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
        if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW) != 0) {
          // just look at the row progress
          return getProgress(range.getStartKey().getRowData(), range.getEndKey().getRowData(), currentKey.getRowData());
        } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM) != 0) {
          // just look at the column family progress
          return getProgress(range.getStartKey().getColumnFamilyData(), range.getEndKey().getColumnFamilyData(), currentKey.getColumnFamilyData());
        } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM_COLQUAL) != 0) {
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
    
    public String[] getLocations() throws IOException {
      return locations;
    }
    
    public void readFields(DataInput in) throws IOException {
      range.readFields(in);
      int numLocs = in.readInt();
      locations = new String[numLocs];
      for (int i = 0; i < numLocs; ++i)
        locations[i] = in.readUTF();
    }
    
    public void write(DataOutput out) throws IOException {
      range.write(out);
      out.writeInt(locations.length);
      for (int i = 0; i < locations.length; ++i)
        out.writeUTF(locations[i]);
    }
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
  
}
