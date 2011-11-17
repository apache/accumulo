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
  
  private static final String COLUMNS = PREFIX + ".columns";
  private static final String LOGLEVEL = PREFIX + ".loglevel";
  
  private static final String ISOLATED = PREFIX + ".isolated";
  
  private static final String LOCAL_ITERATORS = PREFIX + ".localiters";
  
  // Used to specify the maximum # of versions of a Accumulo cell value to return
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
   */
  public static void setIsolated(JobContext job, boolean enable) {
    Configuration conf = job.getConfiguration();
    conf.setBoolean(ISOLATED, enable);
  }
  
  /**
   * Enable or disable use of the {@link ClientSideIteratorScanner}. By default it is not enabled.
   * 
   * @param job
   * @param enable
   */
  public static void setLocalIterators(JobContext job, boolean enable) {
    Configuration conf = job.getConfiguration();
    conf.setBoolean(LOCAL_ITERATORS, enable);
  }
  
  public static void setInputInfo(JobContext job, String user, byte[] passwd, String table, Authorizations auths) {
    Configuration conf = job.getConfiguration();
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
  
  public static void setZooKeeperInstance(JobContext job, String instanceName, String zooKeepers) {
    Configuration conf = job.getConfiguration();
    if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IllegalStateException("Instance info can only be set once per job");
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(instanceName, zooKeepers);
    conf.set(INSTANCE_NAME, instanceName);
    conf.set(ZOOKEEPERS, zooKeepers);
  }
  
  public static void setMockInstance(JobContext job, String instanceName) {
    Configuration conf = job.getConfiguration();
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    conf.setBoolean(MOCK, true);
    conf.set(INSTANCE_NAME, instanceName);
  }
  
  public static void setRanges(JobContext job, Collection<Range> ranges) {
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
  
  public static void disableAutoAdjustRanges(JobContext job) {
    job.getConfiguration().setBoolean(AUTO_ADJUST_RANGES, false);
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
   */
  public static void setMaxVersions(JobContext job, int maxVersions) throws IOException {
    if (maxVersions < 1)
      throw new IOException("Invalid maxVersions: " + maxVersions + ".  Must be >= 1");
    job.getConfiguration().setInt(MAX_VERSIONS, maxVersions);
  }
  
  /**
   * 
   * @param columnFamilyColumnQualifierPairs
   *          A pair of {@link Text} objects corresponding to column family and column qualifier. If the column qualifier is null, the entire column family is
   *          selected. An empty set is the default and is equivalent to scanning the all columns.
   */
  public static void fetchColumns(JobContext job, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
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
  
  public static void setLogLevel(JobContext job, Level level) {
    ArgumentChecker.notNull(level);
    log.setLevel(level);
    job.getConfiguration().setInt(LOGLEVEL, level.toInt());
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
   */
  public static void addIterator(JobContext job, IteratorSetting cfg) {
    // First check to see if anything has been set already
    String iterators = job.getConfiguration().get(ITERATORS);
    
    // No iterators specified yet, create a new string
    if (iterators == null || iterators.isEmpty()) {
      iterators = new AccumuloIterator(cfg.getPriority(), cfg.getIteratorClass(), cfg.getName()).toString();
    } else {
      // append the next iterator & reset
      iterators = iterators.concat(ITERATORS_DELIM + new AccumuloIterator(cfg.getPriority(), cfg.getIteratorClass(), cfg.getName()).toString());
    }
    // Store the iterators w/ the job
    job.getConfiguration().set(ITERATORS, iterators);
    for (Entry<String,String> entry : cfg.getProperties().entrySet()) {
      if (entry.getValue() == null)
        continue;
      
      String iteratorOptions = job.getConfiguration().get(ITERATORS_OPTIONS);
      
      // No options specified yet, create a new string
      if (iteratorOptions == null || iteratorOptions.isEmpty()) {
        iteratorOptions = new AccumuloIteratorOption(cfg.getName(), entry.getKey(), entry.getValue()).toString();
      } else {
        // append the next option & reset
        iteratorOptions = iteratorOptions.concat(ITERATORS_DELIM + new AccumuloIteratorOption(cfg.getName(), entry.getKey(), entry.getValue()));
      }
      
      // Store the options w/ the job
      job.getConfiguration().set(ITERATORS_OPTIONS, iteratorOptions);
    }
  }
  
  /**
   * Specify a Accumulo iterator type to manage the behavior of the underlying table scan this InputFormat's Record Reader will conduct, w/ priority dictating
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
   * @deprecated since 1.4, see {@link #addIterator(JobContext, IteratorSetting)}
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
   * @deprecated since 1.4, see {@link #addIterator(JobContext, IteratorSetting)}
   */
  public static void setIteratorOption(JobContext job, String iteratorName, String key, String value) {
    if (value == null)
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
  
  protected static boolean isIsolated(JobContext job) {
    return job.getConfiguration().getBoolean(ISOLATED, false);
  }
  
  protected static boolean usesLocalIterators(JobContext job) {
    return job.getConfiguration().getBoolean(LOCAL_ITERATORS, false);
  }
  
  protected static String getUsername(JobContext job) {
    return job.getConfiguration().get(USERNAME);
  }
  
  /**
   * WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to provide a charset safe conversion to a
   * string, and is not intended to be secure.
   */
  protected static byte[] getPassword(JobContext job) {
    return Base64.decodeBase64(job.getConfiguration().get(PASSWORD, "").getBytes());
  }
  
  protected static String getTablename(JobContext job) {
    return job.getConfiguration().get(TABLE_NAME);
  }
  
  protected static Authorizations getAuthorizations(JobContext job) {
    String authString = job.getConfiguration().get(AUTHORIZATIONS);
    return authString == null ? Constants.NO_AUTHS : new Authorizations(authString.split(","));
  }
  
  protected static Instance getInstance(JobContext job) {
    Configuration conf = job.getConfiguration();
    if (conf.getBoolean(MOCK, false))
      return new MockInstance(conf.get(INSTANCE_NAME));
    return new ZooKeeperInstance(conf.get(INSTANCE_NAME), conf.get(ZOOKEEPERS));
  }
  
  protected static TabletLocator getTabletLocator(JobContext job) throws TableNotFoundException {
    if (job.getConfiguration().getBoolean(MOCK, false))
      return new MockTabletLocator();
    Instance instance = getInstance(job);
    String username = getUsername(job);
    byte[] password = getPassword(job);
    String tableName = getTablename(job);
    return TabletLocator.getInstance(instance, new AuthInfo(username, ByteBuffer.wrap(password), instance.getInstanceID()),
        new Text(Tables.getTableId(instance, tableName)));
  }
  
  protected static List<Range> getRanges(JobContext job) throws IOException {
    ArrayList<Range> ranges = new ArrayList<Range>();
    for (String rangeString : job.getConfiguration().getStringCollection(RANGES)) {
      ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(rangeString.getBytes()));
      Range range = new Range();
      range.readFields(new DataInputStream(bais));
      ranges.add(range);
    }
    return ranges;
  }
  
  protected static Set<Pair<Text,Text>> getFetchedColumns(JobContext job) {
    Set<Pair<Text,Text>> columns = new HashSet<Pair<Text,Text>>();
    for (String col : job.getConfiguration().getStringCollection(COLUMNS)) {
      int idx = col.indexOf(":");
      Text cf = new Text(idx < 0 ? Base64.decodeBase64(col.getBytes()) : Base64.decodeBase64(col.substring(0, idx).getBytes()));
      Text cq = idx < 0 ? null : new Text(Base64.decodeBase64(col.substring(idx + 1).getBytes()));
      columns.add(new Pair<Text,Text>(cf, cq));
    }
    return columns;
  }
  
  protected static boolean getAutoAdjustRanges(JobContext job) {
    return job.getConfiguration().getBoolean(AUTO_ADJUST_RANGES, true);
  }
  
  protected static Level getLogLevel(JobContext job) {
    return Level.toLevel(job.getConfiguration().getInt(LOGLEVEL, Level.INFO.toInt()));
  }
  
  // InputFormat doesn't have the equivalent of OutputFormat's
  // checkOutputSpecs(JobContext job)
  protected static void validateOptions(JobContext job) throws IOException {
    Configuration conf = job.getConfiguration();
    if (!conf.getBoolean(INPUT_INFO_HAS_BEEN_SET, false))
      throw new IOException("Input info has not been set.");
    if (!conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IOException("Instance info has not been set.");
    // validate that we can connect as configured
    try {
      Connector c = getInstance(job).getConnector(getUsername(job), getPassword(job));
      if (!c.securityOperations().authenticateUser(getUsername(job), getPassword(job)))
        throw new IOException("Unable to authenticate user");
      if (!c.securityOperations().hasTablePermission(getUsername(job), getTablename(job), TablePermission.READ))
        throw new IOException("Unable to access table");
      
      if (!usesLocalIterators(job)) {
        // validate that any scan-time iterators can be loaded by the the tablet servers
        for (AccumuloIterator iter : getIterators(job)) {
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
  protected static int getMaxVersions(JobContext job) {
    return job.getConfiguration().getInt(MAX_VERSIONS, -1);
  }
  
  // Return a list of the iterator settings (for iterators to apply to a scanner)
  protected static List<AccumuloIterator> getIterators(JobContext job) {
    
    String iterators = job.getConfiguration().get(ITERATORS);
    
    // If no iterators are present, return an empty list
    if (iterators == null || iterators.isEmpty())
      return new ArrayList<AccumuloIterator>();
    
    // Compose the set of iterators encoded in the job configuration
    StringTokenizer tokens = new StringTokenizer(job.getConfiguration().get(ITERATORS), ITERATORS_DELIM);
    List<AccumuloIterator> list = new ArrayList<AccumuloIterator>();
    while (tokens.hasMoreTokens()) {
      String itstring = tokens.nextToken();
      list.add(new AccumuloIterator(itstring));
    }
    return list;
  }
  
  // Return a list of the iterator options specified
  protected static List<AccumuloIteratorOption> getIteratorOptions(JobContext job) {
    String iteratorOptions = job.getConfiguration().get(ITERATORS_OPTIONS);
    
    // If no options are present, return an empty list
    if (iteratorOptions == null || iteratorOptions.isEmpty())
      return new ArrayList<AccumuloIteratorOption>();
    
    // Compose the set of options encoded in the job configuration
    StringTokenizer tokens = new StringTokenizer(job.getConfiguration().get(ITERATORS_OPTIONS), ITERATORS_DELIM);
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
    protected RangeInputSplit split;
       
    // Apply the configured iterators from the job to the scanner
    protected void setupIterators(TaskAttemptContext attempt, Scanner scanner) throws AccumuloException {
      List<AccumuloIterator> iterators = getIterators(attempt);
      List<AccumuloIteratorOption> options = getIteratorOptions(attempt);
      
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
    protected void setupMaxVersions(TaskAttemptContext attempt, Scanner scanner) {
      int maxVersions = getMaxVersions(attempt);
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
      Instance instance = getInstance(attempt);
      String user = getUsername(attempt);
      byte[] password = getPassword(attempt);
      Authorizations authorizations = getAuthorizations(attempt);
      
      try {
        log.debug("Creating connector with user: " + user);
        Connector conn = instance.getConnector(user, password);
        log.debug("Creating scanner for table: " + getTablename(attempt));
        log.debug("Authorizations are: " + authorizations);
        scanner = conn.createScanner(getTablename(attempt), authorizations);
        if (isIsolated(attempt)) {
          log.info("Creating isolated scanner");
          scanner = new IsolatedScanner(scanner);
        }
        if (usesLocalIterators(attempt)) {
          log.info("Using local iterators");
          scanner = new ClientSideIteratorScanner(scanner);
        }
        setupMaxVersions(attempt, scanner);
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
    log.setLevel(getLogLevel(job));
    validateOptions(job);
    
    String tableName = getTablename(job);
    boolean autoAdjust = getAutoAdjustRanges(job);
    List<Range> ranges = autoAdjust ? Range.mergeOverlapping(getRanges(job)) : getRanges(job);
    
    if (ranges.isEmpty()) {
      ranges = new ArrayList<Range>(1);
      ranges.add(new Range());
    }
    
    // get the metadata information for these ranges
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    TabletLocator tl;
    try {
      tl = getTabletLocator(job);
      while (!tl.binRanges(ranges, binnedRanges).isEmpty()) {
        log.warn("Unable to locate bins for specified ranges. Retrying.");
        UtilWaitThread.sleep(100 + (int) (Math.random() * 100)); // sleep
        // randomly between 100 and 200 ms
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    ArrayList<InputSplit> splits = new ArrayList<InputSplit>(ranges.size());
    HashMap<Range,ArrayList<String>> splitsToAdd = null;
    
    if (!autoAdjust)
      splitsToAdd = new HashMap<Range,ArrayList<String>>();
    
    for (Entry<String,Map<KeyExtent,List<Range>>> tserverBin : binnedRanges.entrySet()) {
      String location = tserverBin.getKey().split(":", 2)[0];
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
   * The Class RangeInputSplit. Encapsulates a Accumulo range for use in Map Reduce jobs.
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
   * The Class AccumuloIteratorOption. Encapsulates specifics for a Accumulo iterator's optional configuration details - associated via the iteratorName.
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
      this.key = tokenizer.nextToken();
      try {
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
        return new String(iteratorName + FIELD_SEP + key + FIELD_SEP + URLEncoder.encode(value, "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
    
  }
  
}
