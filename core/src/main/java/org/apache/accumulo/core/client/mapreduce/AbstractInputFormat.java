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
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.impl.OfflineScanner;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mapreduce.lib.util.InputConfigurator;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
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
 * An abstract input format to provide shared methods common to all other input format classes. At the very least, any classes inheriting from this class will
 * need to define their own {@link RecordReader}.
 */
public abstract class AbstractInputFormat<K,V> extends InputFormat<K,V> {

  protected static final Class<?> CLASS = AccumuloInputFormat.class;
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
   * @throws org.apache.accumulo.core.client.AccumuloSecurityException
   * @since 1.5.0
   */
  public static void setConnectorInfo(Job job, String principal, AuthenticationToken token) throws AccumuloSecurityException {
    InputConfigurator.setConnectorInfo(CLASS, job.getConfiguration(), principal, token);
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
  public static void setConnectorInfo(Job job, String principal, String tokenFile) throws AccumuloSecurityException {
    InputConfigurator.setConnectorInfo(CLASS, job.getConfiguration(), principal, tokenFile);
  }

  /**
   * Determines if the connector has been configured.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return true if the connector has been configured, false otherwise
   * @since 1.5.0
   * @see #setConnectorInfo(Job, String, AuthenticationToken)
   */
  protected static Boolean isConnectorInfoSet(JobContext context) {
    return InputConfigurator.isConnectorInfoSet(CLASS, getConfiguration(context));
  }

  /**
   * Gets the user name from the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the user name
   * @since 1.5.0
   * @see #setConnectorInfo(Job, String, AuthenticationToken)
   */
  protected static String getPrincipal(JobContext context) {
    return InputConfigurator.getPrincipal(CLASS, getConfiguration(context));
  }

  /**
   * Gets the serialized token class from either the configuration or the token file.
   * 
   * @since 1.5.0
   * @deprecated since 1.6.0; Use {@link #getAuthenticationToken(JobContext)} instead.
   */
  @Deprecated
  protected static String getTokenClass(JobContext context) {
    return getAuthenticationToken(context).getClass().getName();
  }

  /**
   * Gets the serialized token from either the configuration or the token file.
   * 
   * @since 1.5.0
   * @deprecated since 1.6.0; Use {@link #getAuthenticationToken(JobContext)} instead.
   */
  @Deprecated
  protected static byte[] getToken(JobContext context) {
    return AuthenticationToken.AuthenticationTokenSerializer.serialize(getAuthenticationToken(context));
  }

  /**
   * Gets the authenticated token from either the specified token file or directly from the configuration, whichever was used when the job was configured.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return the principal's authentication token
   * @since 1.6.0
   * @see #setConnectorInfo(Job, String, AuthenticationToken)
   * @see #setConnectorInfo(Job, String, String)
   */
  protected static AuthenticationToken getAuthenticationToken(JobContext context) {
    return InputConfigurator.getAuthenticationToken(CLASS, getConfiguration(context));
  }

  /**
   * Configures a {@link org.apache.accumulo.core.client.ZooKeeperInstance} for this job.
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
   * Configures a {@link org.apache.accumulo.core.client.mock.MockInstance} for this job.
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
   * Initializes an Accumulo {@link org.apache.accumulo.core.client.Instance} based on the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @return an Accumulo instance
   * @since 1.5.0
   * @see #setZooKeeperInstance(Job, String, String)
   * @see #setMockInstance(Job, String)
   */
  protected static Instance getInstance(JobContext context) {
    return InputConfigurator.getInstance(CLASS, getConfiguration(context));
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
    return InputConfigurator.getLogLevel(CLASS, getConfiguration(context));
  }

  /**
   * Sets the {@link org.apache.accumulo.core.security.Authorizations} used to scan. Must be a subset of the user's authorization. Defaults to the empty set.
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param auths
   *          the user's authorizations
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
    return InputConfigurator.getScanAuthorizations(CLASS, getConfiguration(context));
  }

  /**
   * Fetches all {@link BatchScanConfig}s that have been set on the given job.
   * 
   * @param context
   *          the Hadoop job instance to be configured
   * @return the {@link BatchScanConfig} objects for the job
   * @since 1.6.0
   */
  protected static Map<String,BatchScanConfig> getBatchScanConfigs(JobContext context) {
    return InputConfigurator.getBatchScanConfigs(CLASS, getConfiguration(context));
  }

  /**
   * Fetches a {@link BatchScanConfig} that has been set on the configuration for a specific table.
   * 
   * <p>
   * null is returned in the event that the table doesn't exist.
   * 
   * @param context
   *          the Hadoop job instance to be configured
   * @param tableName
   *          the table name for which to grab the config object
   * @return the {@link BatchScanConfig} for the given table
   * @since 1.6.0
   */
  protected static BatchScanConfig getBatchScanConfig(JobContext context, String tableName) {
    return InputConfigurator.getBatchScanConfig(CLASS, getConfiguration(context), tableName);
  }

  /**
   * Initializes an Accumulo {@link org.apache.accumulo.core.client.impl.TabletLocator} based on the configuration.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @param table
   *          the table for which to initialize the locator
   * @return an Accumulo tablet locator
   * @throws org.apache.accumulo.core.client.TableNotFoundException
   *           if the table name set on the configuration doesn't exist
   * @since 1.6.0
   */
  protected static TabletLocator getTabletLocator(JobContext context, String table) throws TableNotFoundException {
    return InputConfigurator.getTabletLocator(CLASS, getConfiguration(context), table);
  }

  // InputFormat doesn't have the equivalent of OutputFormat's checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
   * 
   * @param context
   *          the Hadoop context for the configured job
   * @throws java.io.IOException
   *           if the context is improperly configured
   * @since 1.5.0
   */
  protected static void validateOptions(JobContext context) throws IOException {
    InputConfigurator.validateOptions(CLASS, getConfiguration(context));
  }

  /**
   * An abstract base class to be used to create {@link org.apache.hadoop.mapreduce.RecordReader} instances that convert from Accumulo
   * {@link org.apache.accumulo.core.data.Key}/{@link org.apache.accumulo.core.data.Value} pairs to the user's K/V types.
   * 
   * Subclasses must implement {@link #nextKeyValue()} and use it to update the following variables:
   * <ul>
   * <li>K {@link #currentK}</li>
   * <li>V {@link #currentV}</li>
   * <li>Key {@link #currentKey} (used for progress reporting)</li>
   * <li>int {@link #numKeysRead} (used for progress reporting)</li>
   * </ul>
   */
  protected abstract static class AbstractRecordReader<K,V> extends RecordReader<K,V> {
    protected long numKeysRead;
    protected Iterator<Map.Entry<Key,Value>> scannerIterator;
    protected RangeInputSplit split;

    /**
     * Configures the iterators on a scanner for the given table name.
     * 
     * @param context
     *          the Hadoop context for the configured job
     * @param scanner
     *          the scanner for which to configure the iterators
     * @param tableName
     *          the table name for which the scanner is configured
     * @since 1.6.0
     */
    protected abstract void setupIterators(TaskAttemptContext context, Scanner scanner, String tableName);

    /**
     * Initialize a scanner over the given input split using this task attempt configuration.
     */
    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {

      Scanner scanner;
      split = (RangeInputSplit) inSplit;
      log.debug("Initializing input split: " + split.getRange());
      Instance instance = getInstance(attempt);
      String principal = getPrincipal(attempt);
      AuthenticationToken token = getAuthenticationToken(attempt);
      Authorizations authorizations = getScanAuthorizations(attempt);

      // in case the table name changed, we can still use the previous name for terms of configuration,
      // but the scanner will use the table id resolved at job setup time
      BatchScanConfig tableConfig = getBatchScanConfig(attempt, split.getTableName());


      try {
        log.debug("Creating connector with user: " + principal);
        log.debug("Creating scanner for table: " + split.getTableName());
        log.debug("Authorizations are: " + authorizations);
        if (tableConfig.isOfflineScan()) {
          scanner = new OfflineScanner(instance, new Credentials(principal, token), split.getTableId(), authorizations);
        } else {
          scanner = new ScannerImpl(instance, new Credentials(principal, token), split.getTableId(), authorizations);
        }
        if (tableConfig.shouldUseIsolatedScanners()) {
          log.info("Creating isolated scanner");
          scanner = new IsolatedScanner(scanner);
        }
        if (tableConfig.shouldUseLocalIterators()) {
          log.info("Using local iterators");
          scanner = new ClientSideIteratorScanner(scanner);
        }
        setupIterators(attempt, scanner, split.getTableId());
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

  Map<String,Map<KeyExtent,List<Range>>> binOfflineTable(JobContext context, String tableId, List<Range> ranges) throws TableNotFoundException,
      AccumuloException, AccumuloSecurityException {

    Instance instance = getInstance(context);
    Connector conn = instance.getConnector(getPrincipal(context), getAuthenticationToken(context));

    return InputConfigurator.binOffline(tableId, ranges, instance, conn);
  }
  
  /**
   * Gets the splits of the tables that have been set on the job.
   * 
   * @param context
   *          the configuration of the job
   * @return the splits from the tables based on the ranges.
   * @throws java.io.IOException
   *           if a table set on the job doesn't exist or an error occurs initializing the tablet locator
   */
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    log.setLevel(getLogLevel(context));
    validateOptions(context);

    LinkedList<InputSplit> splits = new LinkedList<InputSplit>();
    Map<String,BatchScanConfig> tableConfigs = getBatchScanConfigs(context);
    for (Map.Entry<String,BatchScanConfig> tableConfigEntry : tableConfigs.entrySet()) {

      String tableName = tableConfigEntry.getKey();
      BatchScanConfig tableConfig = tableConfigEntry.getValue();

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
        // resolve table name to id once, and use id from this point forward
        tableId = Tables.getTableId(getInstance(context), tableName);
        if (tableConfig.isOfflineScan()) {
          binnedRanges = binOfflineTable(context, tableId, ranges);
          while (binnedRanges == null) {
            // Some tablets were still online, try again
            UtilWaitThread.sleep(100 + (int) (Math.random() * 100)); // sleep randomly between 100 and 200 ms
            binnedRanges = binOfflineTable(context, tableId, ranges);

          }
        } else {
          Instance instance = getInstance(context);
          tl = getTabletLocator(context, tableId);
          // its possible that the cache could contain complete, but old information about a tables tablets... so clear it
          tl.invalidateCache();
          Credentials creds = new Credentials(getPrincipal(context), getAuthenticationToken(context));

          while (!tl.binRanges(creds, ranges, binnedRanges).isEmpty()) {
            if (!(instance instanceof MockInstance)) {
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

      HashMap<Range,ArrayList<String>> splitsToAdd = null;

      if (!autoAdjust)
        splitsToAdd = new HashMap<Range,ArrayList<String>>();

      HashMap<String,String> hostNameCache = new HashMap<String,String>();
      for (Map.Entry<String,Map<KeyExtent,List<Range>>> tserverBin : binnedRanges.entrySet()) {
        String ip = tserverBin.getKey().split(":", 2)[0];
        String location = hostNameCache.get(ip);
        if (location == null) {
          InetAddress inetAddress = InetAddress.getByName(ip);
          location = inetAddress.getHostName();
          hostNameCache.put(ip, location);
        }
        for (Map.Entry<KeyExtent,List<Range>> extentRanges : tserverBin.getValue().entrySet()) {
          Range ke = extentRanges.getKey().toDataRange();
          for (Range r : extentRanges.getValue()) {
            if (autoAdjust) {
              // divide ranges into smaller ranges, based on the tablets
              splits.add(new RangeInputSplit(tableName, tableId, ke.clip(r), new String[] {location}));
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
        for (Map.Entry<Range,ArrayList<String>> entry : splitsToAdd.entrySet())
          splits.add(new RangeInputSplit(tableName, tableId, entry.getKey(), entry.getValue().toArray(new String[0])));
    }
    return splits;
  }

  /**
   * The Class RangeInputSplit. Encapsulates an Accumulo range for use in Map Reduce jobs.
   */
  public static class RangeInputSplit extends InputSplit implements Writable {
    private Range range;
    private String[] locations;
    private String tableId;
    private String tableName;

    public RangeInputSplit() {
      range = new Range();
      locations = new String[0];
      tableId = "";
      tableName = "";
    }

    public RangeInputSplit(RangeInputSplit split) throws IOException {
      this.setRange(split.getRange());
      this.setLocations(split.getLocations());
      this.setTableName(split.getTableName());
      this.setTableId(split.getTableId());
    }

    protected RangeInputSplit(String table, String tableId, Range range, String[] locations) {
      this.range = range;
      this.locations = locations;
      this.tableName = table;
      this.tableId = tableId;
    }

    public Range getRange() {
      return range;
    }

    public void setRange(Range range) {
      this.range = range;
    }

    public String getTableName() {
      return tableName;
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public void setTableId(String tableId) {
      this.tableId = tableId;
    }

    public String getTableId() {
      return tableId;
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
      tableName = in.readUTF();
      tableId = in.readUTF();
      int numLocs = in.readInt();
      locations = new String[numLocs];
      for (int i = 0; i < numLocs; ++i)
        locations[i] = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      range.write(out);
      out.writeUTF(tableName);
      out.writeUTF(tableId);
      out.writeInt(locations.length);
      for (int i = 0; i < locations.length; ++i)
        out.writeUTF(locations[i]);
    }
  }

  // use reflection to pull the Configuration out of the JobContext for Hadoop 1 and Hadoop 2 compatibility
  static Configuration getConfiguration(JobContext context) {
    try {
      Class<?> c = AbstractInputFormat.class.getClassLoader().loadClass("org.apache.hadoop.mapreduce.JobContext");
      Method m = c.getMethod("getConfiguration");
      Object o = m.invoke(context, new Object[0]);
      return (Configuration) o;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
