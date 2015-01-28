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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.impl.OfflineScanner;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.HadoopCompatUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
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
public abstract class AbstractInputFormat<K, V> extends InputFormat<K,V> {

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
   * @deprecated since 1.6.0; Use {@link #setZooKeeperInstance(Job, ClientConfiguration)} instead.
   */
  @Deprecated
  public static void setZooKeeperInstance(Job job, String instanceName, String zooKeepers) {
    setZooKeeperInstance(job, new ClientConfiguration().withInstance(instanceName).withZkHosts(zooKeepers));
  }

  /**
   * Configures a {@link org.apache.accumulo.core.client.ZooKeeperInstance} for this job.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param clientConfig
   *          client configuration containing connection options
   * @since 1.6.0
   */
  public static void setZooKeeperInstance(Job job, ClientConfiguration clientConfig) {
    InputConfigurator.setZooKeeperInstance(CLASS, job.getConfiguration(), clientConfig);
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
   * Fetches all {@link InputTableConfig}s that have been set on the given job.
   *
   * @param context
   *          the Hadoop job instance to be configured
   * @return the {@link InputTableConfig} objects for the job
   * @since 1.6.0
   */
  protected static Map<String,InputTableConfig> getInputTableConfigs(JobContext context) {
    return InputConfigurator.getInputTableConfigs(CLASS, getConfiguration(context));
  }

  /**
   * Fetches a {@link InputTableConfig} that has been set on the configuration for a specific table.
   *
   * <p>
   * null is returned in the event that the table doesn't exist.
   *
   * @param context
   *          the Hadoop job instance to be configured
   * @param tableName
   *          the table name for which to grab the config object
   * @return the {@link InputTableConfig} for the given table
   * @since 1.6.0
   */
  protected static InputTableConfig getInputTableConfig(JobContext context, String tableName) {
    return InputConfigurator.getInputTableConfig(CLASS, getConfiguration(context), tableName);
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
  protected abstract static class AbstractRecordReader<K, V> extends RecordReader<K,V> {
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
    protected abstract void setupIterators(TaskAttemptContext context, Scanner scanner, String tableName, RangeInputSplit split);

    /**
     * Initialize a scanner over the given input split using this task attempt configuration.
     */
    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {

      Scanner scanner;
      split = (RangeInputSplit) inSplit;
      log.debug("Initializing input split: " + split.getRange());

      Instance instance = split.getInstance();
      if (null == instance) {
        instance = getInstance(attempt);
      }

      String principal = split.getPrincipal();
      if (null == principal) {
        principal = getPrincipal(attempt);
      }

      AuthenticationToken token = split.getToken();
      if (null == token) {
        token = getAuthenticationToken(attempt);
      }

      Authorizations authorizations = split.getAuths();
      if (null == authorizations) {
        authorizations = getScanAuthorizations(attempt);
      }

      String table = split.getTableName();

      // in case the table name changed, we can still use the previous name for terms of configuration,
      // but the scanner will use the table id resolved at job setup time
      InputTableConfig tableConfig = getInputTableConfig(attempt, split.getTableName());

      Boolean isOffline = split.isOffline();
      if (null == isOffline) {
        isOffline = tableConfig.isOfflineScan();
      }

      Boolean isIsolated = split.isIsolatedScan();
      if (null == isIsolated) {
        isIsolated = tableConfig.shouldUseIsolatedScanners();
      }

      Boolean usesLocalIterators = split.usesLocalIterators();
      if (null == usesLocalIterators) {
        usesLocalIterators = tableConfig.shouldUseLocalIterators();
      }

      List<IteratorSetting> iterators = split.getIterators();
      if (null == iterators) {
        iterators = tableConfig.getIterators();
      }

      Collection<Pair<Text,Text>> columns = split.getFetchedColumns();
      if (null == columns) {
        columns = tableConfig.getFetchedColumns();
      }

      try {
        log.debug("Creating connector with user: " + principal);
        log.debug("Creating scanner for table: " + table);
        log.debug("Authorizations are: " + authorizations);
        if (isOffline) {
          scanner = new OfflineScanner(instance, new Credentials(principal, token), split.getTableId(), authorizations);
        } else if (instance instanceof MockInstance) {
          scanner = instance.getConnector(principal, token).createScanner(split.getTableName(), authorizations);
        } else {
          scanner = new ScannerImpl(instance, new Credentials(principal, token), split.getTableId(), authorizations);
        }
        if (isIsolated) {
          log.info("Creating isolated scanner");
          scanner = new IsolatedScanner(scanner);
        }
        if (usesLocalIterators) {
          log.info("Using local iterators");
          scanner = new ClientSideIteratorScanner(scanner);
        }

        setupIterators(attempt, scanner, split.getTableName(), split);
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

    /**
     * The Key that should be returned to the client
     */
    protected K currentK = null;

    /**
     * The Value that should be return to the client
     */
    protected V currentV = null;

    /**
     * The Key that is used to determine progress in the current InputSplit. It is not returned to the client and is only used internally
     */
    protected Key currentKey = null;

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
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    Level logLevel = getLogLevel(context);
    log.setLevel(logLevel);
    validateOptions(context);
    Random random = new Random();
    LinkedList<InputSplit> splits = new LinkedList<InputSplit>();
    Map<String,InputTableConfig> tableConfigs = getInputTableConfigs(context);
    for (Map.Entry<String,InputTableConfig> tableConfigEntry : tableConfigs.entrySet()) {

      String tableName = tableConfigEntry.getKey();
      InputTableConfig tableConfig = tableConfigEntry.getValue();

      Instance instance = getInstance(context);
      boolean mockInstance;
      String tableId;
      // resolve table name to id once, and use id from this point forward
      if (instance instanceof MockInstance) {
        tableId = "";
        mockInstance = true;
      } else {
        try {
          tableId = Tables.getTableId(instance, tableName);
        } catch (TableNotFoundException e) {
          throw new IOException(e);
        }
        mockInstance = false;
      }

      Authorizations auths = getScanAuthorizations(context);
      String principal = getPrincipal(context);
      AuthenticationToken token = getAuthenticationToken(context);

      boolean autoAdjust = tableConfig.shouldAutoAdjustRanges();
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
          binnedRanges = binOfflineTable(context, tableId, ranges);
          while (binnedRanges == null) {
            // Some tablets were still online, try again
            UtilWaitThread.sleep(100 + random.nextInt(100)); // sleep randomly between 100 and 200 ms
            binnedRanges = binOfflineTable(context, tableId, ranges);

          }
        } else {
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
            UtilWaitThread.sleep(100 + random.nextInt(100)); // sleep randomly between 100 and 200 ms
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
          location = inetAddress.getCanonicalHostName();
          hostNameCache.put(ip, location);
        }
        for (Map.Entry<KeyExtent,List<Range>> extentRanges : tserverBin.getValue().entrySet()) {
          Range ke = extentRanges.getKey().toDataRange();
          for (Range r : extentRanges.getValue()) {
            if (autoAdjust) {
              // divide ranges into smaller ranges, based on the tablets
              RangeInputSplit split = new RangeInputSplit(tableName, tableId, ke.clip(r), new String[] {location});

              split.setOffline(tableConfig.isOfflineScan());
              split.setIsolatedScan(tableConfig.shouldUseIsolatedScanners());
              split.setUsesLocalIterators(tableConfig.shouldUseLocalIterators());
              split.setMockInstance(mockInstance);
              split.setFetchedColumns(tableConfig.getFetchedColumns());
              split.setPrincipal(principal);
              split.setToken(token);
              split.setInstanceName(instance.getInstanceName());
              split.setZooKeepers(instance.getZooKeepers());
              split.setAuths(auths);
              split.setIterators(tableConfig.getIterators());
              split.setLogLevel(logLevel);

              splits.add(split);
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
        for (Map.Entry<Range,ArrayList<String>> entry : splitsToAdd.entrySet()) {
          RangeInputSplit split = new RangeInputSplit(tableName, tableId, entry.getKey(), entry.getValue().toArray(new String[0]));

          split.setOffline(tableConfig.isOfflineScan());
          split.setIsolatedScan(tableConfig.shouldUseIsolatedScanners());
          split.setUsesLocalIterators(tableConfig.shouldUseLocalIterators());
          split.setMockInstance(mockInstance);
          split.setFetchedColumns(tableConfig.getFetchedColumns());
          split.setPrincipal(principal);
          split.setToken(token);
          split.setInstanceName(instance.getInstanceName());
          split.setZooKeepers(instance.getZooKeepers());
          split.setAuths(auths);
          split.setIterators(tableConfig.getIterators());
          split.setLogLevel(logLevel);

          splits.add(split);
        }
    }
    return splits;
  }

  static Configuration getConfiguration(JobContext context) {
    return HadoopCompatUtil.getConfiguration(context);
  }
}
