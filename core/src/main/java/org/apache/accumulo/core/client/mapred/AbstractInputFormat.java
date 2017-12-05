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

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

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
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.impl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.DelegationTokenImpl;
import org.apache.accumulo.core.client.impl.OfflineScanner;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mapred.impl.BatchInputSplit;
import org.apache.accumulo.core.client.mapreduce.InputTableConfig;
import org.apache.accumulo.core.client.mapreduce.impl.SplitUtils;
import org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.DeprecationUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * An abstract input format to provide shared methods common to all other input format classes. At the very least, any classes inheriting from this class will
 * need to define their own {@link RecordReader}.
 */
public abstract class AbstractInputFormat<K,V> implements InputFormat<K,V> {
  protected static final Class<?> CLASS = AccumuloInputFormat.class;
  protected static final Logger log = Logger.getLogger(CLASS);

  /**
   * Sets the name of the classloader context on this scanner
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param context
   *          name of the classloader context
   * @since 1.8.0
   */
  public static void setClassLoaderContext(JobConf job, String context) {
    InputConfigurator.setClassLoaderContext(CLASS, job, context);
  }

  /**
   * Returns the name of the current classloader context set on this scanner
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @return name of the current context
   * @since 1.8.0
   */
  public static String getClassLoaderContext(JobConf job) {
    return InputConfigurator.getClassLoaderContext(CLASS, job);
  }

  /**
   * Sets the connector information needed to communicate with Accumulo in this job.
   *
   * <p>
   * <b>WARNING:</b> Some tokens, when serialized, divulge sensitive information in the configuration as a means to pass the token to MapReduce tasks. This
   * information is BASE64 encoded to provide a charset safe conversion to a string, but this conversion is not intended to be secure. {@link PasswordToken} is
   * one example that is insecure in this way; however {@link DelegationToken}s, acquired using
   * {@link SecurityOperations#getDelegationToken(DelegationTokenConfig)}, is not subject to this concern.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param principal
   *          a valid Accumulo user name (user must have Table.CREATE permission)
   * @param token
   *          the user's password
   * @since 1.5.0
   */
  public static void setConnectorInfo(JobConf job, String principal, AuthenticationToken token) throws AccumuloSecurityException {
    if (token instanceof KerberosToken) {
      log.info("Received KerberosToken, attempting to fetch DelegationToken");
      try {
        Instance instance = getInstance(job);
        Connector conn = instance.getConnector(principal, token);
        token = conn.securityOperations().getDelegationToken(new DelegationTokenConfig());
      } catch (Exception e) {
        log.warn("Failed to automatically obtain DelegationToken, Mappers/Reducers will likely fail to communicate with Accumulo", e);
      }
    }
    // DelegationTokens can be passed securely from user to task without serializing insecurely in the configuration
    if (token instanceof DelegationTokenImpl) {
      DelegationTokenImpl delegationToken = (DelegationTokenImpl) token;

      // Convert it into a Hadoop Token
      AuthenticationTokenIdentifier identifier = delegationToken.getIdentifier();
      Token<AuthenticationTokenIdentifier> hadoopToken = new Token<>(identifier.getBytes(), delegationToken.getPassword(), identifier.getKind(),
          delegationToken.getServiceName());

      // Add the Hadoop Token to the Job so it gets serialized and passed along.
      job.getCredentials().addToken(hadoopToken.getService(), hadoopToken);
    }

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
    AuthenticationToken token = InputConfigurator.getAuthenticationToken(CLASS, job);
    return ConfiguratorBase.unwrapAuthenticationToken(job, token);
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
   * @deprecated since 1.6.0; Use {@link #setZooKeeperInstance(JobConf, ClientConfiguration)} instead.
   */
  @Deprecated
  public static void setZooKeeperInstance(JobConf job, String instanceName, String zooKeepers) {
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
  public static void setZooKeeperInstance(JobConf job, ClientConfiguration clientConfig) {
    InputConfigurator.setZooKeeperInstance(CLASS, job, clientConfig);
  }

  /**
   * Configures a {@link org.apache.accumulo.core.client.mock.MockInstance} for this job.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param instanceName
   *          the Accumulo instance name
   * @since 1.5.0
   * @deprecated since 1.8.0; use MiniAccumuloCluster or a standard mock framework
   */
  @Deprecated
  public static void setMockInstance(JobConf job, String instanceName) {
    InputConfigurator.setMockInstance(CLASS, job, instanceName);
  }

  /**
   * Initializes an Accumulo {@link org.apache.accumulo.core.client.Instance} based on the configuration.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @return an Accumulo instance
   * @since 1.5.0
   * @see #setZooKeeperInstance(JobConf, ClientConfiguration)
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
   * Sets the {@link org.apache.accumulo.core.security.Authorizations} used to scan. Must be a subset of the user's authorization. Defaults to the empty set.
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
   * Fetch the client configuration from the job.
   *
   * @param job
   *          The job
   * @return The client configuration for the job
   * @since 1.7.0
   */
  protected static ClientConfiguration getClientConfiguration(JobConf job) {
    return InputConfigurator.getClientConfiguration(CLASS, job);
  }

  // InputFormat doesn't have the equivalent of OutputFormat's checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo {@link InputFormat}.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @throws java.io.IOException
   *           if the context is improperly configured
   * @since 1.5.0
   */
  protected static void validateOptions(JobConf job) throws IOException {
    final Instance inst = InputConfigurator.validateInstance(CLASS, job);
    String principal = InputConfigurator.getPrincipal(CLASS, job);
    AuthenticationToken token = InputConfigurator.getAuthenticationToken(CLASS, job);
    // In secure mode, we need to convert the DelegationTokenStub into a real DelegationToken
    token = ConfiguratorBase.unwrapAuthenticationToken(job, token);
    Connector conn;
    try {
      conn = inst.getConnector(principal, token);
    } catch (Exception e) {
      throw new IOException(e);
    }
    InputConfigurator.validatePermissions(CLASS, job, conn);
  }

  /**
   * Fetches all {@link InputTableConfig}s that have been set on the given Hadoop job.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @return the {@link InputTableConfig} objects set on the job
   * @since 1.6.0
   */
  public static Map<String,InputTableConfig> getInputTableConfigs(JobConf job) {
    return InputConfigurator.getInputTableConfigs(CLASS, job);
  }

  /**
   * Fetches a {@link InputTableConfig} that has been set on the configuration for a specific table.
   *
   * <p>
   * null is returned in the event that the table doesn't exist.
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param tableName
   *          the table name for which to grab the config object
   * @return the {@link InputTableConfig} for the given table
   * @since 1.6.0
   */
  public static InputTableConfig getInputTableConfig(JobConf job, String tableName) {
    return InputConfigurator.getInputTableConfig(CLASS, job, tableName);
  }

  /**
   * An abstract base class to be used to create {@link org.apache.hadoop.mapred.RecordReader} instances that convert from Accumulo
   * {@link org.apache.accumulo.core.data.Key}/{@link org.apache.accumulo.core.data.Value} pairs to the user's K/V types.
   *
   * Subclasses must implement {@link #next(Object, Object)} to update key and value, and also to update the following variables:
   * <ul>
   * <li>Key {@link #currentKey} (used for progress reporting)</li>
   * <li>int {@link #numKeysRead} (used for progress reporting)</li>
   * </ul>
   */
  protected abstract static class AbstractRecordReader<K,V> implements RecordReader<K,V> {
    protected long numKeysRead;
    protected Iterator<Map.Entry<Key,Value>> scannerIterator;
    protected RangeInputSplit split;
    private org.apache.accumulo.core.client.mapreduce.RangeInputSplit baseSplit;
    protected ScannerBase scannerBase;

    /**
     * Extracts Iterators settings from the context to be used by RecordReader.
     *
     * @param job
     *          the Hadoop job configuration
     * @param tableName
     *          the table name for which the scanner is configured
     * @return List of iterator settings for given table
     * @since 1.7.0
     */
    protected abstract List<IteratorSetting> jobIterators(JobConf job, String tableName);

    /**
     * Configures the iterators on a scanner for the given table name.
     *
     * @param job
     *          the Hadoop job configuration
     * @param scanner
     *          the scanner for which to configure the iterators
     * @param tableName
     *          the table name for which the scanner is configured
     * @since 1.7.0
     */
    private void setupIterators(JobConf job, ScannerBase scanner, String tableName, org.apache.accumulo.core.client.mapreduce.RangeInputSplit split) {
      List<IteratorSetting> iterators = null;

      if (null == split) {
        iterators = jobIterators(job, tableName);
      } else {
        iterators = split.getIterators();
        if (null == iterators) {
          iterators = jobIterators(job, tableName);
        }
      }

      for (IteratorSetting iterator : iterators)
        scanner.addScanIterator(iterator);
    }

    /**
     * Configures the iterators on a scanner for the given table name.
     *
     * @param job
     *          the Hadoop job configuration
     * @param scanner
     *          the scanner for which to configure the iterators
     * @param tableName
     *          the table name for which the scanner is configured
     * @since 1.6.0
     * @deprecated since 1.7.0; Use {@link #jobIterators} instead.
     */
    @Deprecated
    protected void setupIterators(JobConf job, Scanner scanner, String tableName, RangeInputSplit split) {
      setupIterators(job, (ScannerBase) scanner, tableName, split);
    }

    /**
     * Initialize a scanner over the given input split using this task attempt configuration.
     */
    public void initialize(InputSplit inSplit, JobConf job) throws IOException {
      baseSplit = (org.apache.accumulo.core.client.mapreduce.RangeInputSplit) inSplit;
      log.debug("Initializing input split: " + baseSplit.toString());

      Instance instance = baseSplit.getInstance(getClientConfiguration(job));
      if (null == instance) {
        instance = getInstance(job);
      }

      String principal = baseSplit.getPrincipal();
      if (null == principal) {
        principal = getPrincipal(job);
      }

      AuthenticationToken token = baseSplit.getToken();
      if (null == token) {
        token = getAuthenticationToken(job);
      }

      Authorizations authorizations = baseSplit.getAuths();
      if (null == authorizations) {
        authorizations = getScanAuthorizations(job);
      }
      String classLoaderContext = getClassLoaderContext(job);
      String table = baseSplit.getTableName();

      // in case the table name changed, we can still use the previous name for terms of configuration,
      // but the scanner will use the table id resolved at job setup time
      InputTableConfig tableConfig = getInputTableConfig(job, baseSplit.getTableName());

      log.debug("Creating connector with user: " + principal);
      log.debug("Creating scanner for table: " + table);
      log.debug("Authorizations are: " + authorizations);

      if (baseSplit instanceof BatchInputSplit) {
        BatchScanner scanner;
        BatchInputSplit multiRangeSplit = (BatchInputSplit) baseSplit;

        try {
          // Note: BatchScanner will use at most one thread per tablet, currently BatchInputSplit will not span tablets
          int scanThreads = 1;
          scanner = instance.getConnector(principal, token).createBatchScanner(baseSplit.getTableName(), authorizations, scanThreads);
          setupIterators(job, scanner, baseSplit.getTableName(), baseSplit);
          if (null != classLoaderContext) {
            scanner.setClassLoaderContext(classLoaderContext);
          }
        } catch (Exception e) {
          throw new IOException(e);
        }

        scanner.setRanges(multiRangeSplit.getRanges());
        scannerBase = scanner;

      } else if (baseSplit instanceof RangeInputSplit) {
        split = (RangeInputSplit) baseSplit;
        Boolean isOffline = baseSplit.isOffline();
        if (null == isOffline) {
          isOffline = tableConfig.isOfflineScan();
        }

        Boolean isIsolated = baseSplit.isIsolatedScan();
        if (null == isIsolated) {
          isIsolated = tableConfig.shouldUseIsolatedScanners();
        }

        Boolean usesLocalIterators = baseSplit.usesLocalIterators();
        if (null == usesLocalIterators) {
          usesLocalIterators = tableConfig.shouldUseLocalIterators();
        }

        Scanner scanner;

        try {
          if (isOffline) {
            scanner = new OfflineScanner(instance, new Credentials(principal, token), Table.ID.of(baseSplit.getTableId()), authorizations);
          } else if (DeprecationUtil.isMockInstance(instance)) {
            scanner = instance.getConnector(principal, token).createScanner(baseSplit.getTableName(), authorizations);
          } else {
            ClientConfiguration clientConf = getClientConfiguration(job);
            ClientContext context = new ClientContext(instance, new Credentials(principal, token), clientConf);
            scanner = new ScannerImpl(context, Table.ID.of(baseSplit.getTableId()), authorizations);
          }
          if (isIsolated) {
            log.info("Creating isolated scanner");
            scanner = new IsolatedScanner(scanner);
          }
          if (usesLocalIterators) {
            log.info("Using local iterators");
            scanner = new ClientSideIteratorScanner(scanner);
          }
          setupIterators(job, scanner, baseSplit.getTableName(), baseSplit);
        } catch (Exception e) {
          throw new IOException(e);
        }

        scanner.setRange(baseSplit.getRange());
        scannerBase = scanner;
      } else {
        throw new IllegalArgumentException("Can not initialize from " + baseSplit.getClass().toString());
      }

      Collection<Pair<Text,Text>> columns = baseSplit.getFetchedColumns();
      if (null == columns) {
        columns = tableConfig.getFetchedColumns();
      }

      // setup a scanner within the bounds of this split
      for (Pair<Text,Text> c : columns) {
        if (c.getSecond() != null) {
          log.debug("Fetching column " + c.getFirst() + ":" + c.getSecond());
          scannerBase.fetchColumn(c.getFirst(), c.getSecond());
        } else {
          log.debug("Fetching column family " + c.getFirst());
          scannerBase.fetchColumnFamily(c.getFirst());
        }
      }

      SamplerConfiguration samplerConfig = baseSplit.getSamplerConfiguration();
      if (null == samplerConfig) {
        samplerConfig = tableConfig.getSamplerConfiguration();
      }

      if (samplerConfig != null) {
        scannerBase.setSamplerConfiguration(samplerConfig);
      }

      scannerIterator = scannerBase.iterator();
      numKeysRead = 0;
    }

    @Override
    public void close() {
      if (null != scannerBase) {
        scannerBase.close();
      }
    }

    @Override
    public long getPos() throws IOException {
      return numKeysRead;
    }

    @Override
    public float getProgress() throws IOException {
      if (numKeysRead > 0 && currentKey == null)
        return 1.0f;
      return baseSplit.getProgress(currentKey);
    }

    protected Key currentKey = null;

  }

  Map<String,Map<KeyExtent,List<Range>>> binOfflineTable(JobConf job, Table.ID tableId, List<Range> ranges) throws TableNotFoundException, AccumuloException,
      AccumuloSecurityException {

    Instance instance = getInstance(job);
    Connector conn = instance.getConnector(getPrincipal(job), getAuthenticationToken(job));

    return InputConfigurator.binOffline(tableId, ranges, instance, conn);
  }

  /**
   * Gets the splits of the tables that have been set on the job by reading the metadata table for the specified ranges.
   *
   * @return the splits from the tables based on the ranges.
   * @throws java.io.IOException
   *           if a table set on the job doesn't exist or an error occurs initializing the tablet locator
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    Level logLevel = getLogLevel(job);
    log.setLevel(logLevel);
    validateOptions(job);

    Random random = new Random();
    LinkedList<InputSplit> splits = new LinkedList<>();
    Map<String,InputTableConfig> tableConfigs = getInputTableConfigs(job);
    for (Map.Entry<String,InputTableConfig> tableConfigEntry : tableConfigs.entrySet()) {
      String tableName = tableConfigEntry.getKey();
      InputTableConfig tableConfig = tableConfigEntry.getValue();

      Instance instance = getInstance(job);
      Table.ID tableId;
      // resolve table name to id once, and use id from this point forward
      if (DeprecationUtil.isMockInstance(instance)) {
        tableId = null;
      } else {
        try {
          tableId = Tables.getTableId(instance, tableName);
        } catch (TableNotFoundException e) {
          throw new IOException(e);
        }
      }

      Authorizations auths = getScanAuthorizations(job);
      String principal = getPrincipal(job);
      AuthenticationToken token = getAuthenticationToken(job);

      boolean batchScan = InputConfigurator.isBatchScan(CLASS, job);
      boolean supportBatchScan = !(tableConfig.isOfflineScan() || tableConfig.shouldUseIsolatedScanners() || tableConfig.shouldUseLocalIterators());
      if (batchScan && !supportBatchScan)
        throw new IllegalArgumentException("BatchScanner optimization not available for offline scan, isolated, or local iterators");

      boolean autoAdjust = tableConfig.shouldAutoAdjustRanges();
      if (batchScan && !autoAdjust)
        throw new IllegalArgumentException("AutoAdjustRanges must be enabled when using BatchScanner optimization");

      List<Range> ranges = autoAdjust ? Range.mergeOverlapping(tableConfig.getRanges()) : tableConfig.getRanges();
      if (ranges.isEmpty()) {
        ranges = new ArrayList<>(1);
        ranges.add(new Range());
      }

      // get the metadata information for these ranges
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();
      TabletLocator tl;
      try {
        if (tableConfig.isOfflineScan()) {
          binnedRanges = binOfflineTable(job, tableId, ranges);
          while (binnedRanges == null) {
            // Some tablets were still online, try again
            // sleep randomly between 100 and 200 ms
            sleepUninterruptibly(100 + random.nextInt(100), TimeUnit.MILLISECONDS);
            binnedRanges = binOfflineTable(job, tableId, ranges);
          }
        } else {
          tl = InputConfigurator.getTabletLocator(CLASS, job, tableId);
          // its possible that the cache could contain complete, but old information about a tables tablets... so clear it
          tl.invalidateCache();

          ClientContext context = new ClientContext(getInstance(job), new Credentials(getPrincipal(job), getAuthenticationToken(job)),
              getClientConfiguration(job));
          while (!tl.binRanges(context, ranges, binnedRanges).isEmpty()) {
            if (!DeprecationUtil.isMockInstance(instance)) {
              String tableIdStr = tableId != null ? tableId.canonicalID() : null;
              if (!Tables.exists(instance, tableId))
                throw new TableDeletedException(tableIdStr);
              if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
                throw new TableOfflineException(instance, tableIdStr);
            }
            binnedRanges.clear();
            log.warn("Unable to locate bins for specified ranges. Retrying.");
            // sleep randomly between 100 and 200 ms
            sleepUninterruptibly(100 + random.nextInt(100), TimeUnit.MILLISECONDS);
            tl.invalidateCache();
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }

      HashMap<Range,ArrayList<String>> splitsToAdd = null;

      if (!autoAdjust)
        splitsToAdd = new HashMap<>();

      HashMap<String,String> hostNameCache = new HashMap<>();
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
          if (batchScan) {
            // group ranges by tablet to be read by a BatchScanner
            ArrayList<Range> clippedRanges = new ArrayList<>();
            for (Range r : extentRanges.getValue())
              clippedRanges.add(ke.clip(r));

            BatchInputSplit split = new BatchInputSplit(tableName, tableId, clippedRanges, new String[] {location});
            SplitUtils.updateSplit(split, instance, tableConfig, principal, token, auths, logLevel);

            splits.add(split);
          } else {
            // not grouping by tablet
            for (Range r : extentRanges.getValue()) {
              if (autoAdjust) {
                // divide ranges into smaller ranges, based on the tablets
                RangeInputSplit split = new RangeInputSplit(tableName, tableId.canonicalID(), ke.clip(r), new String[] {location});
                SplitUtils.updateSplit(split, instance, tableConfig, principal, token, auths, logLevel);
                split.setOffline(tableConfig.isOfflineScan());
                split.setIsolatedScan(tableConfig.shouldUseIsolatedScanners());
                split.setUsesLocalIterators(tableConfig.shouldUseLocalIterators());

                splits.add(split);
              } else {
                // don't divide ranges
                ArrayList<String> locations = splitsToAdd.get(r);
                if (locations == null)
                  locations = new ArrayList<>(1);
                locations.add(location);
                splitsToAdd.put(r, locations);
              }
            }
          }
        }
      }

      if (!autoAdjust)
        for (Map.Entry<Range,ArrayList<String>> entry : splitsToAdd.entrySet()) {
          RangeInputSplit split = new RangeInputSplit(tableName, tableId.canonicalID(), entry.getKey(), entry.getValue().toArray(new String[0]));
          SplitUtils.updateSplit(split, instance, tableConfig, principal, token, auths, logLevel);
          split.setOffline(tableConfig.isOfflineScan());
          split.setIsolatedScan(tableConfig.shouldUseIsolatedScanners());
          split.setUsesLocalIterators(tableConfig.shouldUseLocalIterators());

          splits.add(split);
        }
    }

    return splits.toArray(new InputSplit[splits.size()]);
  }

}
