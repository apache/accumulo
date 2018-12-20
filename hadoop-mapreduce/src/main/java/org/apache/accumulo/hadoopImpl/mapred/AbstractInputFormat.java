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
package org.apache.accumulo.hadoopImpl.mapred;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.clientImpl.OfflineScanner;
import org.apache.accumulo.core.clientImpl.ScannerImpl;
import org.apache.accumulo.core.clientImpl.Table;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.hadoop.mapred.AccumuloInputFormat;
import org.apache.accumulo.hadoopImpl.mapreduce.InputTableConfig;
import org.apache.accumulo.hadoopImpl.mapreduce.SplitUtils;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.InputConfigurator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract input format to provide shared methods common to all other input format classes. At
 * the very least, any classes inheriting from this class will need to define their own
 * {@link RecordReader}.
 */
public abstract class AbstractInputFormat {
  protected static final Class<?> CLASS = AccumuloInputFormat.class;
  private static final Logger log = LoggerFactory.getLogger(CLASS);

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
  protected static String getClassLoaderContext(JobConf job) {
    return InputConfigurator.getClassLoaderContext(CLASS, job);
  }

  /**
   * Sets connection information needed to communicate with Accumulo for this job
   *
   * @param job
   *          Hadoop job instance to be configured
   * @param info
   *          Connection information for Accumulo
   * @since 2.0.0
   */
  public static void setClientInfo(JobConf job, ClientInfo info) {
    ClientInfo inputInfo = InputConfigurator.updateToken(job.getCredentials(), info);
    InputConfigurator.setClientInfo(CLASS, job, inputInfo);
  }

  /**
   * Set Accumulo client properties file used to connect to Accumulo
   *
   * @param job
   *          Hadoop job to be configured
   * @param clientPropsFile
   *          URL to Accumulo client properties file
   * @since 2.0.0
   */
  protected static void setClientPropertiesFile(JobConf job, String clientPropsFile) {
    InputConfigurator.setClientPropertiesFile(CLASS, job, clientPropsFile);
  }

  /**
   * Retrieves {@link ClientInfo} from the configuration
   *
   * @param job
   *          Hadoop job instance configuration
   * @return {@link ClientInfo} object
   * @since 2.0.0
   */
  protected static ClientInfo getClientInfo(JobConf job) {
    return InputConfigurator.getClientInfo(CLASS, job);
  }

  /**
   * Creates {@link AccumuloClient} from the configuration
   *
   * @param job
   *          Hadoop job instance configuration
   * @return {@link AccumuloClient} object
   * @since 2.0.0
   */
  protected static AccumuloClient createClient(JobConf job) {
    return Accumulo.newClient().from(getClientInfo(job).getProperties()).build();
  }

  /**
   * Sets the {@link org.apache.accumulo.core.security.Authorizations} used to scan. Must be a
   * subset of the user's authorization. Defaults to the empty set.
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

  // InputFormat doesn't have the equivalent of OutputFormat's checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo
   * {@link InputFormat}.
   *
   * @param job
   *          the Hadoop context for the configured job
   * @throws java.io.IOException
   *           if the context is improperly configured
   * @since 1.5.0
   */
  public static void validateOptions(JobConf job) throws IOException {
    try (AccumuloClient client = InputConfigurator.createClient(CLASS, job)) {
      InputConfigurator.validatePermissions(CLASS, job, client);
    }
  }

  /**
   * An abstract base class to be used to create {@link org.apache.hadoop.mapred.RecordReader}
   * instances that convert from Accumulo
   * {@link org.apache.accumulo.core.data.Key}/{@link org.apache.accumulo.core.data.Value} pairs to
   * the user's K/V types.
   *
   * Subclasses must implement {@link #next(Object, Object)} to update key and value, and also to
   * update the following variables:
   * <ul>
   * <li>Key {@link #currentKey} (used for progress reporting)</li>
   * <li>int {@link #numKeysRead} (used for progress reporting)</li>
   * </ul>
   */
  public abstract static class AbstractRecordReader<K,V> implements RecordReader<K,V> {
    protected long numKeysRead;
    protected AccumuloClient client;
    protected Iterator<Map.Entry<Key,Value>> scannerIterator;
    protected RangeInputSplit split;
    private org.apache.accumulo.hadoopImpl.mapreduce.RangeInputSplit baseSplit;
    protected ScannerBase scannerBase;

    /**
     * Extracts Iterators settings from the context to be used by RecordReader.
     *
     * @param job
     *          the Hadoop job configuration
     * @return List of iterator settings for given table
     * @since 1.7.0
     */
    protected abstract List<IteratorSetting> jobIterators(JobConf job);

    /**
     * Configures the iterators on a scanner for the given table name.
     *
     * @param job
     *          the Hadoop job configuration
     * @param scanner
     *          the scanner for which to configure the iterators
     * @since 1.7.0
     */
    private void setupIterators(JobConf job, ScannerBase scanner,
        org.apache.accumulo.hadoopImpl.mapreduce.RangeInputSplit split) {
      List<IteratorSetting> iterators = null;

      if (split == null) {
        iterators = jobIterators(job);
      } else {
        iterators = split.getIterators();
        if (iterators == null) {
          iterators = jobIterators(job);
        }
      }

      for (IteratorSetting iterator : iterators)
        scanner.addScanIterator(iterator);
    }

    /**
     * Initialize a scanner over the given input split using this task attempt configuration.
     */
    public void initialize(InputSplit inSplit, JobConf job) throws IOException {
      baseSplit = (org.apache.accumulo.hadoopImpl.mapreduce.RangeInputSplit) inSplit;
      log.debug("Initializing input split: " + baseSplit);

      client = createClient(job);
      ClientContext context = (ClientContext) client;
      Authorizations authorizations = getScanAuthorizations(job);
      String classLoaderContext = getClassLoaderContext(job);
      String table = baseSplit.getTableName();

      // in case the table name changed, we can still use the previous name for terms of
      // configuration, but the scanner will use the table id resolved at job setup time
      InputTableConfig tableConfig = InputConfigurator.getInputTableConfig(CLASS, job,
          baseSplit.getTableName());

      log.debug("Created client with user: " + context.whoami());
      log.debug("Creating scanner for table: " + table);
      log.debug("Authorizations are: " + authorizations);

      if (baseSplit instanceof BatchInputSplit) {
        BatchScanner scanner;
        BatchInputSplit multiRangeSplit = (BatchInputSplit) baseSplit;

        try {
          // Note: BatchScanner will use at most one thread per tablet, currently BatchInputSplit
          // will not span tablets
          int scanThreads = 1;
          scanner = context.createBatchScanner(baseSplit.getTableName(), authorizations,
              scanThreads);
          setupIterators(job, scanner, baseSplit);
          if (classLoaderContext != null) {
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
        if (isOffline == null) {
          isOffline = tableConfig.isOfflineScan();
        }

        Boolean isIsolated = baseSplit.isIsolatedScan();
        if (isIsolated == null) {
          isIsolated = tableConfig.shouldUseIsolatedScanners();
        }

        Boolean usesLocalIterators = baseSplit.usesLocalIterators();
        if (usesLocalIterators == null) {
          usesLocalIterators = tableConfig.shouldUseLocalIterators();
        }

        Scanner scanner;

        try {
          if (isOffline) {
            scanner = new OfflineScanner(context, Table.ID.of(baseSplit.getTableId()),
                authorizations);
          } else {
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
          setupIterators(job, scanner, baseSplit);
        } catch (Exception e) {
          throw new IOException(e);
        }

        scanner.setRange(baseSplit.getRange());
        scannerBase = scanner;
      } else {
        throw new IllegalArgumentException("Can not initialize from " + baseSplit.getClass());
      }

      Collection<IteratorSetting.Column> columns = baseSplit.getFetchedColumns();
      if (columns == null) {
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
      if (samplerConfig == null) {
        samplerConfig = tableConfig.getSamplerConfiguration();
      }

      if (samplerConfig != null) {
        scannerBase.setSamplerConfiguration(samplerConfig);
      }

      Map<String,String> executionHints = baseSplit.getExecutionHints();
      if (executionHints == null || executionHints.size() == 0) {
        executionHints = tableConfig.getExecutionHints();
      }

      if (executionHints != null) {
        scannerBase.setExecutionHints(executionHints);
      }

      scannerIterator = scannerBase.iterator();
      numKeysRead = 0;
    }

    @Override
    public void close() {
      if (scannerBase != null) {
        scannerBase.close();
      }
      if (client != null) {
        client.close();
      }
    }

    @Override
    public long getPos() {
      return numKeysRead;
    }

    @Override
    public float getProgress() {
      if (numKeysRead > 0 && currentKey == null)
        return 1.0f;
      return baseSplit.getProgress(currentKey);
    }

    protected Key currentKey = null;

  }

  public static Map<String,Map<KeyExtent,List<Range>>> binOfflineTable(JobConf job,
      Table.ID tableId, List<Range> ranges)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    try (AccumuloClient client = createClient(job)) {
      return InputConfigurator.binOffline(tableId, ranges, (ClientContext) client);
    }
  }

  /**
   * Gets the splits of the tables that have been set on the job by reading the metadata table for
   * the specified ranges.
   */
  public static InputSplit[] getSplits(JobConf job) throws IOException {
    validateOptions(job);

    Random random = new SecureRandom();
    LinkedList<InputSplit> splits = new LinkedList<>();
    Map<String,InputTableConfig> tableConfigs = InputConfigurator.getInputTableConfigs(CLASS, job);
    try (AccumuloClient client = createClient(job)) {
      for (Map.Entry<String,InputTableConfig> tableConfigEntry : tableConfigs.entrySet()) {
        String tableName = tableConfigEntry.getKey();
        InputTableConfig tableConfig = tableConfigEntry.getValue();

        ClientContext context = (ClientContext) client;
        Table.ID tableId;
        // resolve table name to id once, and use id from this point forward
        try {
          tableId = Tables.getTableId(context, tableName);
        } catch (TableNotFoundException e) {
          throw new IOException(e);
        }

        boolean batchScan = InputConfigurator.isBatchScan(CLASS, job);
        boolean supportBatchScan = !(tableConfig.isOfflineScan()
            || tableConfig.shouldUseIsolatedScanners() || tableConfig.shouldUseLocalIterators());
        if (batchScan && !supportBatchScan)
          throw new IllegalArgumentException("BatchScanner optimization not available for offline"
              + " scan, isolated, or local iterators");

        boolean autoAdjust = tableConfig.shouldAutoAdjustRanges();
        if (batchScan && !autoAdjust)
          throw new IllegalArgumentException(
              "AutoAdjustRanges must be enabled when using BatchScanner optimization");

        List<Range> ranges = autoAdjust ? Range.mergeOverlapping(tableConfig.getRanges())
            : tableConfig.getRanges();
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
            // its possible that the cache could contain complete, but old information about a
            // tables
            // tablets... so clear it
            tl.invalidateCache();

            while (!tl.binRanges(context, ranges, binnedRanges).isEmpty()) {
              String tableIdStr = tableId.canonicalID();
              if (!Tables.exists(context, tableId))
                throw new TableDeletedException(tableIdStr);
              if (Tables.getTableState(context, tableId) == TableState.OFFLINE)
                throw new TableOfflineException(Tables.getTableOfflineMsg(context, tableId));
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

              BatchInputSplit split = new BatchInputSplit(tableName, tableId, clippedRanges,
                  new String[] {location});
              SplitUtils.updateSplit(split, tableConfig);

              splits.add(split);
            } else {
              // not grouping by tablet
              for (Range r : extentRanges.getValue()) {
                if (autoAdjust) {
                  // divide ranges into smaller ranges, based on the tablets
                  RangeInputSplit split = new RangeInputSplit(tableName, tableId.canonicalID(),
                      ke.clip(r), new String[] {location});
                  SplitUtils.updateSplit(split, tableConfig);
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
            RangeInputSplit split = new RangeInputSplit(tableName, tableId.canonicalID(),
                entry.getKey(), entry.getValue().toArray(new String[0]));
            SplitUtils.updateSplit(split, tableConfig);
            split.setOffline(tableConfig.isOfflineScan());
            split.setIsolatedScan(tableConfig.shouldUseIsolatedScanners());
            split.setUsesLocalIterators(tableConfig.shouldUseLocalIterators());

            splits.add(split);
          }
      }
    }

    return splits.toArray(new InputSplit[splits.size()]);
  }
}
