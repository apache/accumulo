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
package org.apache.accumulo.hadoopImpl.mapreduce;

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
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.InputConfigurator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
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
  public static void setClassLoaderContext(Job job, String context) {
    InputConfigurator.setClassLoaderContext(CLASS, job.getConfiguration(), context);
  }

  /**
   * Returns the name of the current classloader context set on this scanner
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @return name of the current context
   * @since 1.8.0
   */
  protected static String getClassLoaderContext(JobContext job) {
    return InputConfigurator.getClassLoaderContext(CLASS, job.getConfiguration());
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
  public static void setClientInfo(Job job, ClientInfo info) {
    ClientInfo inputInfo = InputConfigurator.updateToken(job.getCredentials(), info);
    InputConfigurator.setClientInfo(CLASS, job.getConfiguration(), inputInfo);
  }

  /**
   * Set Accumulo client properties file used to connect to Accumulo
   *
   * @param job
   *          Hadoop job to be configured
   * @param clientPropsFile
   *          URL (hdfs:// or http://) to Accumulo client properties file
   * @since 2.0.0
   */
  protected static void setClientPropertiesFile(Job job, String clientPropsFile) {
    InputConfigurator.setClientPropertiesFile(CLASS, job.getConfiguration(), clientPropsFile);
  }

  /**
   * Gets the {@link ClientInfo} from the configuration
   *
   * @param context
   *          Hadoop job context
   * @return ClientInfo
   * @since 2.0.0
   */
  public static ClientInfo getClientInfo(JobContext context) {
    return InputConfigurator.getClientInfo(CLASS, context.getConfiguration());
  }

  /**
   * Creates {@link AccumuloClient} from the configuration
   *
   * @param context
   *          Hadoop job context
   * @return AccumuloClient
   * @since 2.0.0
   */
  public static AccumuloClient createClient(JobContext context) {
    return Accumulo.newClient().from(getClientInfo(context).getProperties()).build();
  }

  /**
   * Sets the {@link org.apache.accumulo.core.security.Authorizations} used to scan. Must be a
   * subset of the user's authorization. Defaults to the empty set.
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
    return InputConfigurator.getScanAuthorizations(CLASS, context.getConfiguration());
  }

  // InputFormat doesn't have the equivalent of OutputFormat's checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo
   * {@link org.apache.hadoop.mapreduce.InputFormat}.
   *
   * @param context
   *          the Hadoop context for the configured job
   * @throws java.io.IOException
   *           if the context is improperly configured
   * @since 1.5.0
   */
  public static void validateOptions(JobContext context) throws IOException {
    try (
        AccumuloClient client = InputConfigurator.createClient(CLASS, context.getConfiguration())) {
      InputConfigurator.validatePermissions(CLASS, context.getConfiguration(), client);
    }
  }

  /**
   * An abstract base class to be used to create {@link org.apache.hadoop.mapreduce.RecordReader}
   * instances that convert from Accumulo
   * {@link org.apache.accumulo.core.data.Key}/{@link org.apache.accumulo.core.data.Value} pairs to
   * the user's K/V types.
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
    protected AccumuloClient client;
    protected Iterator<Map.Entry<Key,Value>> scannerIterator;
    protected ScannerBase scannerBase;
    protected RangeInputSplit split;

    /**
     * Extracts Iterators settings from the context to be used by RecordReader.
     *
     * @param context
     *          the Hadoop context for the configured job
     * @return List of iterator settings for given table
     * @since 1.7.0
     */
    protected abstract List<IteratorSetting> contextIterators(TaskAttemptContext context);

    /**
     * Configures the iterators on a scanner for the given table name. Will attempt to use
     * configuration from the InputSplit, on failure will try to extract them from
     * TaskAttemptContext.
     *
     * @param context
     *          the Hadoop context for the configured job
     * @param scanner
     *          the scanner for which to configure the iterators
     * @param split
     *          InputSplit containing configurations
     * @since 1.7.0
     */
    private void setupIterators(TaskAttemptContext context, ScannerBase scanner,
        RangeInputSplit split) {
      List<IteratorSetting> iterators = null;

      if (split == null) {
        iterators = contextIterators(context);
      } else {
        iterators = split.getIterators();
        if (iterators == null) {
          iterators = contextIterators(context);
        }
      }

      for (IteratorSetting iterator : iterators)
        scanner.addScanIterator(iterator);
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {

      split = (RangeInputSplit) inSplit;
      log.debug("Initializing input split: " + split);

      client = createClient(attempt);
      ClientContext context = (ClientContext) client;
      Authorizations authorizations = getScanAuthorizations(attempt);
      String classLoaderContext = getClassLoaderContext(attempt);
      String table = split.getTableName();

      // in case the table name changed, we can still use the previous name for terms of
      // configuration,
      // but the scanner will use the table id resolved at job setup time
      InputTableConfig tableConfig = InputConfigurator.getInputTableConfig(CLASS,
          attempt.getConfiguration(), split.getTableName());

      log.debug("Creating client with user: " + client.whoami());
      log.debug("Creating scanner for table: " + table);
      log.debug("Authorizations are: " + authorizations);

      if (split instanceof BatchInputSplit) {
        BatchInputSplit batchSplit = (BatchInputSplit) split;

        BatchScanner scanner;
        try {
          // Note: BatchScanner will use at most one thread per tablet, currently BatchInputSplit
          // will not span tablets
          int scanThreads = 1;
          scanner = context.createBatchScanner(split.getTableName(), authorizations, scanThreads);
          setupIterators(attempt, scanner, split);
          if (classLoaderContext != null) {
            scanner.setClassLoaderContext(classLoaderContext);
          }
        } catch (Exception e) {
          e.printStackTrace();
          throw new IOException(e);
        }

        scanner.setRanges(batchSplit.getRanges());
        scannerBase = scanner;
      } else {
        Scanner scanner;

        Boolean isOffline = split.isOffline();
        if (isOffline == null) {
          isOffline = tableConfig.isOfflineScan();
        }

        Boolean isIsolated = split.isIsolatedScan();
        if (isIsolated == null) {
          isIsolated = tableConfig.shouldUseIsolatedScanners();
        }

        Boolean usesLocalIterators = split.usesLocalIterators();
        if (usesLocalIterators == null) {
          usesLocalIterators = tableConfig.shouldUseLocalIterators();
        }

        try {
          if (isOffline) {
            scanner = new OfflineScanner(context, Table.ID.of(split.getTableId()), authorizations);
          } else {
            // Not using public API to create scanner so that we can use table ID
            // Table ID is used in case of renames during M/R job
            scanner = new ScannerImpl(context, Table.ID.of(split.getTableId()), authorizations);
          }
          if (isIsolated) {
            log.info("Creating isolated scanner");
            scanner = new IsolatedScanner(scanner);
          }
          if (usesLocalIterators) {
            log.info("Using local iterators");
            scanner = new ClientSideIteratorScanner(scanner);
          }

          setupIterators(attempt, scanner, split);
        } catch (Exception e) {
          throw new IOException(e);
        }

        scanner.setRange(split.getRange());
        scannerBase = scanner;

      }

      Collection<IteratorSetting.Column> columns = split.getFetchedColumns();
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

      SamplerConfiguration samplerConfig = split.getSamplerConfiguration();
      if (samplerConfig == null) {
        samplerConfig = tableConfig.getSamplerConfiguration();
      }

      if (samplerConfig != null) {
        scannerBase.setSamplerConfiguration(samplerConfig);
      }

      Map<String,String> executionHints = split.getExecutionHints();
      if (executionHints == null || executionHints.isEmpty()) {
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
    public float getProgress() {
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
     * The Key that is used to determine progress in the current InputSplit. It is not returned to
     * the client and is only used internally
     */
    protected Key currentKey = null;

    @Override
    public K getCurrentKey() {
      return currentK;
    }

    @Override
    public V getCurrentValue() {
      return currentV;
    }
  }

  public static Map<String,Map<KeyExtent,List<Range>>> binOfflineTable(JobContext context,
      Table.ID tableId, List<Range> ranges)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    try (AccumuloClient client = createClient(context)) {
      return InputConfigurator.binOffline(tableId, ranges, (ClientContext) client);
    }
  }

  public static List<InputSplit> getSplits(JobContext context) throws IOException {
    validateOptions(context);
    Random random = new SecureRandom();
    LinkedList<InputSplit> splits = new LinkedList<>();
    try (AccumuloClient client = createClient(context)) {
      Map<String,InputTableConfig> tableConfigs = InputConfigurator.getInputTableConfigs(CLASS,
          context.getConfiguration());
      for (Map.Entry<String,InputTableConfig> tableConfigEntry : tableConfigs.entrySet()) {

        String tableName = tableConfigEntry.getKey();
        InputTableConfig tableConfig = tableConfigEntry.getValue();

        ClientContext clientContext = (ClientContext) client;
        Table.ID tableId;
        // resolve table name to id once, and use id from this point forward
        try {
          tableId = Tables.getTableId(clientContext, tableName);
        } catch (TableNotFoundException e) {
          throw new IOException(e);
        }

        boolean batchScan = InputConfigurator.isBatchScan(CLASS, context.getConfiguration());
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
            binnedRanges = binOfflineTable(context, tableId, ranges);
            while (binnedRanges == null) {
              // Some tablets were still online, try again
              // sleep randomly between 100 and 200 ms
              sleepUninterruptibly(100 + random.nextInt(100), TimeUnit.MILLISECONDS);
              binnedRanges = binOfflineTable(context, tableId, ranges);

            }
          } else {
            tl = InputConfigurator.getTabletLocator(CLASS, context.getConfiguration(), tableId);
            // its possible that the cache could contain complete, but old information about a
            // tables tablets... so clear it
            tl.invalidateCache();

            while (!tl.binRanges(clientContext, ranges, binnedRanges).isEmpty()) {
              String tableIdStr = tableId.canonicalID();
              if (!Tables.exists(clientContext, tableId))
                throw new TableDeletedException(tableIdStr);
              if (Tables.getTableState(clientContext, tableId) == TableState.OFFLINE)
                throw new TableOfflineException(Tables.getTableOfflineMsg(clientContext, tableId));
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

        // all of this code will add either range per each locations or split ranges and add
        // range-location split
        // Map from Range to Array of Locations, we only use this if we're don't split
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
    return splits;
  }

}
