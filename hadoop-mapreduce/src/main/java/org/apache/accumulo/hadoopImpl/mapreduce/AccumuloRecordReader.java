/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.hadoopImpl.mapreduce;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

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
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.OfflineScanner;
import org.apache.accumulo.core.clientImpl.ScannerImpl;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.InputConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link org.apache.hadoop.mapreduce.RecordReader} that converts Accumulo
 * {@link org.apache.accumulo.core.data.Key}/{@link org.apache.accumulo.core.data.Value} pairs to
 * the user's K/V types.
 */
public abstract class AccumuloRecordReader<K,V> extends RecordReader<K,V> {
  private static final SecureRandom random = new SecureRandom();
  private static final Logger log = LoggerFactory.getLogger(AccumuloRecordReader.class);
  // class to serialize configuration under in the job
  private final Class<?> CLASS;
  protected long numKeysRead;
  protected AccumuloClient client;
  protected Iterator<Map.Entry<Key,Value>> scannerIterator;
  protected ScannerBase scannerBase;
  protected RangeInputSplit split;

  public AccumuloRecordReader(Class<?> callingClass) {
    this.CLASS = callingClass;
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
   * The Key that is used to determine progress in the current InputSplit. It is not returned to the
   * client and is only used internally
   */
  protected Key currentKey = null;

  /**
   * Extracts Iterators settings from the context to be used by RecordReader.
   *
   * @param context the Hadoop context for the configured job
   * @return List of iterator settings for given table
   */
  private List<IteratorSetting> contextIterators(TaskAttemptContext context) {
    return InputConfigurator.getIterators(CLASS, context.getConfiguration());
  }

  /**
   * Configures the iterators on a scanner for the given table name. Will attempt to use
   * configuration from the InputSplit, on failure will try to extract them from TaskAttemptContext.
   *
   * @param context the Hadoop context for the configured job
   * @param scanner the scanner for which to configure the iterators
   * @param split InputSplit containing configurations
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

    for (IteratorSetting iterator : iterators) {
      scanner.addScanIterator(iterator);
    }
  }

  @Override
  public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {

    split = (RangeInputSplit) inSplit;
    log.debug("Initializing input split: " + split);
    Configuration conf = attempt.getConfiguration();

    client = createClient(attempt, this.CLASS);
    ClientContext context = (ClientContext) client;
    Authorizations authorizations = InputConfigurator.getScanAuthorizations(CLASS, conf);
    String classLoaderContext = InputConfigurator.getClassLoaderContext(CLASS, conf);
    ConsistencyLevel cl = InputConfigurator.getConsistencyLevel(CLASS, conf);
    String table = split.getTableName();

    // in case the table name changed, we can still use the previous name for terms of
    // configuration,
    // but the scanner will use the table id resolved at job setup time
    InputTableConfig tableConfig =
        InputConfigurator.getInputTableConfig(CLASS, conf, split.getTableName());

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
      } catch (TableNotFoundException e) {
        e.printStackTrace();
        throw new IOException(e);
      }

      scanner.setConsistencyLevel(cl == null ? ConsistencyLevel.IMMEDIATE : cl);
      log.info("Using consistency level: {}", scanner.getConsistencyLevel());
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
          scanner = new OfflineScanner(context, TableId.of(split.getTableId()), authorizations);
        } else {
          // Not using public API to create scanner so that we can use table ID
          // Table ID is used in case of renames during M/R job
          scanner = new ScannerImpl(context, TableId.of(split.getTableId()), authorizations);
          scanner.setConsistencyLevel(cl == null ? ConsistencyLevel.IMMEDIATE : cl);
          log.info("Using consistency level: {}", scanner.getConsistencyLevel());
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
      } catch (RuntimeException e) {
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
    if (numKeysRead > 0 && currentKey == null) {
      return 1.0f;
    }
    return split.getProgress(currentKey);
  }

  @Override
  public K getCurrentKey() {
    return currentK;
  }

  @Override
  public V getCurrentValue() {
    return currentV;
  }

  /**
   * Check whether a configuration is fully configured to be used with an Accumulo
   * {@link org.apache.hadoop.mapreduce.InputFormat}.
   */
  private static void validateOptions(JobContext context, Class<?> callingClass)
      throws IOException {
    InputConfigurator.checkJobStored(callingClass, context.getConfiguration());
    try (AccumuloClient client =
        InputConfigurator.createClient(callingClass, context.getConfiguration())) {
      InputConfigurator.validatePermissions(callingClass, context.getConfiguration(), client);
    }
  }

  private static Map<String,Map<KeyExtent,List<Range>>> binOfflineTable(JobContext context,
      TableId tableId, List<Range> ranges, Class<?> callingClass)
      throws TableNotFoundException, AccumuloException {
    try (AccumuloClient client = createClient(context, callingClass)) {
      return InputConfigurator.binOffline(tableId, ranges, (ClientContext) client);
    }
  }

  public static List<InputSplit> getSplits(JobContext context, Class<?> callingClass)
      throws IOException {
    validateOptions(context, callingClass);
    LinkedList<InputSplit> splits = new LinkedList<>();
    try (AccumuloClient client = createClient(context, callingClass);
        var clientContext = ((ClientContext) client)) {
      Map<String,InputTableConfig> tableConfigs =
          InputConfigurator.getInputTableConfigs(callingClass, context.getConfiguration());
      for (Map.Entry<String,InputTableConfig> tableConfigEntry : tableConfigs.entrySet()) {

        String tableName = tableConfigEntry.getKey();
        InputTableConfig tableConfig = tableConfigEntry.getValue();

        TableId tableId;
        // resolve table name to id once, and use id from this point forward
        try {
          tableId = clientContext.getTableId(tableName);
        } catch (TableNotFoundException e) {
          throw new IOException(e);
        }

        boolean batchScan = InputConfigurator.isBatchScan(callingClass, context.getConfiguration());
        boolean supportBatchScan = !(tableConfig.isOfflineScan()
            || tableConfig.shouldUseIsolatedScanners() || tableConfig.shouldUseLocalIterators());
        if (batchScan && !supportBatchScan) {
          throw new IllegalArgumentException("BatchScanner optimization not available for offline"
              + " scan, isolated, or local iterators");
        }

        boolean autoAdjust = tableConfig.shouldAutoAdjustRanges();
        if (batchScan && !autoAdjust) {
          throw new IllegalArgumentException(
              "AutoAdjustRanges must be enabled when using BatchScanner optimization");
        }

        List<Range> ranges =
            autoAdjust ? Range.mergeOverlapping(tableConfig.getRanges()) : tableConfig.getRanges();
        if (ranges.isEmpty()) {
          ranges = new ArrayList<>(1);
          ranges.add(new Range());
        }

        // get the metadata information for these ranges
        Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();
        TabletLocator tl;
        try {
          if (tableConfig.isOfflineScan()) {
            binnedRanges = binOfflineTable(context, tableId, ranges, callingClass);
            while (binnedRanges == null) {
              // Some tablets were still online, try again
              // sleep randomly between 100 and 200 ms
              sleepUninterruptibly(100 + random.nextInt(100), TimeUnit.MILLISECONDS);
              binnedRanges = binOfflineTable(context, tableId, ranges, callingClass);

            }
          } else {
            tl = InputConfigurator.getTabletLocator(callingClass, context.getConfiguration(),
                tableId);
            // its possible that the cache could contain complete, but old information about a
            // tables tablets... so clear it
            tl.invalidateCache();

            while (!tl.binRanges(clientContext, ranges, binnedRanges).isEmpty()) {
              clientContext.requireNotDeleted(tableId);
              clientContext.requireNotOffline(tableId, tableName);
              binnedRanges.clear();
              log.warn("Unable to locate bins for specified ranges. Retrying.");
              // sleep randomly between 100 and 200 ms
              sleepUninterruptibly(100 + random.nextInt(100), TimeUnit.MILLISECONDS);
              tl.invalidateCache();
            }
          }
        } catch (TableOfflineException | TableNotFoundException | AccumuloException
            | AccumuloSecurityException e) {
          throw new IOException(e);
        }

        // all of this code will add either range per each locations or split ranges and add
        // range-location split
        // Map from Range to Array of Locations, we only use this if we're don't split
        HashMap<Range,ArrayList<String>> splitsToAdd = null;

        if (!autoAdjust) {
          splitsToAdd = new HashMap<>();
        }

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
              for (Range r : extentRanges.getValue()) {
                clippedRanges.add(ke.clip(r));
              }
              BatchInputSplit split =
                  new BatchInputSplit(tableName, tableId, clippedRanges, new String[] {location});
              SplitUtils.updateSplit(split, tableConfig);

              splits.add(split);
            } else {
              // not grouping by tablet
              for (Range r : extentRanges.getValue()) {
                if (autoAdjust) {
                  // divide ranges into smaller ranges, based on the tablets
                  RangeInputSplit split = new RangeInputSplit(tableName, tableId.canonical(),
                      ke.clip(r), new String[] {location});
                  SplitUtils.updateSplit(split, tableConfig);
                  split.setOffline(tableConfig.isOfflineScan());
                  split.setIsolatedScan(tableConfig.shouldUseIsolatedScanners());
                  split.setUsesLocalIterators(tableConfig.shouldUseLocalIterators());
                  splits.add(split);
                } else {
                  // don't divide ranges
                  ArrayList<String> locations = splitsToAdd.get(r);
                  if (locations == null) {
                    locations = new ArrayList<>(1);
                  }
                  locations.add(location);
                  splitsToAdd.put(r, locations);
                }
              }
            }
          }
        }

        if (!autoAdjust) {
          for (Map.Entry<Range,ArrayList<String>> entry : splitsToAdd.entrySet()) {
            RangeInputSplit split = new RangeInputSplit(tableName, tableId.canonical(),
                entry.getKey(), entry.getValue().toArray(new String[0]));
            SplitUtils.updateSplit(split, tableConfig);
            split.setOffline(tableConfig.isOfflineScan());
            split.setIsolatedScan(tableConfig.shouldUseIsolatedScanners());
            split.setUsesLocalIterators(tableConfig.shouldUseLocalIterators());

            splits.add(split);
          }
        }
      }
    }
    return splits;
  }

  /**
   * Creates {@link AccumuloClient} from the configuration
   */
  private static AccumuloClient createClient(JobContext context, Class<?> callingClass) {
    return InputConfigurator.createClient(callingClass, context.getConfiguration());
  }
}
