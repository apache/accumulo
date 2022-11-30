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
package org.apache.accumulo.hadoopImpl.mapred;

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
 * @see org.apache.accumulo.hadoopImpl.mapreduce.AccumuloRecordReader
 */
public abstract class AccumuloRecordReader<K,V> implements RecordReader<K,V> {

  private static final SecureRandom random = new SecureRandom();
  // class to serialize configuration under in the job
  private final Class<?> CLASS;
  private static final Logger log = LoggerFactory.getLogger(AccumuloRecordReader.class);

  protected long numKeysRead;
  protected AccumuloClient client;
  protected Iterator<Map.Entry<Key,Value>> scannerIterator;
  protected RangeInputSplit split;
  private org.apache.accumulo.hadoopImpl.mapreduce.RangeInputSplit baseSplit;
  protected ScannerBase scannerBase;

  public AccumuloRecordReader(Class<?> callingClass) {
    this.CLASS = callingClass;
  }

  /**
   * Extracts Iterators settings from the context to be used by RecordReader.
   *
   * @param job the Hadoop job configuration
   * @return List of iterator settings for given table
   */
  private List<IteratorSetting> jobIterators(JobConf job) {
    return InputConfigurator.getIterators(CLASS, job);
  }

  /**
   * Configures the iterators on a scanner for the given table name.
   *
   * @param job the Hadoop job configuration
   * @param scanner the scanner for which to configure the iterators
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

    for (IteratorSetting iterator : iterators) {
      scanner.addScanIterator(iterator);
    }
  }

  /**
   * Initialize a scanner over the given input split using this task attempt configuration.
   */
  public void initialize(InputSplit inSplit, JobConf job) throws IOException {
    baseSplit = (org.apache.accumulo.hadoopImpl.mapreduce.RangeInputSplit) inSplit;
    log.debug("Initializing input split: " + baseSplit);

    client = createClient(job, CLASS);
    ClientContext context = (ClientContext) client;
    Authorizations authorizations = InputConfigurator.getScanAuthorizations(CLASS, job);
    String classLoaderContext = InputConfigurator.getClassLoaderContext(CLASS, job);
    ConsistencyLevel cl = InputConfigurator.getConsistencyLevel(CLASS, job);
    String table = baseSplit.getTableName();

    // in case the table name changed, we can still use the previous name for terms of
    // configuration, but the scanner will use the table id resolved at job setup time
    InputTableConfig tableConfig =
        InputConfigurator.getInputTableConfig(CLASS, job, baseSplit.getTableName());

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
        scanner = context.createBatchScanner(baseSplit.getTableName(), authorizations, scanThreads);
        setupIterators(job, scanner, baseSplit);
        if (classLoaderContext != null) {
          scanner.setClassLoaderContext(classLoaderContext);
        }
      } catch (TableNotFoundException e) {
        throw new IOException(e);
      }
      scanner.setConsistencyLevel(cl == null ? ConsistencyLevel.IMMEDIATE : cl);
      log.info("Using consistency level: {}", scanner.getConsistencyLevel());
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
          scanner = new OfflineScanner(context, TableId.of(baseSplit.getTableId()), authorizations);
        } else {
          scanner = new ScannerImpl(context, TableId.of(baseSplit.getTableId()), authorizations);
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
        setupIterators(job, scanner, baseSplit);
      } catch (RuntimeException e) {
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
  public long getPos() {
    return numKeysRead;
  }

  @Override
  public float getProgress() {
    if (numKeysRead > 0 && currentKey == null) {
      return 1.0f;
    }
    return baseSplit.getProgress(currentKey);
  }

  protected Key currentKey = null;

  private static Map<String,Map<KeyExtent,List<Range>>> binOfflineTable(JobConf job,
      TableId tableId, List<Range> ranges, Class<?> callingClass)
      throws TableNotFoundException, AccumuloException {
    try (AccumuloClient client = createClient(job, callingClass)) {
      return InputConfigurator.binOffline(tableId, ranges, (ClientContext) client);
    }
  }

  /**
   * Gets the splits of the tables that have been set on the job by reading the metadata table for
   * the specified ranges.
   */
  public static InputSplit[] getSplits(JobConf job, Class<?> callingClass) throws IOException {
    validateOptions(job, callingClass);

    LinkedList<InputSplit> splits = new LinkedList<>();
    Map<String,InputTableConfig> tableConfigs =
        InputConfigurator.getInputTableConfigs(callingClass, job);
    try (AccumuloClient client = createClient(job, callingClass);
        var context = ((ClientContext) client)) {
      for (Map.Entry<String,InputTableConfig> tableConfigEntry : tableConfigs.entrySet()) {
        String tableName = tableConfigEntry.getKey();
        InputTableConfig tableConfig = tableConfigEntry.getValue();

        TableId tableId;
        // resolve table name to id once, and use id from this point forward
        try {
          tableId = context.getTableId(tableName);
        } catch (TableNotFoundException e) {
          throw new IOException(e);
        }

        boolean batchScan = InputConfigurator.isBatchScan(callingClass, job);
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
            binnedRanges = binOfflineTable(job, tableId, ranges, callingClass);
            while (binnedRanges == null) {
              // Some tablets were still online, try again
              // sleep randomly between 100 and 200 ms
              sleepUninterruptibly(100 + random.nextInt(100), TimeUnit.MILLISECONDS);
              binnedRanges = binOfflineTable(job, tableId, ranges, callingClass);
            }
          } else {
            tl = InputConfigurator.getTabletLocator(callingClass, job, tableId);
            // its possible that the cache could contain complete, but old information about a
            // tables
            // tablets... so clear it
            tl.invalidateCache();

            while (!tl.binRanges(context, ranges, binnedRanges).isEmpty()) {
              context.requireNotDeleted(tableId);
              context.requireNotOffline(tableId, tableName);
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

    return splits.toArray(new InputSplit[splits.size()]);
  }

  // InputFormat doesn't have the equivalent of OutputFormat's checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo
   * {@link InputFormat}.
   */
  private static void validateOptions(JobConf job, Class<?> callingClass) throws IOException {
    InputConfigurator.checkJobStored(callingClass, job);
    try (AccumuloClient client = InputConfigurator.createClient(callingClass, job)) {
      InputConfigurator.validatePermissions(callingClass, job, client);
    }
  }

  /**
   * Creates {@link AccumuloClient} from the configuration
   */
  private static AccumuloClient createClient(JobConf job, Class<?> callingClass) {
    return InputConfigurator.createClient(callingClass, job);
  }
}
