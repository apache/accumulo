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
package org.apache.accumulo.hadoopImpl.mapreduce.lib;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.hadoopImpl.mapreduce.InputTableConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Maps;

/**
 * @since 1.6.0
 */
public class InputConfigurator extends ConfiguratorBase {

  /**
   * Configuration keys for {@link Scanner}.
   *
   * @since 1.6.0
   */
  public enum ScanOpts {
    TABLE_NAME,
    AUTHORIZATIONS,
    RANGES,
    COLUMNS,
    ITERATORS,
    TABLE_CONFIGS,
    SAMPLER_CONFIG,
    CLASSLOADER_CONTEXT,
    EXECUTION_HINTS
  }

  /**
   * Configuration keys for various features.
   *
   * @since 1.6.0
   */
  public enum Features {
    AUTO_ADJUST_RANGES,
    SCAN_ISOLATION,
    USE_LOCAL_ITERATORS,
    SCAN_OFFLINE,
    BATCH_SCANNER,
    BATCH_SCANNER_THREADS,
    CONSISTENCY_LEVEL
  }

  /**
   * Sets the name of the context classloader to use for scans
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param context the name of the context classloader
   * @since 1.8.0
   */
  public static void setClassLoaderContext(Class<?> implementingClass, Configuration conf,
      String context) {
    checkArgument(context != null, "context is null");
    conf.set(enumToConfKey(implementingClass, ScanOpts.CLASSLOADER_CONTEXT), context);
  }

  /**
   * Gets the name of the context classloader to use for scans
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return the classloader context name
   * @since 1.8.0
   */
  public static String getClassLoaderContext(Class<?> implementingClass, Configuration conf) {
    return conf.get(enumToConfKey(implementingClass, ScanOpts.CLASSLOADER_CONTEXT), null);
  }

  /**
   * Sets the name of the input table, over which this job will scan.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param tableName the table to use when the tablename is null in the write call
   * @since 1.6.0
   */
  public static void setInputTableName(Class<?> implementingClass, Configuration conf,
      String tableName) {
    checkArgument(tableName != null, "tableName is null");
    conf.set(enumToConfKey(implementingClass, ScanOpts.TABLE_NAME), tableName);
  }

  /**
   * Sets the name of the input table, over which this job will scan.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @since 1.6.0
   */
  public static String getInputTableName(Class<?> implementingClass, Configuration conf) {
    return conf.get(enumToConfKey(implementingClass, ScanOpts.TABLE_NAME));
  }

  /**
   * Sets the {@link Authorizations} used to scan. Must be a subset of the user's authorization.
   * Defaults to the empty set.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param auths the user's authorizations
   * @since 1.6.0
   */
  public static void setScanAuthorizations(Class<?> implementingClass, Configuration conf,
      Authorizations auths) {
    if (auths != null && !auths.isEmpty()) {
      conf.set(enumToConfKey(implementingClass, ScanOpts.AUTHORIZATIONS), auths.serialize());
    }
  }

  /**
   * Gets the authorizations to set for the scans from the configuration.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return the Accumulo scan authorizations
   * @since 1.6.0
   * @see #setScanAuthorizations(Class, Configuration, Authorizations)
   */
  public static Authorizations getScanAuthorizations(Class<?> implementingClass,
      Configuration conf) {
    String authString = conf.get(enumToConfKey(implementingClass, ScanOpts.AUTHORIZATIONS));
    return authString == null ? Authorizations.EMPTY
        : new Authorizations(authString.getBytes(UTF_8));
  }

  /**
   * Sets the input ranges to scan on all input tables for this job. If not set, the entire table
   * will be scanned.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param ranges the ranges that will be mapped over
   * @throws IllegalArgumentException if the ranges cannot be encoded into base 64
   * @since 1.6.0
   */
  public static void setRanges(Class<?> implementingClass, Configuration conf,
      Collection<Range> ranges) {
    checkArgument(ranges != null, "ranges is null");

    ArrayList<String> rangeStrings = new ArrayList<>(ranges.size());
    try {
      for (Range r : ranges) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        r.write(new DataOutputStream(baos));
        rangeStrings.add(Base64.getEncoder().encodeToString(baos.toByteArray()));
      }
      conf.setStrings(enumToConfKey(implementingClass, ScanOpts.RANGES),
          rangeStrings.toArray(new String[0]));
    } catch (IOException ex) {
      throw new IllegalArgumentException("Unable to encode ranges to Base64", ex);
    }
  }

  /**
   * Gets the ranges to scan over from a job.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return the ranges
   * @throws IOException if the ranges have been encoded improperly
   * @since 1.6.0
   * @see #setRanges(Class, Configuration, Collection)
   */
  public static List<Range> getRanges(Class<?> implementingClass, Configuration conf)
      throws IOException {

    Collection<String> encodedRanges =
        conf.getStringCollection(enumToConfKey(implementingClass, ScanOpts.RANGES));
    List<Range> ranges = new ArrayList<>();
    for (String rangeString : encodedRanges) {
      ByteArrayInputStream bais = new ByteArrayInputStream(Base64.getDecoder().decode(rangeString));
      Range range = new Range();
      range.readFields(new DataInputStream(bais));
      ranges.add(range);
    }
    return ranges;
  }

  /**
   * Gets a list of the iterator settings (for iterators to apply to a scanner) from this
   * configuration.
   *
   * @see #writeIteratorsToConf(Class, Configuration, Collection)
   */
  public static List<IteratorSetting> getIterators(Class<?> implementingClass, Configuration conf) {
    String iterators = conf.get(enumToConfKey(implementingClass, ScanOpts.ITERATORS));

    // If no iterators are present, return an empty list
    if (iterators == null || iterators.isEmpty()) {
      return new ArrayList<>();
    }

    // Compose the set of iterators encoded in the job configuration
    StringTokenizer tokens = new StringTokenizer(iterators, StringUtils.COMMA_STR);
    List<IteratorSetting> list = new ArrayList<>();
    try {
      while (tokens.hasMoreTokens()) {
        String itstring = tokens.nextToken();
        ByteArrayInputStream bais = new ByteArrayInputStream(Base64.getDecoder().decode(itstring));
        list.add(new IteratorSetting(new DataInputStream(bais)));
        bais.close();
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("couldn't decode iterator settings");
    }
    return list;
  }

  /**
   * Restricts the columns that will be mapped over for the single input table on this job.
   */
  public static void fetchColumns(Class<?> implementingClass, Configuration conf,
      Collection<IteratorSetting.Column> columnFamilyColumnQualifierPairs) {
    checkArgument(columnFamilyColumnQualifierPairs != null,
        "columnFamilyColumnQualifierPairs is null");
    String[] columnStrings = serializeColumns(columnFamilyColumnQualifierPairs);
    conf.setStrings(enumToConfKey(implementingClass, ScanOpts.COLUMNS), columnStrings);
  }

  public static String[]
      serializeColumns(Collection<IteratorSetting.Column> columnFamilyColumnQualifierPairs) {
    checkArgument(columnFamilyColumnQualifierPairs != null,
        "columnFamilyColumnQualifierPairs is null");
    ArrayList<String> columnStrings = new ArrayList<>(columnFamilyColumnQualifierPairs.size());
    for (Pair<Text,Text> column : columnFamilyColumnQualifierPairs) {

      if (column.getFirst() == null) {
        throw new IllegalArgumentException("Column family can not be null");
      }

      String col = Base64.getEncoder().encodeToString(TextUtil.getBytes(column.getFirst()));
      if (column.getSecond() != null) {
        col += ":" + Base64.getEncoder().encodeToString(TextUtil.getBytes(column.getSecond()));
      }
      columnStrings.add(col);
    }

    return columnStrings.toArray(new String[0]);
  }

  /**
   * Gets the columns to be mapped over from this job.
   *
   * @see #fetchColumns(Class, Configuration, Collection)
   */
  public static Set<IteratorSetting.Column> getFetchedColumns(Class<?> implementingClass,
      Configuration conf) {
    checkArgument(conf != null, "conf is null");
    String confValue = conf.get(enumToConfKey(implementingClass, ScanOpts.COLUMNS));
    List<String> serialized = new ArrayList<>();
    if (confValue != null) {
      // Split and include any trailing empty strings to allow empty column families
      Collections.addAll(serialized, confValue.split(",", -1));
    }
    return deserializeFetchedColumns(serialized);
  }

  public static Set<IteratorSetting.Column>
      deserializeFetchedColumns(Collection<String> serialized) {
    Set<IteratorSetting.Column> columns = new HashSet<>();

    if (serialized == null) {
      return columns;
    }

    for (String col : serialized) {
      int idx = col.indexOf(":");
      Text cf = new Text(idx < 0 ? Base64.getDecoder().decode(col)
          : Base64.getDecoder().decode(col.substring(0, idx)));
      Text cq = idx < 0 ? null : new Text(Base64.getDecoder().decode(col.substring(idx + 1)));
      columns.add(new IteratorSetting.Column(cf, cq));
    }
    return columns;
  }

  /**
   * Serialize the iterators to the hadoop configuration under one key.
   */
  public static void writeIteratorsToConf(Class<?> implementingClass, Configuration conf,
      Collection<IteratorSetting> iterators) {
    String confKey = enumToConfKey(implementingClass, ScanOpts.ITERATORS);
    StringBuilder iterBuilder = new StringBuilder();
    int count = 0;
    for (IteratorSetting cfg : iterators) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      String newIter;
      try {
        cfg.write(new DataOutputStream(baos));
        newIter = Base64.getEncoder().encodeToString(baos.toByteArray());
        baos.close();
      } catch (IOException e) {
        throw new IllegalArgumentException("unable to serialize IteratorSetting");
      }

      if (count == 0) {
        iterBuilder.append(newIter);
      } else {
        // append the next iterator & reset
        iterBuilder.append(StringUtils.COMMA_STR + newIter);
      }
      count++;
    }
    // Store the iterators w/ the job
    conf.set(confKey, iterBuilder.toString());
  }

  /**
   * Controls the automatic adjustment of ranges for this job. This feature merges overlapping
   * ranges, then splits them to align with tablet boundaries. Disabling this feature will cause
   * exactly one Map task to be created for each specified range. The default setting is enabled. *
   *
   * <p>
   * By default, this feature is <b>enabled</b>.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param enableFeature the feature is enabled if true, disabled otherwise
   * @see #setRanges(Class, Configuration, Collection)
   * @since 1.6.0
   */
  public static void setAutoAdjustRanges(Class<?> implementingClass, Configuration conf,
      boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.AUTO_ADJUST_RANGES), enableFeature);
  }

  /**
   * Determines whether a configuration has auto-adjust ranges enabled.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return false if the feature is disabled, true otherwise
   * @since 1.6.0
   * @see #setAutoAdjustRanges(Class, Configuration, boolean)
   */
  public static Boolean getAutoAdjustRanges(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.AUTO_ADJUST_RANGES), true);
  }

  /**
   * Controls the use of the {@link IsolatedScanner} in this job.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param enableFeature the feature is enabled if true, disabled otherwise
   * @since 1.6.0
   */
  public static void setScanIsolation(Class<?> implementingClass, Configuration conf,
      boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.SCAN_ISOLATION), enableFeature);
  }

  /**
   * Determines whether a configuration has isolation enabled.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @since 1.6.0
   * @see #setScanIsolation(Class, Configuration, boolean)
   */
  public static Boolean isIsolated(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.SCAN_ISOLATION), false);
  }

  /**
   * Controls the use of the {@link ClientSideIteratorScanner} in this job. Enabling this feature
   * will cause the iterator stack to be constructed within the Map task, rather than within the
   * Accumulo TServer. To use this feature, all classes needed for those iterators must be available
   * on the classpath for the task.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param enableFeature the feature is enabled if true, disabled otherwise
   * @since 1.6.0
   */
  public static void setLocalIterators(Class<?> implementingClass, Configuration conf,
      boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.USE_LOCAL_ITERATORS), enableFeature);
  }

  /**
   * Determines whether a configuration uses local iterators.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @since 1.6.0
   * @see #setLocalIterators(Class, Configuration, boolean)
   */
  public static Boolean usesLocalIterators(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.USE_LOCAL_ITERATORS), false);
  }

  /**
   * Enable reading offline tables. By default, this feature is disabled and only online tables are
   * scanned. This will make the map reduce job directly read the table's files. If the table is not
   * offline, then the job will fail. If the table comes online during the map reduce job, it is
   * likely that the job will fail.
   *
   * <p>
   * To use this option, the map reduce user will need access to read the Accumulo directory in
   * HDFS.
   *
   * <p>
   * Reading the offline table will create the scan time iterator stack in the map process. So any
   * iterators that are configured for the table will need to be on the mapper's classpath.
   *
   * <p>
   * One way to use this feature is to clone a table, take the clone offline, and use the clone as
   * the input table for a map reduce job. If you plan to map reduce over the data many times, it
   * may be better to the compact the table, clone it, take it offline, and use the clone for all
   * map reduce jobs. The reason to do this is that compaction will reduce each tablet in the table
   * to one file, and it is faster to read from one file.
   *
   * <p>
   * There are two possible advantages to reading a tables file directly out of HDFS. First, you may
   * see better read performance. Second, it will support speculative execution better. When reading
   * an online table speculative execution can put more load on an already slow tablet server.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param enableFeature the feature is enabled if true, disabled otherwise
   * @since 1.6.0
   */
  public static void setOfflineTableScan(Class<?> implementingClass, Configuration conf,
      boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.SCAN_OFFLINE), enableFeature);
  }

  /**
   * Determines whether a configuration has the offline table scan feature enabled.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @since 1.6.0
   * @see #setOfflineTableScan(Class, Configuration, boolean)
   */
  public static Boolean isOfflineScan(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.SCAN_OFFLINE), false);
  }

  /**
   * Controls the use of the {@link BatchScanner} in this job. Using this feature will group ranges
   * by their source tablet per InputSplit and use BatchScanner to read them.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param enableFeature the feature is enabled if true, disabled otherwise
   * @since 1.7.0
   */
  public static void setBatchScan(Class<?> implementingClass, Configuration conf,
      boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.BATCH_SCANNER), enableFeature);
  }

  /**
   * Determines whether a configuration has the BatchScanner feature enabled.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @since 1.7.0
   * @see #setBatchScan(Class, Configuration, boolean)
   */
  public static Boolean isBatchScan(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.BATCH_SCANNER), false);
  }

  /**
   * Set the ConsistencyLevel for the Accumulo scans that create the input data
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param level the consistency level
   * @since 2.1.0
   */
  public static void setConsistencyLevel(Class<?> implementingClass, Configuration conf,
      ConsistencyLevel level) {
    conf.set(enumToConfKey(implementingClass, Features.CONSISTENCY_LEVEL), level.name());
  }

  /**
   * Get the ConsistencyLevel for the Accumulo scans that create the input data
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return the consistency level
   * @since 2.1.0
   */
  public static ConsistencyLevel getConsistencyLevel(Class<?> implementingClass,
      Configuration conf) {
    return ConsistencyLevel
        .valueOf(conf.get(enumToConfKey(implementingClass, Features.CONSISTENCY_LEVEL),
            ConsistencyLevel.IMMEDIATE.name()));
  }

  /**
   * Sets configurations for multiple tables at a time.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param configs an array of {@link InputTableConfig} objects to associate with the job
   * @since 1.6.0
   */
  public static void setInputTableConfigs(Class<?> implementingClass, Configuration conf,
      Map<String,InputTableConfig> configs) {
    MapWritable mapWritable = new MapWritable();
    for (Map.Entry<String,InputTableConfig> tableConfig : configs.entrySet()) {
      mapWritable.put(new Text(tableConfig.getKey()), tableConfig.getValue());
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      mapWritable.write(new DataOutputStream(baos));
    } catch (IOException e) {
      throw new IllegalStateException("Table configuration could not be serialized.");
    }

    String confKey = enumToConfKey(implementingClass, ScanOpts.TABLE_CONFIGS);
    conf.set(confKey, Base64.getEncoder().encodeToString(baos.toByteArray()));
  }

  /**
   * Returns all {@link InputTableConfig} objects associated with this job.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @return all of the table query configs for the job
   * @since 1.6.0
   */
  public static Map<String,InputTableConfig> getInputTableConfigs(Class<?> implementingClass,
      Configuration conf) {
    return getInputTableConfigs(implementingClass, conf,
        getInputTableName(implementingClass, conf));
  }

  /**
   * Returns all {@link InputTableConfig} objects associated with this job.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param tableName the table name for which to retrieve the configuration
   * @return all of the table query configs for the job
   * @since 1.6.0
   */
  private static Map<String,InputTableConfig> getInputTableConfigs(Class<?> implementingClass,
      Configuration conf, String tableName) {
    Map<String,InputTableConfig> configs = new HashMap<>();
    Map.Entry<String,InputTableConfig> defaultConfig =
        getDefaultInputTableConfig(implementingClass, conf, tableName);
    if (defaultConfig != null) {
      configs.put(defaultConfig.getKey(), defaultConfig.getValue());
    }
    String configString = conf.get(enumToConfKey(implementingClass, ScanOpts.TABLE_CONFIGS));
    MapWritable mapWritable = new MapWritable();
    if (configString != null) {
      try {
        byte[] bytes = Base64.getDecoder().decode(configString);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        mapWritable.readFields(new DataInputStream(bais));
        bais.close();
      } catch (IOException e) {
        throw new IllegalStateException("The table query configurations could not be deserialized"
            + " from the given configuration");
      }
    }
    for (Map.Entry<Writable,Writable> entry : mapWritable.entrySet()) {
      configs.put(entry.getKey().toString(), (InputTableConfig) entry.getValue());
    }

    return configs;
  }

  /**
   * Returns the {@link InputTableConfig} for the given table
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param tableName the table name for which to fetch the table query config
   * @return the table query config for the given table name (if it exists) and null if it does not
   * @since 1.6.0
   */
  public static InputTableConfig getInputTableConfig(Class<?> implementingClass, Configuration conf,
      String tableName) {
    Map<String,InputTableConfig> queryConfigs =
        getInputTableConfigs(implementingClass, conf, tableName);
    return queryConfigs.get(tableName);
  }

  /**
   * Initializes an Accumulo {@link TabletLocator} based on the configuration.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param tableId The table id for which to initialize the {@link TabletLocator}
   * @return an Accumulo tablet locator
   * @since 1.6.0
   */
  public static TabletLocator getTabletLocator(Class<?> implementingClass, Configuration conf,
      TableId tableId) {
    try (AccumuloClient client = createClient(implementingClass, conf)) {
      return TabletLocator.getLocator((ClientContext) client, tableId);
    }
  }

  private static String extractNamespace(final String tableName) {
    final int delimiterPos = tableName.indexOf('.');
    if (delimiterPos < 1) {
      return ""; // default namespace
    } else {
      return tableName.substring(0, delimiterPos);
    }
  }

  /**
   * Validates that the user has permissions on the requested tables
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop configuration object to configure
   * @param client the Accumulo client
   * @since 1.7.0
   */
  public static void validatePermissions(Class<?> implementingClass, Configuration conf,
      AccumuloClient client) throws IOException {
    Map<String,InputTableConfig> inputTableConfigs = getInputTableConfigs(implementingClass, conf);

    try {
      if (getInputTableConfigs(implementingClass, conf).isEmpty()) {
        throw new IOException("No table set.");
      }

      Properties props = getClientProperties(implementingClass, conf);
      String principal = ClientProperty.AUTH_PRINCIPAL.getValue(props);

      for (Map.Entry<String,InputTableConfig> tableConfig : inputTableConfigs.entrySet()) {

        final String tableName = tableConfig.getKey();
        final String namespace = extractNamespace(tableName);
        final boolean hasTableRead = client.securityOperations().hasTablePermission(principal,
            tableName, TablePermission.READ);
        final boolean hasNamespaceRead = client.securityOperations()
            .hasNamespacePermission(principal, namespace, NamespacePermission.READ);

        if (!hasTableRead && !hasNamespaceRead) {
          throw new IOException("Unable to access table");
        }
      }

      for (Map.Entry<String,InputTableConfig> tableConfigEntry : inputTableConfigs.entrySet()) {
        InputTableConfig tableConfig = tableConfigEntry.getValue();
        if (!tableConfig.shouldUseLocalIterators()) {
          if (tableConfig.getIterators() != null) {
            for (IteratorSetting iter : tableConfig.getIterators()) {
              if (!client.tableOperations().testClassLoad(tableConfigEntry.getKey(),
                  iter.getIteratorClass(), SortedKeyValueIterator.class.getName())) {
                throw new AccumuloException("Servers are unable to load " + iter.getIteratorClass()
                    + " as a " + SortedKeyValueIterator.class.getName());
              }
            }
          }
        }
      }
    } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
      throw new IOException(e);
    }
  }

  /**
   * Returns the {@link InputTableConfig} for the configuration based on the properties set using
   * the single-table input methods.
   *
   * @param implementingClass the class whose name will be used as a prefix for the property
   *        configuration key
   * @param conf the Hadoop instance for which to retrieve the configuration
   * @param tableName the table name for which to retrieve the configuration
   * @return the config object built from the single input table properties set on the job
   * @since 1.6.0
   */
  protected static Map.Entry<String,InputTableConfig>
      getDefaultInputTableConfig(Class<?> implementingClass, Configuration conf, String tableName) {
    if (tableName != null) {
      InputTableConfig queryConfig = new InputTableConfig();
      List<IteratorSetting> itrs = getIterators(implementingClass, conf);
      if (itrs != null) {
        itrs.forEach(queryConfig::addIterator);
      }
      Set<IteratorSetting.Column> columns = getFetchedColumns(implementingClass, conf);
      if (columns != null) {
        queryConfig.fetchColumns(columns);
      }
      List<Range> ranges = null;
      try {
        ranges = getRanges(implementingClass, conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (ranges != null) {
        queryConfig.setRanges(ranges);
      }

      SamplerConfiguration samplerConfig = getSamplerConfiguration(implementingClass, conf);
      if (samplerConfig != null) {
        queryConfig.setSamplerConfiguration(samplerConfig);
      }

      queryConfig.setAutoAdjustRanges(getAutoAdjustRanges(implementingClass, conf))
          .setUseIsolatedScanners(isIsolated(implementingClass, conf))
          .setUseLocalIterators(usesLocalIterators(implementingClass, conf))
          .setOfflineScan(isOfflineScan(implementingClass, conf))
          .setExecutionHints(getExecutionHints(implementingClass, conf));
      return Maps.immutableEntry(tableName, queryConfig);
    }
    return null;
  }

  public static Map<String,Map<KeyExtent,List<Range>>> binOffline(TableId tableId,
      List<Range> ranges, ClientContext context) throws AccumuloException, TableNotFoundException {
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();

    if (context.getTableState(tableId) != TableState.OFFLINE) {
      context.clearTableListCache();
      if (context.getTableState(tableId) != TableState.OFFLINE) {
        throw new AccumuloException(
            "Table is online tableId:" + tableId + " cannot scan table in offline mode ");
      }
    }

    for (Range range : ranges) {
      Text startRow;

      if (range.getStartKey() != null) {
        startRow = range.getStartKey().getRow();
      } else {
        startRow = new Text();
      }

      Range metadataRange =
          new Range(new KeyExtent(tableId, startRow, null).toMetaRow(), true, null, false);
      Scanner scanner = context.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(LastLocationColumnFamily.NAME);
      scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
      scanner.fetchColumnFamily(FutureLocationColumnFamily.NAME);
      scanner.setRange(metadataRange);

      RowIterator rowIter = new RowIterator(scanner);
      KeyExtent lastExtent = null;
      while (rowIter.hasNext()) {
        Iterator<Map.Entry<Key,Value>> row = rowIter.next();
        String last = "";
        KeyExtent extent = null;
        String location = null;

        while (row.hasNext()) {
          Map.Entry<Key,Value> entry = row.next();
          Key key = entry.getKey();

          if (key.getColumnFamily().equals(LastLocationColumnFamily.NAME)) {
            last = entry.getValue().toString();
          }

          if (key.getColumnFamily().equals(CurrentLocationColumnFamily.NAME)
              || key.getColumnFamily().equals(FutureLocationColumnFamily.NAME)) {
            location = entry.getValue().toString();
          }

          if (TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)) {
            extent = KeyExtent.fromMetaPrevRow(entry);
          }

        }

        if (location != null) {
          return null;
        }

        if (!extent.tableId().equals(tableId)) {
          throw new AccumuloException("Saw unexpected table Id " + tableId + " " + extent);
        }

        if (lastExtent != null && !extent.isPreviousExtent(lastExtent)) {
          throw new AccumuloException(" " + lastExtent + " is not previous extent " + extent);
        }

        binnedRanges.computeIfAbsent(last, k -> new HashMap<>())
            .computeIfAbsent(extent, k -> new ArrayList<>()).add(range);

        if (extent.endRow() == null
            || range.afterEndKey(new Key(extent.endRow()).followingKey(PartialKey.ROW))) {
          break;
        }

        lastExtent = extent;
      }

    }
    return binnedRanges;
  }

  private static String toBase64(Writable writable) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      writable.write(dos);
      dos.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }

  private static <T extends Writable> T fromBase64(T writable, String enc) {
    ByteArrayInputStream bais = new ByteArrayInputStream(Base64.getDecoder().decode(enc));
    DataInputStream dis = new DataInputStream(bais);
    try {
      writable.readFields(dis);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return writable;
  }

  public static void setSamplerConfiguration(Class<?> implementingClass, Configuration conf,
      SamplerConfiguration samplerConfig) {
    requireNonNull(samplerConfig);

    String key = enumToConfKey(implementingClass, ScanOpts.SAMPLER_CONFIG);
    String val = toBase64(new SamplerConfigurationImpl(samplerConfig));

    conf.set(key, val);
  }

  public static SamplerConfiguration getSamplerConfiguration(Class<?> implementingClass,
      Configuration conf) {
    String key = enumToConfKey(implementingClass, ScanOpts.SAMPLER_CONFIG);

    String encodedSC = conf.get(key);
    if (encodedSC == null) {
      return null;
    }

    return fromBase64(new SamplerConfigurationImpl(), encodedSC).toSamplerConfiguration();
  }

  public static void setExecutionHints(Class<?> implementingClass, Configuration conf,
      Map<String,String> hints) {
    MapWritable mapWritable = new MapWritable();
    hints.forEach((k, v) -> mapWritable.put(new Text(k), new Text(v)));

    String key = enumToConfKey(implementingClass, ScanOpts.EXECUTION_HINTS);
    String val = toBase64(mapWritable);

    conf.set(key, val);
  }

  public static Map<String,String> getExecutionHints(Class<?> implementingClass,
      Configuration conf) {
    String key = enumToConfKey(implementingClass, ScanOpts.EXECUTION_HINTS);
    String encodedEH = conf.get(key);
    if (encodedEH == null) {
      return Collections.emptyMap();
    }

    MapWritable mapWritable = new MapWritable();
    fromBase64(mapWritable, encodedEH);

    HashMap<String,String> hints = new HashMap<>();
    mapWritable.forEach((k, v) -> hints.put(k.toString(), v.toString()));
    return hints;
  }
}
