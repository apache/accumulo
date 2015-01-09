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
package org.apache.accumulo.core.client.mapreduce.lib.impl;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.accumulo.core.util.ArgumentChecker.notNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mapreduce.InputTableConfig;
import org.apache.accumulo.core.client.mock.impl.MockTabletLocator;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
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
  public static enum ScanOpts {
    TABLE_NAME, AUTHORIZATIONS, RANGES, COLUMNS, ITERATORS, TABLE_CONFIGS
  }

  /**
   * Configuration keys for various features.
   *
   * @since 1.6.0
   */
  public static enum Features {
    AUTO_ADJUST_RANGES, SCAN_ISOLATION, USE_LOCAL_ITERATORS, SCAN_OFFLINE
  }

  /**
   * Sets the name of the input table, over which this job will scan.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param tableName
   *          the table to use when the tablename is null in the write call
   * @since 1.6.0
   */
  public static void setInputTableName(Class<?> implementingClass, Configuration conf, String tableName) {
    notNull(tableName);
    conf.set(enumToConfKey(implementingClass, ScanOpts.TABLE_NAME), tableName);
  }

  /**
   * Sets the name of the input table, over which this job will scan.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @since 1.6.0
   */
  public static String getInputTableName(Class<?> implementingClass, Configuration conf) {
    return conf.get(enumToConfKey(implementingClass, ScanOpts.TABLE_NAME));
  }

  /**
   * Sets the {@link Authorizations} used to scan. Must be a subset of the user's authorization. Defaults to the empty set.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param auths
   *          the user's authorizations
   * @since 1.6.0
   */
  public static void setScanAuthorizations(Class<?> implementingClass, Configuration conf, Authorizations auths) {
    if (auths != null && !auths.isEmpty())
      conf.set(enumToConfKey(implementingClass, ScanOpts.AUTHORIZATIONS), auths.serialize());
  }

  /**
   * Gets the authorizations to set for the scans from the configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the Accumulo scan authorizations
   * @since 1.6.0
   * @see #setScanAuthorizations(Class, Configuration, Authorizations)
   */
  public static Authorizations getScanAuthorizations(Class<?> implementingClass, Configuration conf) {
    String authString = conf.get(enumToConfKey(implementingClass, ScanOpts.AUTHORIZATIONS));
    return authString == null ? Authorizations.EMPTY : new Authorizations(authString.getBytes(UTF_8));
  }

  /**
   * Sets the input ranges to scan on all input tables for this job. If not set, the entire table will be scanned.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param ranges
   *          the ranges that will be mapped over
   * @throws IllegalArgumentException
   *           if the ranges cannot be encoded into base 64
   * @since 1.6.0
   */
  public static void setRanges(Class<?> implementingClass, Configuration conf, Collection<Range> ranges) {
    notNull(ranges);

    ArrayList<String> rangeStrings = new ArrayList<String>(ranges.size());
    try {
      for (Range r : ranges) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        r.write(new DataOutputStream(baos));
        rangeStrings.add(Base64.encodeBase64String(baos.toByteArray()));
      }
      conf.setStrings(enumToConfKey(implementingClass, ScanOpts.RANGES), rangeStrings.toArray(new String[0]));
    } catch (IOException ex) {
      throw new IllegalArgumentException("Unable to encode ranges to Base64", ex);
    }
  }

  /**
   * Gets the ranges to scan over from a job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the ranges
   * @throws IOException
   *           if the ranges have been encoded improperly
   * @since 1.6.0
   * @see #setRanges(Class, Configuration, Collection)
   */
  public static List<Range> getRanges(Class<?> implementingClass, Configuration conf) throws IOException {

    Collection<String> encodedRanges = conf.getStringCollection(enumToConfKey(implementingClass, ScanOpts.RANGES));
    List<Range> ranges = new ArrayList<Range>();
    for (String rangeString : encodedRanges) {
      ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(rangeString.getBytes()));
      Range range = new Range();
      range.readFields(new DataInputStream(bais));
      ranges.add(range);
    }
    return ranges;
  }

  /**
   * Gets a list of the iterator settings (for iterators to apply to a scanner) from this configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return a list of iterators
   * @since 1.6.0
   * @see #addIterator(Class, Configuration, IteratorSetting)
   */
  public static List<IteratorSetting> getIterators(Class<?> implementingClass, Configuration conf) {
    String iterators = conf.get(enumToConfKey(implementingClass, ScanOpts.ITERATORS));

    // If no iterators are present, return an empty list
    if (iterators == null || iterators.isEmpty())
      return new ArrayList<IteratorSetting>();

    // Compose the set of iterators encoded in the job configuration
    StringTokenizer tokens = new StringTokenizer(iterators, StringUtils.COMMA_STR);
    List<IteratorSetting> list = new ArrayList<IteratorSetting>();
    try {
      while (tokens.hasMoreTokens()) {
        String itstring = tokens.nextToken();
        ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(itstring.getBytes()));
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
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param columnFamilyColumnQualifierPairs
   *          a pair of {@link Text} objects corresponding to column family and column qualifier. If the column qualifier is null, the entire column family is
   *          selected. An empty set is the default and is equivalent to scanning the all columns.
   * @throws IllegalArgumentException
   *           if the column family is null
   * @since 1.6.0
   */
  public static void fetchColumns(Class<?> implementingClass, Configuration conf, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    notNull(columnFamilyColumnQualifierPairs);
    String[] columnStrings = serializeColumns(columnFamilyColumnQualifierPairs);
    conf.setStrings(enumToConfKey(implementingClass, ScanOpts.COLUMNS), columnStrings);
  }

  public static String[] serializeColumns(Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    notNull(columnFamilyColumnQualifierPairs);
    ArrayList<String> columnStrings = new ArrayList<String>(columnFamilyColumnQualifierPairs.size());
    for (Pair<Text,Text> column : columnFamilyColumnQualifierPairs) {

      if (column.getFirst() == null)
        throw new IllegalArgumentException("Column family can not be null");

      String col = Base64.encodeBase64String(TextUtil.getBytes(column.getFirst()));
      if (column.getSecond() != null)
        col += ":" + Base64.encodeBase64String(TextUtil.getBytes(column.getSecond()));
      columnStrings.add(col);
    }

    return columnStrings.toArray(new String[0]);
  }

  /**
   * Gets the columns to be mapped over from this job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return a set of columns
   * @since 1.6.0
   * @see #fetchColumns(Class, Configuration, Collection)
   */
  public static Set<Pair<Text,Text>> getFetchedColumns(Class<?> implementingClass, Configuration conf) {
    notNull(conf);
    String confValue = conf.get(enumToConfKey(implementingClass, ScanOpts.COLUMNS));
    List<String> serialized = new ArrayList<String>();
    if (confValue != null) {
      // Split and include any trailing empty strings to allow empty column families
      for (String val : confValue.split(",", -1)) {
        serialized.add(val);
      }
    }
    return deserializeFetchedColumns(serialized);
  }

  public static Set<Pair<Text,Text>> deserializeFetchedColumns(Collection<String> serialized) {
    Set<Pair<Text,Text>> columns = new HashSet<Pair<Text,Text>>();

    if (null == serialized) {
      return columns;
    }

    for (String col : serialized) {
      int idx = col.indexOf(":");
      Text cf = new Text(idx < 0 ? Base64.decodeBase64(col.getBytes(UTF_8)) : Base64.decodeBase64(col.substring(0, idx).getBytes(UTF_8)));
      Text cq = idx < 0 ? null : new Text(Base64.decodeBase64(col.substring(idx + 1).getBytes(UTF_8)));
      columns.add(new Pair<Text,Text>(cf, cq));
    }
    return columns;
  }

  /**
   * Encode an iterator on the input for the single input table associated with this job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param cfg
   *          the configuration of the iterator
   * @throws IllegalArgumentException
   *           if the iterator can't be serialized into the configuration
   * @since 1.6.0
   */
  public static void addIterator(Class<?> implementingClass, Configuration conf, IteratorSetting cfg) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String newIter;
    try {
      cfg.write(new DataOutputStream(baos));
      newIter = Base64.encodeBase64String(baos.toByteArray());
      baos.close();
    } catch (IOException e) {
      throw new IllegalArgumentException("unable to serialize IteratorSetting");
    }

    String confKey = enumToConfKey(implementingClass, ScanOpts.ITERATORS);
    String iterators = conf.get(confKey);
    // No iterators specified yet, create a new string
    if (iterators == null || iterators.isEmpty()) {
      iterators = newIter;
    } else {
      // append the next iterator & reset
      iterators = iterators.concat(StringUtils.COMMA_STR + newIter);
    }
    // Store the iterators w/ the job
    conf.set(confKey, iterators);
  }

  /**
   * Controls the automatic adjustment of ranges for this job. This feature merges overlapping ranges, then splits them to align with tablet boundaries.
   * Disabling this feature will cause exactly one Map task to be created for each specified range. The default setting is enabled. *
   *
   * <p>
   * By default, this feature is <b>enabled</b>.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @see #setRanges(Class, Configuration, Collection)
   * @since 1.6.0
   */
  public static void setAutoAdjustRanges(Class<?> implementingClass, Configuration conf, boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.AUTO_ADJUST_RANGES), enableFeature);
  }

  /**
   * Determines whether a configuration has auto-adjust ranges enabled.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
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
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @since 1.6.0
   */
  public static void setScanIsolation(Class<?> implementingClass, Configuration conf, boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.SCAN_ISOLATION), enableFeature);
  }

  /**
   * Determines whether a configuration has isolation enabled.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @since 1.6.0
   * @see #setScanIsolation(Class, Configuration, boolean)
   */
  public static Boolean isIsolated(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.SCAN_ISOLATION), false);
  }

  /**
   * Controls the use of the {@link ClientSideIteratorScanner} in this job. Enabling this feature will cause the iterator stack to be constructed within the Map
   * task, rather than within the Accumulo TServer. To use this feature, all classes needed for those iterators must be available on the classpath for the task.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @since 1.6.0
   */
  public static void setLocalIterators(Class<?> implementingClass, Configuration conf, boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.USE_LOCAL_ITERATORS), enableFeature);
  }

  /**
   * Determines whether a configuration uses local iterators.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @since 1.6.0
   * @see #setLocalIterators(Class, Configuration, boolean)
   */
  public static Boolean usesLocalIterators(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.USE_LOCAL_ITERATORS), false);
  }

  /**
   * <p>
   * Enable reading offline tables. By default, this feature is disabled and only online tables are scanned. This will make the map reduce job directly read the
   * table's files. If the table is not offline, then the job will fail. If the table comes online during the map reduce job, it is likely that the job will
   * fail.
   *
   * <p>
   * To use this option, the map reduce user will need access to read the Accumulo directory in HDFS.
   *
   * <p>
   * Reading the offline table will create the scan time iterator stack in the map process. So any iterators that are configured for the table will need to be
   * on the mapper's classpath.
   *
   * <p>
   * One way to use this feature is to clone a table, take the clone offline, and use the clone as the input table for a map reduce job. If you plan to map
   * reduce over the data many times, it may be better to the compact the table, clone it, take it offline, and use the clone for all map reduce jobs. The
   * reason to do this is that compaction will reduce each tablet in the table to one file, and it is faster to read from one file.
   *
   * <p>
   * There are two possible advantages to reading a tables file directly out of HDFS. First, you may see better read performance. Second, it will support
   * speculative execution better. When reading an online table speculative execution can put more load on an already slow tablet server.
   *
   * <p>
   * By default, this feature is <b>disabled</b>.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param enableFeature
   *          the feature is enabled if true, disabled otherwise
   * @since 1.6.0
   */
  public static void setOfflineTableScan(Class<?> implementingClass, Configuration conf, boolean enableFeature) {
    conf.setBoolean(enumToConfKey(implementingClass, Features.SCAN_OFFLINE), enableFeature);
  }

  /**
   * Determines whether a configuration has the offline table scan feature enabled.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return true if the feature is enabled, false otherwise
   * @since 1.6.0
   * @see #setOfflineTableScan(Class, Configuration, boolean)
   */
  public static Boolean isOfflineScan(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.SCAN_OFFLINE), false);
  }

  /**
   * Sets configurations for multiple tables at a time.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param configs
   *          an array of {@link InputTableConfig} objects to associate with the job
   * @since 1.6.0
   */
  public static void setInputTableConfigs(Class<?> implementingClass, Configuration conf, Map<String,InputTableConfig> configs) {
    MapWritable mapWritable = new MapWritable();
    for (Map.Entry<String,InputTableConfig> tableConfig : configs.entrySet())
      mapWritable.put(new Text(tableConfig.getKey()), tableConfig.getValue());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      mapWritable.write(new DataOutputStream(baos));
    } catch (IOException e) {
      throw new IllegalStateException("Table configuration could not be serialized.");
    }

    String confKey = enumToConfKey(implementingClass, ScanOpts.TABLE_CONFIGS);
    conf.set(confKey, Base64.encodeBase64String(baos.toByteArray()));
  }

  /**
   * Returns all {@link InputTableConfig} objects associated with this job.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return all of the table query configs for the job
   * @since 1.6.0
   */
  public static Map<String,InputTableConfig> getInputTableConfigs(Class<?> implementingClass, Configuration conf) {
    Map<String,InputTableConfig> configs = new HashMap<String,InputTableConfig>();
    Map.Entry<String,InputTableConfig> defaultConfig = getDefaultInputTableConfig(implementingClass, conf);
    if (defaultConfig != null)
      configs.put(defaultConfig.getKey(), defaultConfig.getValue());
    String configString = conf.get(enumToConfKey(implementingClass, ScanOpts.TABLE_CONFIGS));
    MapWritable mapWritable = new MapWritable();
    if (configString != null) {
      try {
        byte[] bytes = Base64.decodeBase64(configString.getBytes());
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        mapWritable.readFields(new DataInputStream(bais));
        bais.close();
      } catch (IOException e) {
        throw new IllegalStateException("The table query configurations could not be deserialized from the given configuration");
      }
    }
    for (Map.Entry<Writable,Writable> entry : mapWritable.entrySet())
      configs.put(((Text) entry.getKey()).toString(), (InputTableConfig) entry.getValue());

    return configs;
  }

  /**
   * Returns the {@link InputTableConfig} for the given table
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param tableName
   *          the table name for which to fetch the table query config
   * @return the table query config for the given table name (if it exists) and null if it does not
   * @since 1.6.0
   */
  public static InputTableConfig getInputTableConfig(Class<?> implementingClass, Configuration conf, String tableName) {
    Map<String,InputTableConfig> queryConfigs = getInputTableConfigs(implementingClass, conf);
    return queryConfigs.get(tableName);
  }

  /**
   * Initializes an Accumulo {@link TabletLocator} based on the configuration.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param tableId
   *          The table id for which to initialize the {@link TabletLocator}
   * @return an Accumulo tablet locator
   * @throws TableNotFoundException
   *           if the table name set on the configuration doesn't exist
   * @since 1.6.0
   */
  public static TabletLocator getTabletLocator(Class<?> implementingClass, Configuration conf, String tableId) throws TableNotFoundException {
    String instanceType = conf.get(enumToConfKey(implementingClass, InstanceOpts.TYPE));
    if ("MockInstance".equals(instanceType))
      return new MockTabletLocator();
    Instance instance = getInstance(implementingClass, conf);
    return TabletLocator.getLocator(instance, new Text(tableId));
  }

  // InputFormat doesn't have the equivalent of OutputFormat's checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @throws IOException
   *           if the context is improperly configured
   * @since 1.6.0
   */
  public static void validateOptions(Class<?> implementingClass, Configuration conf) throws IOException {

    Map<String,InputTableConfig> inputTableConfigs = getInputTableConfigs(implementingClass, conf);
    if (!isConnectorInfoSet(implementingClass, conf))
      throw new IOException("Input info has not been set.");
    String instanceKey = conf.get(enumToConfKey(implementingClass, InstanceOpts.TYPE));
    if (!"MockInstance".equals(instanceKey) && !"ZooKeeperInstance".equals(instanceKey))
      throw new IOException("Instance info has not been set.");
    // validate that we can connect as configured
    try {
      String principal = getPrincipal(implementingClass, conf);
      AuthenticationToken token = getAuthenticationToken(implementingClass, conf);
      Connector c = getInstance(implementingClass, conf).getConnector(principal, token);
      if (!c.securityOperations().authenticateUser(principal, token))
        throw new IOException("Unable to authenticate user");

      if (getInputTableConfigs(implementingClass, conf).size() == 0)
        throw new IOException("No table set.");

      for (Map.Entry<String,InputTableConfig> tableConfig : inputTableConfigs.entrySet()) {
        if (!c.securityOperations().hasTablePermission(getPrincipal(implementingClass, conf), tableConfig.getKey(), TablePermission.READ))
          throw new IOException("Unable to access table");
      }
      for (Map.Entry<String,InputTableConfig> tableConfigEntry : inputTableConfigs.entrySet()) {
        InputTableConfig tableConfig = tableConfigEntry.getValue();
        if (!tableConfig.shouldUseLocalIterators()) {
          if (tableConfig.getIterators() != null) {
            for (IteratorSetting iter : tableConfig.getIterators()) {
              if (!c.tableOperations().testClassLoad(tableConfigEntry.getKey(), iter.getIteratorClass(), SortedKeyValueIterator.class.getName()))
                throw new AccumuloException("Servers are unable to load " + iter.getIteratorClass() + " as a " + SortedKeyValueIterator.class.getName());
            }
          }
        }
      }
    } catch (AccumuloException e) {
      throw new IOException(e);
    } catch (AccumuloSecurityException e) {
      throw new IOException(e);
    } catch (TableNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * Returns the {@link org.apache.accumulo.core.client.mapreduce.InputTableConfig} for the configuration based on the properties set using the single-table
   * input methods.
   *
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop instance for which to retrieve the configuration
   * @return the config object built from the single input table properties set on the job
   * @since 1.6.0
   */
  protected static Map.Entry<String,InputTableConfig> getDefaultInputTableConfig(Class<?> implementingClass, Configuration conf) {
    String tableName = getInputTableName(implementingClass, conf);
    if (tableName != null) {
      InputTableConfig queryConfig = new InputTableConfig();
      List<IteratorSetting> itrs = getIterators(implementingClass, conf);
      if (itrs != null)
        queryConfig.setIterators(itrs);
      Set<Pair<Text,Text>> columns = getFetchedColumns(implementingClass, conf);
      if (columns != null)
        queryConfig.fetchColumns(columns);
      List<Range> ranges = null;
      try {
        ranges = getRanges(implementingClass, conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (ranges != null)
        queryConfig.setRanges(ranges);

      queryConfig.setAutoAdjustRanges(getAutoAdjustRanges(implementingClass, conf)).setUseIsolatedScanners(isIsolated(implementingClass, conf))
          .setUseLocalIterators(usesLocalIterators(implementingClass, conf)).setOfflineScan(isOfflineScan(implementingClass, conf));
      return Maps.immutableEntry(tableName, queryConfig);
    }
    return null;
  }

  public static Map<String,Map<KeyExtent,List<Range>>> binOffline(String tableId, List<Range> ranges, Instance instance, Connector conn)
      throws AccumuloException, TableNotFoundException {
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();

    if (Tables.getTableState(instance, tableId) != TableState.OFFLINE) {
      Tables.clearCache(instance);
      if (Tables.getTableState(instance, tableId) != TableState.OFFLINE) {
        throw new AccumuloException("Table is online tableId:" + tableId + " cannot scan table in offline mode ");
      }
    }

    for (Range range : ranges) {
      Text startRow;

      if (range.getStartKey() != null)
        startRow = range.getStartKey().getRow();
      else
        startRow = new Text();

      Range metadataRange = new Range(new KeyExtent(new Text(tableId), startRow, null).getMetadataEntry(), true, null, false);
      Scanner scanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.LastLocationColumnFamily.NAME);
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME);
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

          if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.LastLocationColumnFamily.NAME)) {
            last = entry.getValue().toString();
          }

          if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME)
              || key.getColumnFamily().equals(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME)) {
            location = entry.getValue().toString();
          }

          if (MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)) {
            extent = new KeyExtent(key.getRow(), entry.getValue());
          }

        }

        if (location != null)
          return null;

        if (!extent.getTableId().toString().equals(tableId)) {
          throw new AccumuloException("Saw unexpected table Id " + tableId + " " + extent);
        }

        if (lastExtent != null && !extent.isPreviousExtent(lastExtent)) {
          throw new AccumuloException(" " + lastExtent + " is not previous extent " + extent);
        }

        Map<KeyExtent,List<Range>> tabletRanges = binnedRanges.get(last);
        if (tabletRanges == null) {
          tabletRanges = new HashMap<KeyExtent,List<Range>>();
          binnedRanges.put(last, tabletRanges);
        }

        List<Range> rangeList = tabletRanges.get(extent);
        if (rangeList == null) {
          rangeList = new ArrayList<Range>();
          tabletRanges.put(extent, rangeList);
        }

        rangeList.add(range);

        if (extent.getEndRow() == null || range.afterEndKey(new Key(extent.getEndRow()).followingKey(PartialKey.ROW))) {
          break;
        }

        lastExtent = extent;
      }

    }
    return binnedRanges;
  }
}
