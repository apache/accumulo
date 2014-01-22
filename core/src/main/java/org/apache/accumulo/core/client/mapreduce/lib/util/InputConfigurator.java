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
package org.apache.accumulo.core.client.mapreduce.lib.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mock.MockTabletLocator;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

/**
 * @since 1.5.0
 */
public class InputConfigurator extends ConfiguratorBase {
  
  /**
   * Configuration keys for {@link Scanner}.
   * 
   * @since 1.5.0
   */
  public static enum ScanOpts {
    TABLE_NAME, AUTHORIZATIONS, RANGES, COLUMNS, ITERATORS
  }
  
  /**
   * Configuration keys for various features.
   * 
   * @since 1.5.0
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
   * @since 1.5.0
   */
  public static void setInputTableName(Class<?> implementingClass, Configuration conf, String tableName) {
    ArgumentChecker.notNull(tableName);
    conf.set(enumToConfKey(implementingClass, ScanOpts.TABLE_NAME), tableName);
  }
  
  /**
   * Gets the table name from the configuration.
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return the table name
   * @since 1.5.0
   * @see #setInputTableName(Class, Configuration, String)
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
   * @since 1.5.0
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
   * @since 1.5.0
   * @see #setScanAuthorizations(Class, Configuration, Authorizations)
   */
  public static Authorizations getScanAuthorizations(Class<?> implementingClass, Configuration conf) {
    String authString = conf.get(enumToConfKey(implementingClass, ScanOpts.AUTHORIZATIONS));
    return authString == null ? Constants.NO_AUTHS : new Authorizations(authString.getBytes());
  }
  
  /**
   * Sets the input ranges to scan for this job. If not set, the entire table will be scanned.
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param ranges
   *          the ranges that will be mapped over
   * @since 1.5.0
   */
  public static void setRanges(Class<?> implementingClass, Configuration conf, Collection<Range> ranges) {
    ArgumentChecker.notNull(ranges);
    ArrayList<String> rangeStrings = new ArrayList<String>(ranges.size());
    try {
      for (Range r : ranges) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        r.write(new DataOutputStream(baos));
        rangeStrings.add(new String(Base64.encodeBase64(baos.toByteArray()), Charset.forName("UTF-8")));
      }
    } catch (IOException ex) {
      throw new IllegalArgumentException("Unable to encode ranges to Base64", ex);
    }
    conf.setStrings(enumToConfKey(implementingClass, ScanOpts.RANGES), rangeStrings.toArray(new String[0]));
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
   * @since 1.5.0
   * @see #setRanges(Class, Configuration, Collection)
   */
  public static List<Range> getRanges(Class<?> implementingClass, Configuration conf) throws IOException {
    ArrayList<Range> ranges = new ArrayList<Range>();
    for (String rangeString : conf.getStringCollection(enumToConfKey(implementingClass, ScanOpts.RANGES))) {
      ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(rangeString.getBytes(Charset.forName("UTF-8"))));
      Range range = new Range();
      range.readFields(new DataInputStream(bais));
      ranges.add(range);
    }
    return ranges;
  }
  
  /**
   * Restricts the columns that will be mapped over for this job.
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param columnFamilyColumnQualifierPairs
   *          a pair of {@link Text} objects corresponding to column family and column qualifier. If the column qualifier is null, the entire column family is
   *          selected. An empty set is the default and is equivalent to scanning the all columns.
   * @since 1.5.0
   */
  public static void fetchColumns(Class<?> implementingClass, Configuration conf, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    ArgumentChecker.notNull(columnFamilyColumnQualifierPairs);
    String[] columnStrings = serializeColumns(columnFamilyColumnQualifierPairs);
    conf.setStrings(enumToConfKey(implementingClass, ScanOpts.COLUMNS), columnStrings);
  }

  public static String[] serializeColumns(Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
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
   * @since 1.5.0
   * @see #fetchColumns(Class, Configuration, Collection)
   */
  public static Set<Pair<Text,Text>> getFetchedColumns(Class<?> implementingClass, Configuration conf) {
    ArgumentChecker.notNull(conf);
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
      Text cf = new Text(idx < 0 ? Base64.decodeBase64(col.getBytes(Charset.forName("UTF-8"))) : Base64.decodeBase64(col.substring(0, idx).getBytes(Charset.forName("UTF-8"))));
      Text cq = idx < 0 ? null : new Text(Base64.decodeBase64(col.substring(idx + 1).getBytes()));
      columns.add(new Pair<Text,Text>(cf, cq));
    }
    return columns;
  }
  
  /**
   * Encode an iterator on the input for this job.
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @param cfg
   *          the configuration of the iterator
   * @since 1.5.0
   */
  public static void addIterator(Class<?> implementingClass, Configuration conf, IteratorSetting cfg) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String newIter;
    try {
      cfg.write(new DataOutputStream(baos));
      newIter = new String(Base64.encodeBase64(baos.toByteArray()), Charset.forName("UTF-8"));
      baos.close();
    } catch (IOException e) {
      throw new IllegalArgumentException("unable to serialize IteratorSetting");
    }
    
    String iterators = conf.get(enumToConfKey(implementingClass, ScanOpts.ITERATORS));
    // No iterators specified yet, create a new string
    if (iterators == null || iterators.isEmpty()) {
      iterators = newIter;
    } else {
      // append the next iterator & reset
      iterators = iterators.concat(StringUtils.COMMA_STR + newIter);
    }
    // Store the iterators w/ the job
    conf.set(enumToConfKey(implementingClass, ScanOpts.ITERATORS), iterators);
  }
  
  /**
   * Gets a list of the iterator settings (for iterators to apply to a scanner) from this configuration.
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return a list of iterators
   * @since 1.5.0
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
   * @since 1.5.0
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
   * @since 1.5.0
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
   * @since 1.5.0
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
   * @since 1.5.0
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
   * @since 1.5.0
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
   * @since 1.5.0
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
   * on the mapper's classpath. The accumulo-site.xml may need to be on the mapper's classpath if HDFS or the Accumulo directory in HDFS are non-standard.
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
   * @since 1.5.0
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
   * @since 1.5.0
   * @see #setOfflineTableScan(Class, Configuration, boolean)
   */
  public static Boolean isOfflineScan(Class<?> implementingClass, Configuration conf) {
    return conf.getBoolean(enumToConfKey(implementingClass, Features.SCAN_OFFLINE), false);
  }
  
  /**
   * Initializes an Accumulo {@link TabletLocator} based on the configuration.
   * 
   * @param implementingClass
   *          the class whose name will be used as a prefix for the property configuration key
   * @param conf
   *          the Hadoop configuration object to configure
   * @return an Accumulo tablet locator
   * @throws TableNotFoundException
   *           if the table name set on the configuration doesn't exist
   * @since 1.5.0
   */
  public static TabletLocator getTabletLocator(Class<?> implementingClass, Configuration conf) throws TableNotFoundException {
    String instanceType = conf.get(enumToConfKey(implementingClass, InstanceOpts.TYPE));
    if ("MockInstance".equals(instanceType))
      return new MockTabletLocator();
    Instance instance = getInstance(implementingClass, conf);
    String tableName = getInputTableName(implementingClass, conf);
    return TabletLocator.getInstance(instance, new Text(Tables.getTableId(instance, tableName)));
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
   * @since 1.5.0
   */
  public static void validateOptions(Class<?> implementingClass, Configuration conf) throws IOException {
    if (!isConnectorInfoSet(implementingClass, conf))
      throw new IOException("Input info has not been set.");
    String instanceKey = conf.get(enumToConfKey(implementingClass, InstanceOpts.TYPE));
    if (!"MockInstance".equals(instanceKey) && !"ZooKeeperInstance".equals(instanceKey))
      throw new IOException("Instance info has not been set.");
    // validate that we can connect as configured
    try {
      Connector c = getInstance(implementingClass, conf).getConnector(getPrincipal(implementingClass, conf),
          CredentialHelper.extractToken(getTokenClass(implementingClass, conf), getToken(implementingClass, conf)));
      if (!c.securityOperations().authenticateUser(getPrincipal(implementingClass, conf),
          CredentialHelper.extractToken(getTokenClass(implementingClass, conf), getToken(implementingClass, conf))))
        throw new IOException("Unable to authenticate user");
      if (!c.securityOperations().hasTablePermission(getPrincipal(implementingClass, conf), getInputTableName(implementingClass, conf), TablePermission.READ))
        throw new IOException("Unable to access table");
      
      if (!conf.getBoolean(enumToConfKey(implementingClass, Features.USE_LOCAL_ITERATORS), false)) {
        // validate that any scan-time iterators can be loaded by the the tablet servers
        for (IteratorSetting iter : getIterators(implementingClass, conf)) {
          if (!c.tableOperations().testClassLoad(getInputTableName(implementingClass, conf), iter.getIteratorClass(), SortedKeyValueIterator.class.getName()))
            throw new AccumuloException("Servers are unable to load " + iter.getIteratorClass() + " as a " + SortedKeyValueIterator.class.getName());
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
  
}
