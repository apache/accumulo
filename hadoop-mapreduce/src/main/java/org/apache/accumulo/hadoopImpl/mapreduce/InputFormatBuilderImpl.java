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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.InputConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class InputFormatBuilderImpl<T>
    implements InputFormatBuilder, InputFormatBuilder.ClientParams<T>,
    InputFormatBuilder.TableParams<T>, InputFormatBuilder.InputFormatOptions<T> {

  private Class<?> callingClass;
  private Properties clientProps;
  private String clientPropsPath;
  private String currentTable;
  private Map<String,InputTableConfig> tableConfigMap = Collections.emptyMap();

  public InputFormatBuilderImpl(Class<?> callingClass) {
    this.callingClass = callingClass;
  }

  @Override
  public InputFormatBuilder.TableParams<T> clientProperties(Properties clientProperties) {
    this.clientProps =
        Objects.requireNonNull(clientProperties, "clientProperties must not be null");
    return this;
  }

  @Override
  public TableParams<T> clientPropertiesPath(String clientPropsPath) {
    this.clientPropsPath =
        Objects.requireNonNull(clientPropsPath, "clientPropsPath must not be null");
    return this;
  }

  @Override
  public InputFormatBuilder.InputFormatOptions<T> table(String tableName) {
    this.currentTable = Objects.requireNonNull(tableName, "Table name must not be null");
    if (tableConfigMap.isEmpty()) {
      tableConfigMap = new LinkedHashMap<>();
    }
    tableConfigMap.put(currentTable, new InputTableConfig());
    return this;
  }

  @Override
  public InputFormatBuilder.InputFormatOptions<T> auths(Authorizations auths) {
    tableConfigMap.get(currentTable)
        .setScanAuths(Objects.requireNonNull(auths, "Authorizations must not be null"));
    return this;
  }

  @Override
  public InputFormatBuilder.InputFormatOptions<T> classLoaderContext(String context) {
    tableConfigMap.get(currentTable).setContext(context);
    return this;
  }

  @Override
  public InputFormatBuilder.InputFormatOptions<T> ranges(Collection<Range> ranges) {
    List<Range> newRanges =
        List.copyOf(Objects.requireNonNull(ranges, "Collection of ranges is null"));
    if (newRanges.isEmpty()) {
      throw new IllegalArgumentException("Specified collection of ranges is empty.");
    }
    tableConfigMap.get(currentTable).setRanges(newRanges);
    return this;
  }

  @Override
  public InputFormatBuilder.InputFormatOptions<T>
      fetchColumns(Collection<IteratorSetting.Column> fetchColumns) {
    Collection<IteratorSetting.Column> newFetchColumns =
        List.copyOf(Objects.requireNonNull(fetchColumns, "Collection of fetch columns is null"));
    if (newFetchColumns.isEmpty()) {
      throw new IllegalArgumentException("Specified collection of fetch columns is empty.");
    }
    tableConfigMap.get(currentTable).fetchColumns(newFetchColumns);
    return this;
  }

  @Override
  public InputFormatBuilder.InputFormatOptions<T> addIterator(IteratorSetting cfg) {
    // store iterators by name to prevent duplicates
    Objects.requireNonNull(cfg, "IteratorSetting must not be null.");
    tableConfigMap.get(currentTable).addIterator(cfg);
    return this;
  }

  @Override
  public InputFormatBuilder.InputFormatOptions<T> executionHints(Map<String,String> hints) {
    Map<String,String> newHints =
        Map.copyOf(Objects.requireNonNull(hints, "Map of execution hints must not be null."));
    if (newHints.isEmpty()) {
      throw new IllegalArgumentException("Specified map of execution hints is empty.");
    }
    tableConfigMap.get(currentTable).setExecutionHints(newHints);
    return this;
  }

  @Override
  public InputFormatBuilder.InputFormatOptions<T>
      samplerConfiguration(SamplerConfiguration samplerConfig) {
    tableConfigMap.get(currentTable).setSamplerConfiguration(samplerConfig);
    return this;
  }

  @Override
  public InputFormatOptions<T> autoAdjustRanges(boolean value) {
    tableConfigMap.get(currentTable).setAutoAdjustRanges(value);
    return this;
  }

  @Override
  public InputFormatOptions<T> scanIsolation(boolean value) {
    tableConfigMap.get(currentTable).setUseIsolatedScanners(value);
    return this;
  }

  @Override
  public InputFormatOptions<T> localIterators(boolean value) {
    tableConfigMap.get(currentTable).setUseLocalIterators(value);
    return this;
  }

  @Override
  public InputFormatOptions<T> offlineScan(boolean value) {
    tableConfigMap.get(currentTable).setOfflineScan(value);
    return this;
  }

  @Override
  public InputFormatOptions<T> batchScan(boolean value) {
    tableConfigMap.get(currentTable).setUseBatchScan(value);
    if (value) {
      tableConfigMap.get(currentTable).setAutoAdjustRanges(true);
    }
    return this;
  }

  @Override
  public InputFormatOptions<T> consistencyLevel(ConsistencyLevel level) {
    tableConfigMap.get(currentTable).setConsistencyLevel(level);
    return this;
  }

  @Override
  public void store(T j) throws AccumuloException, AccumuloSecurityException {
    if (j instanceof Job) {
      store((Job) j);
    } else if (j instanceof JobConf) {
      store((JobConf) j);
    } else {
      throw new IllegalArgumentException("Unexpected type " + j.getClass().getName());
    }
  }

  /**
   * Final builder method for mapreduce configuration
   */
  private void store(Job job) throws AccumuloException, AccumuloSecurityException {
    _store(job.getConfiguration());
  }

  private void _store(Configuration conf) throws AccumuloException, AccumuloSecurityException {
    InputConfigurator.setClientProperties(callingClass, conf, clientProps, clientPropsPath);
    if (tableConfigMap.isEmpty()) {
      throw new IllegalArgumentException("At least one Table must be configured for job.");
    }
    // if only one table use the single table configuration method
    if (tableConfigMap.size() == 1) {
      Map.Entry<String,InputTableConfig> entry = tableConfigMap.entrySet().iterator().next();
      InputConfigurator.setInputTableName(callingClass, conf, entry.getKey());
      InputTableConfig config = entry.getValue();
      if (!config.getScanAuths().isPresent()) {
        Properties props = InputConfigurator.getClientProperties(callingClass, conf);
        try (AccumuloClient c = Accumulo.newClient().from(props).build()) {
          String principal = ClientProperty.AUTH_PRINCIPAL.getValue(props);
          config.setScanAuths(c.securityOperations().getUserAuthorizations(principal));
        }
      }
      InputConfigurator.setScanAuthorizations(callingClass, conf, config.getScanAuths().get());
      // all optional values
      if (config.getContext().isPresent()) {
        InputConfigurator.setClassLoaderContext(callingClass, conf, config.getContext().get());
      }
      if (!config.getRanges().isEmpty()) {
        InputConfigurator.setRanges(callingClass, conf, config.getRanges());
      }
      if (!config.getIterators().isEmpty()) {
        InputConfigurator.writeIteratorsToConf(callingClass, conf, config.getIterators());
      }
      if (!config.getFetchedColumns().isEmpty()) {
        InputConfigurator.fetchColumns(callingClass, conf, config.getFetchedColumns());
      }
      if (config.getSamplerConfiguration() != null) {
        InputConfigurator.setSamplerConfiguration(callingClass, conf,
            config.getSamplerConfiguration());
      }
      if (!config.getExecutionHints().isEmpty()) {
        InputConfigurator.setExecutionHints(callingClass, conf, config.getExecutionHints());
      }
      InputConfigurator.setAutoAdjustRanges(callingClass, conf, config.shouldAutoAdjustRanges());
      InputConfigurator.setScanIsolation(callingClass, conf, config.shouldUseIsolatedScanners());
      InputConfigurator.setLocalIterators(callingClass, conf, config.shouldUseLocalIterators());
      InputConfigurator.setOfflineTableScan(callingClass, conf, config.isOfflineScan());
      InputConfigurator.setBatchScan(callingClass, conf, config.shouldBatchScan());
      if (config.getConsistencyLevel() != null) {
        InputConfigurator.setConsistencyLevel(callingClass, conf, config.getConsistencyLevel());
      }
    } else {
      InputConfigurator.setInputTableConfigs(callingClass, conf, tableConfigMap);
    }
    InputConfigurator.setJobStored(callingClass, conf);
  }

  /**
   * Final builder method for legacy mapred configuration
   */
  private void store(JobConf jobConf) throws AccumuloException, AccumuloSecurityException {
    _store(jobConf);
  }

}
