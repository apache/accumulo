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
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.InputConfigurator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class InputFormatBuilderImpl<T>
    implements InputFormatBuilder, InputFormatBuilder.ClientParams<T>,
    InputFormatBuilder.TableParams<T>, InputFormatBuilder.InputFormatOptions<T> {

  Class<?> callingClass;
  ClientInfo clientInfo;

  String currentTable;
  Map<String,InputTableConfig> tableConfigMap = Collections.emptyMap();

  public InputFormatBuilderImpl(Class<?> callingClass) {
    this.callingClass = callingClass;
  }

  @Override
  public InputFormatBuilder.TableParams<T> clientProperties(Properties clientProperties) {
    this.clientInfo = ClientInfo
        .from(Objects.requireNonNull(clientProperties, "clientProperties must not be null"));
    return this;
  }

  @Override
  public InputFormatBuilder.InputFormatOptions<T> table(String tableName) {
    this.currentTable = Objects.requireNonNull(tableName, "Table name must not be null");
    if (tableConfigMap.isEmpty())
      tableConfigMap = new LinkedHashMap<>();
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
    List<Range> newRanges = ImmutableList
        .copyOf(Objects.requireNonNull(ranges, "Collection of ranges is null"));
    if (newRanges.size() == 0)
      throw new IllegalArgumentException("Specified collection of ranges is empty.");
    tableConfigMap.get(currentTable).setRanges(newRanges);
    return this;
  }

  @Override
  public InputFormatBuilder.InputFormatOptions<T> fetchColumns(
      Collection<IteratorSetting.Column> fetchColumns) {
    Collection<IteratorSetting.Column> newFetchColumns = ImmutableList
        .copyOf(Objects.requireNonNull(fetchColumns, "Collection of fetch columns is null"));
    if (newFetchColumns.size() == 0)
      throw new IllegalArgumentException("Specified collection of fetch columns is empty.");
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
    Map<String,String> newHints = ImmutableMap
        .copyOf(Objects.requireNonNull(hints, "Map of execution hints must not be null."));
    if (newHints.size() == 0)
      throw new IllegalArgumentException("Specified map of execution hints is empty.");
    tableConfigMap.get(currentTable).setExecutionHints(newHints);
    return this;
  }

  @Override
  public InputFormatBuilder.InputFormatOptions<T> samplerConfiguration(
      SamplerConfiguration samplerConfig) {
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
    if (value)
      tableConfigMap.get(currentTable).setAutoAdjustRanges(true);
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
    AbstractInputFormat.setClientInfo(job, clientInfo);
    if (tableConfigMap.size() == 0) {
      throw new IllegalArgumentException("At least one Table must be configured for job.");
    }
    // if only one table use the single table configuration method
    if (tableConfigMap.size() == 1) {
      Map.Entry<String,InputTableConfig> entry = tableConfigMap.entrySet().iterator().next();
      InputFormatBase.setInputTableName(job, entry.getKey());
      InputTableConfig config = entry.getValue();
      if (!config.getScanAuths().isPresent())
        config.setScanAuths(getUserAuths(clientInfo));
      AbstractInputFormat.setScanAuthorizations(job, config.getScanAuths().get());
      // all optional values
      if (config.getContext().isPresent())
        AbstractInputFormat.setClassLoaderContext(job, config.getContext().get());
      if (config.getRanges().size() > 0)
        InputFormatBase.setRanges(job, config.getRanges());
      if (config.getIterators().size() > 0)
        InputConfigurator.writeIteratorsToConf(callingClass, job.getConfiguration(),
            config.getIterators());
      if (config.getFetchedColumns().size() > 0)
        InputConfigurator.fetchColumns(callingClass, job.getConfiguration(),
            config.getFetchedColumns());
      if (config.getSamplerConfiguration() != null)
        InputFormatBase.setSamplerConfiguration(job, config.getSamplerConfiguration());
      if (config.getExecutionHints().size() > 0)
        InputFormatBase.setExecutionHints(job, config.getExecutionHints());
      InputFormatBase.setAutoAdjustRanges(job, config.shouldAutoAdjustRanges());
      InputFormatBase.setScanIsolation(job, config.shouldUseIsolatedScanners());
      InputFormatBase.setLocalIterators(job, config.shouldUseLocalIterators());
      InputFormatBase.setOfflineTableScan(job, config.isOfflineScan());
      InputFormatBase.setBatchScan(job, config.shouldBatchScan());
    } else {
      InputConfigurator.setInputTableConfigs(callingClass, job.getConfiguration(), tableConfigMap);
    }
  }

  /**
   * Final builder method for legacy mapred configuration
   */
  private void store(JobConf jobConf) throws AccumuloException, AccumuloSecurityException {
    org.apache.accumulo.hadoopImpl.mapred.AbstractInputFormat.setClientInfo(jobConf, clientInfo);
    if (tableConfigMap.size() == 0) {
      throw new IllegalArgumentException("At least one Table must be configured for job.");
    }
    // if only one table use the single table configuration method
    if (tableConfigMap.size() == 1) {
      Map.Entry<String,InputTableConfig> entry = tableConfigMap.entrySet().iterator().next();
      org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setInputTableName(jobConf,
          entry.getKey());
      InputTableConfig config = entry.getValue();
      if (!config.getScanAuths().isPresent())
        config.setScanAuths(getUserAuths(clientInfo));
      org.apache.accumulo.hadoopImpl.mapred.AbstractInputFormat.setScanAuthorizations(jobConf,
          config.getScanAuths().get());
      // all optional values
      if (config.getContext().isPresent())
        org.apache.accumulo.hadoopImpl.mapred.AbstractInputFormat.setClassLoaderContext(jobConf,
            config.getContext().get());
      if (config.getRanges().size() > 0)
        org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setRanges(jobConf,
            config.getRanges());
      if (config.getIterators().size() > 0)
        InputConfigurator.writeIteratorsToConf(callingClass, jobConf, config.getIterators());
      if (config.getFetchedColumns().size() > 0)
        InputConfigurator.fetchColumns(callingClass, jobConf, config.getFetchedColumns());
      if (config.getSamplerConfiguration() != null)
        org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setSamplerConfiguration(jobConf,
            config.getSamplerConfiguration());
      if (config.getExecutionHints().size() > 0)
        org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setExecutionHints(jobConf,
            config.getExecutionHints());
      org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setAutoAdjustRanges(jobConf,
          config.shouldAutoAdjustRanges());
      org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setScanIsolation(jobConf,
          config.shouldUseIsolatedScanners());
      org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setLocalIterators(jobConf,
          config.shouldUseLocalIterators());
      org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setOfflineTableScan(jobConf,
          config.isOfflineScan());
      org.apache.accumulo.hadoopImpl.mapred.InputFormatBase.setBatchScan(jobConf,
          config.shouldBatchScan());
    } else {
      InputConfigurator.setInputTableConfigs(callingClass, jobConf, tableConfigMap);
    }
  }

  private Authorizations getUserAuths(ClientInfo clientInfo)
      throws AccumuloSecurityException, AccumuloException {
    try (AccumuloClient c = Accumulo.newClient().from(clientInfo.getProperties()).build()) {
      return c.securityOperations().getUserAuthorizations(clientInfo.getPrincipal());
    }
  }

}
