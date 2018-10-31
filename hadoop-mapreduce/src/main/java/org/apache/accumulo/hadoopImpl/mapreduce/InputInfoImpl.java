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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.hadoop.mapreduce.InputInfo;

public class InputInfoImpl implements InputInfo {
  String tableName;
  ClientInfo clientInfo;
  Authorizations scanAuths;

  // optional values
  Optional<String> context;
  Collection<Range> ranges;
  Collection<Pair<byte[],byte[]>> cfcqPairs;
  Map<String,IteratorSetting> iterators;
  Optional<SamplerConfiguration> samplerConfig;
  Map<String,String> hints;
  InputInfoBooleans bools;

  public InputInfoImpl(String tableName, ClientInfo clientInfo, Authorizations scanAuths,
      Optional<String> context, Collection<Range> ranges, Collection<Pair<byte[],byte[]>> cfcqPairs,
      Map<String,IteratorSetting> iterators, Optional<SamplerConfiguration> samplerConfig,
      Map<String,String> hints, InputInfoBooleans bools) {
    this.tableName = tableName;
    this.clientInfo = clientInfo;
    this.scanAuths = scanAuths;
    this.context = context;
    this.ranges = ranges;
    this.cfcqPairs = cfcqPairs;
    this.iterators = iterators;
    this.samplerConfig = samplerConfig;
    this.hints = hints;
    this.bools = bools;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public ClientInfo getClientInfo() {
    return clientInfo;
  }

  @Override
  public Properties getClientProperties() {
    return clientInfo.getProperties();
  }

  public Authorizations getScanAuths() {
    return scanAuths;
  }

  @Override
  public Optional<String> getContext() {
    return context;
  }

  @Override
  public Collection<Range> getRanges() {
    return ranges;
  }

  @Override
  public Collection<Pair<byte[],byte[]>> getFetchColumns() {
    return cfcqPairs;
  }

  @Override
  public Collection<IteratorSetting> getIterators() {
    return iterators.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toSet());
  }

  @Override
  public Optional<SamplerConfiguration> getSamplerConfig() {
    return samplerConfig;
  }

  @Override
  public Map<String,String> getExecutionHints() {
    return hints;
  }

  @Override
  public boolean isAutoAdjustRanges() {
    return bools.autoAdjustRanges;
  }

  @Override
  public boolean isScanIsolation() {
    return bools.scanIsolation;
  }

  @Override
  public boolean isLocalIterators() {
    return bools.localIters;
  }

  @Override
  public boolean isOfflineScan() {
    return bools.offlineScan;
  }

  @Override
  public boolean isBatchScan() {
    return bools.batchScan;
  }

  private static class InputInfoBooleans {
    boolean autoAdjustRanges = true;
    boolean scanIsolation = false;
    boolean offlineScan = false;
    boolean localIters = false;
    boolean batchScan = false;
  }

  public static class InputInfoBuilderImpl
      implements InputInfoBuilder, InputInfoBuilder.ClientParams, InputInfoBuilder.TableParams,
      InputInfoBuilder.AuthsParams, InputInfoBuilder.InputFormatOptions,
      InputInfoBuilder.NonBatchScanOptions, InputInfoBuilder.BatchScanOptions {

    String tableName;
    ClientInfo clientInfo;
    Authorizations scanAuths;

    Optional<String> context = Optional.empty();
    Collection<Range> ranges = new ArrayList<>();
    Collection<Pair<byte[],byte[]>> cfcqPairs = new ArrayList<>();
    Map<String,IteratorSetting> iterators = new LinkedHashMap<>();
    Optional<SamplerConfiguration> samplerConfig = Optional.empty();
    Map<String,String> hints = new HashMap<>();
    InputInfoBooleans bools = new InputInfoBooleans();

    @Override
    public InputInfoBuilder.TableParams clientInfo(ClientInfo clientInfo) {
      Objects.requireNonNull(clientInfo, "ClientInfo must not be null");
      this.clientInfo = clientInfo;
      return this;
    }

    @Override
    public InputInfoBuilder.TableParams clientProperties(Properties clientProps) {
      Objects.requireNonNull(clientProps, "Properties must not be null");
      this.clientInfo = ClientInfo.from(clientProps);
      return this;
    }

    @Override
    public InputInfoBuilder.AuthsParams table(String tableName) {
      Objects.requireNonNull(tableName, "Table name must not be null");
      this.tableName = tableName;
      return this;
    }

    @Override
    public InputInfoBuilder.InputFormatOptions scanAuths(Authorizations auths) {
      Objects.requireNonNull(auths, "Authorizations must not be null");
      this.scanAuths = auths;
      return this;
    }

    @Override
    public InputInfoBuilder.InputFormatOptions classLoaderContext(String context) {
      this.context = Optional.of(context);
      return this;
    }

    @Override
    public InputInfoBuilder.InputFormatOptions ranges(Collection<Range> ranges) {
      this.ranges = ranges;
      return this;
    }

    @Override
    public InputInfoBuilder.InputFormatOptions fetchColumns(
        Collection<Pair<byte[],byte[]>> cfcqPairs) {
      this.cfcqPairs = cfcqPairs;
      return this;
    }

    @Override
    public InputInfoBuilder.InputFormatOptions addIterator(IteratorSetting cfg) {
      // store iterators by name to prevent duplicates
      this.iterators.put(cfg.getName(), cfg);
      return this;
    }

    @Override
    public InputInfoBuilder.InputFormatOptions setExecutionHints(Map<String,String> hints) {
      this.hints = hints;
      return this;
    }

    @Override
    public InputInfoBuilder.InputFormatOptions setSamplerConfiguration(
        SamplerConfiguration samplerConfig) {
      this.samplerConfig = Optional.of(samplerConfig);
      return this;
    }

    @Override
    public InputFormatOptions disableAutoAdjustRanges() {
      bools.autoAdjustRanges = false;
      return this;
    }

    @Override
    public NonBatchScanOptions scanIsolation() {
      bools.scanIsolation = true;
      return this;
    }

    @Override
    public NonBatchScanOptions localIterators() {
      bools.localIters = true;
      return this;
    }

    @Override
    public NonBatchScanOptions offlineScan() {
      bools.offlineScan = true;
      return this;
    }

    @Override
    public BatchScanOptions batchScan() {
      bools.batchScan = true;
      bools.autoAdjustRanges = true;
      return this;
    }

    @Override
    public InputInfo build() {
      return new InputInfoImpl(tableName, clientInfo, scanAuths, context, ranges, cfcqPairs,
          iterators, samplerConfig, hints, bools);
    }
  }
}
