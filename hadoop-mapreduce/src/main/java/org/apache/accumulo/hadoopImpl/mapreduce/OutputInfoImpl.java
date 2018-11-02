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

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.hadoop.mapreduce.OutputInfo;

public class OutputInfoImpl implements OutputInfo {
  ClientInfo clientInfo;

  // optional values
  Optional<String> defaultTableName;
  Optional<BatchWriterConfig> bwConfig;
  boolean createTables;
  boolean simulationMode;

  public OutputInfoImpl(ClientInfo ci, Optional<String> defaultTableName,
      Optional<BatchWriterConfig> bwConfig, boolean createTables, boolean simulationMode) {
    this.clientInfo = ci;
    this.defaultTableName = defaultTableName;
    this.bwConfig = bwConfig;
    this.createTables = createTables;
    this.simulationMode = simulationMode;
  }

  @Override
  public ClientInfo getClientInfo() {
    return clientInfo;
  }

  @Override
  public Properties getClientProperties() {
    return clientInfo.getProperties();
  }

  @Override
  public Optional<BatchWriterConfig> getBatchWriterOptions() {
    return bwConfig;
  }

  @Override
  public Optional<String> getDefaultTableName() {
    return defaultTableName;
  }

  @Override
  public boolean isCreateTables() {
    return createTables;
  }

  @Override
  public boolean isSimulationMode() {
    return simulationMode;
  }

  public static class OutputInfoBuilderImpl implements OutputInfoBuilder,
      OutputInfoBuilder.ClientParams, OutputInfoBuilder.OutputOptions {
    ClientInfo clientInfo;

    // optional values
    Optional<String> defaultTableName = Optional.empty();
    Optional<BatchWriterConfig> bwConfig = Optional.empty();
    boolean createTables = false;
    boolean simulationMode = false;

    @Override
    public OutputOptions clientInfo(ClientInfo clientInfo) {
      this.clientInfo = Objects.requireNonNull(clientInfo, "ClientInfo must not be null");
      return this;
    }

    @Override
    public OutputOptions batchWriterOptions(BatchWriterConfig bwConfig) {
      this.bwConfig = Optional.of(bwConfig);
      return this;
    }

    @Override
    public OutputOptions defaultTableName(String tableName) {
      this.defaultTableName = Optional.of(tableName);
      return this;
    }

    @Override
    public OutputOptions enableCreateTables() {
      this.createTables = true;
      return this;
    }

    @Override
    public OutputOptions enableSimulationMode() {
      this.simulationMode = true;
      return this;
    }

    @Override
    public OutputInfo build() {
      return new OutputInfoImpl(clientInfo, defaultTableName, bwConfig, createTables,
          simulationMode);
    }
  }
}
