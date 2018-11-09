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

import static org.apache.accumulo.hadoopImpl.mapreduce.AccumuloOutputFormatImpl.setBatchWriterOptions;
import static org.apache.accumulo.hadoopImpl.mapreduce.AccumuloOutputFormatImpl.setClientInfo;
import static org.apache.accumulo.hadoopImpl.mapreduce.AccumuloOutputFormatImpl.setCreateTables;
import static org.apache.accumulo.hadoopImpl.mapreduce.AccumuloOutputFormatImpl.setDefaultTableName;
import static org.apache.accumulo.hadoopImpl.mapreduce.AccumuloOutputFormatImpl.setSimulationMode;

import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.hadoop.mapreduce.OutputFormatBuilder;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class OutputFormatBuilderImpl implements OutputFormatBuilder,
    OutputFormatBuilder.ClientParams, OutputFormatBuilder.OutputOptions {
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
  public void store(Job job) {
    setClientInfo(job, clientInfo);
    if (bwConfig.isPresent())
      setBatchWriterOptions(job, bwConfig.get());
    if (defaultTableName.isPresent())
      setDefaultTableName(job, defaultTableName.get());
    setCreateTables(job, createTables);
    setSimulationMode(job, simulationMode);
  }

  @Override
  public void store(JobConf jobConf) {
    org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setClientInfo(jobConf,
        clientInfo);
    if (bwConfig.isPresent())
      org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setBatchWriterOptions(jobConf,
          bwConfig.get());
    if (defaultTableName.isPresent())
      org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setDefaultTableName(jobConf,
          defaultTableName.get());
    org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setCreateTables(jobConf,
        createTables);
    org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setSimulationMode(jobConf,
        simulationMode);

  }

}
