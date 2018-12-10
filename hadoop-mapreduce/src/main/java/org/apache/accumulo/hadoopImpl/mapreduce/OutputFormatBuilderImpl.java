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

import static org.apache.accumulo.hadoopImpl.mapreduce.AccumuloOutputFormatImpl.setClientInfo;
import static org.apache.accumulo.hadoopImpl.mapreduce.AccumuloOutputFormatImpl.setCreateTables;
import static org.apache.accumulo.hadoopImpl.mapreduce.AccumuloOutputFormatImpl.setDefaultTableName;
import static org.apache.accumulo.hadoopImpl.mapreduce.AccumuloOutputFormatImpl.setSimulationMode;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.hadoop.mapreduce.OutputFormatBuilder;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class OutputFormatBuilderImpl<T>
    implements OutputFormatBuilder.ClientParams<T>, OutputFormatBuilder.OutputOptions<T> {
  ClientInfo clientInfo;

  // optional values
  Optional<String> defaultTableName = Optional.empty();
  boolean createTables = false;
  boolean simulationMode = false;

  @Override
  public OutputFormatBuilder.OutputOptions<T> clientProperties(Properties clientProperties) {
    this.clientInfo = ClientInfo
        .from(Objects.requireNonNull(clientProperties, "ClientInfo must not be null"));
    return this;
  }

  @Override
  public OutputFormatBuilder.OutputOptions<T> defaultTable(String tableName) {
    this.defaultTableName = Optional.of(tableName);
    return this;
  }

  @Override
  public OutputFormatBuilder.OutputOptions<T> createTables(boolean value) {
    this.createTables = value;
    return this;
  }

  @Override
  public OutputFormatBuilder.OutputOptions<T> simulationMode(boolean value) {
    this.simulationMode = value;
    return this;
  }

  @Override
  public void store(T j) {
    if (j instanceof Job) {
      store((Job) j);
    } else if (j instanceof JobConf) {
      store((JobConf) j);
    } else {
      throw new IllegalArgumentException("Unexpected type " + j.getClass().getName());
    }
  }

  private void store(Job job) {
    setClientInfo(job, clientInfo);
    if (defaultTableName.isPresent())
      setDefaultTableName(job, defaultTableName.get());
    setCreateTables(job, createTables);
    setSimulationMode(job, simulationMode);
  }

  private void store(JobConf jobConf) {
    org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setClientInfo(jobConf,
        clientInfo);
    if (defaultTableName.isPresent())
      org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setDefaultTableName(jobConf,
          defaultTableName.get());
    org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setCreateTables(jobConf,
        createTables);
    org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setSimulationMode(jobConf,
        simulationMode);

  }

}
