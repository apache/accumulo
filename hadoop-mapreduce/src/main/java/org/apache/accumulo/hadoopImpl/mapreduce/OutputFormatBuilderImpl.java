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

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.accumulo.hadoop.mapreduce.OutputFormatBuilder;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.OutputConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class OutputFormatBuilderImpl<T>
    implements OutputFormatBuilder.ClientParams<T>, OutputFormatBuilder.OutputOptions<T> {
  private final Class<?> callingClass;
  private Properties clientProps;
  private String clientPropsPath;

  // optional values
  private Optional<String> defaultTableName = Optional.empty();
  private boolean createTables = false;
  private boolean simulationMode = false;

  public OutputFormatBuilderImpl(Class<?> callingClass) {
    this.callingClass = callingClass;
  }

  @Override
  public OutputFormatBuilder.OutputOptions<T> clientProperties(Properties clientProperties) {
    this.clientProps =
        Objects.requireNonNull(clientProperties, "clientProperties must not be null");
    return this;
  }

  @Override
  public OutputFormatBuilder.OutputOptions<T> clientPropertiesPath(String clientPropsPath) {
    this.clientPropsPath =
        Objects.requireNonNull(clientPropsPath, "clientPropsPath must not be null");
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
    _store(job.getConfiguration());
  }

  private void _store(Configuration conf) {
    OutputConfigurator.setClientProperties(callingClass, conf, clientProps, clientPropsPath);
    if (defaultTableName.isPresent()) {
      OutputConfigurator.setDefaultTableName(callingClass, conf, defaultTableName.get());
    }
    OutputConfigurator.setCreateTables(callingClass, conf, createTables);
    OutputConfigurator.setSimulationMode(callingClass, conf, simulationMode);
    OutputConfigurator.setJobStored(callingClass, conf);
  }

  private void store(JobConf jobConf) {
    _store(jobConf);
  }

}
