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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.hadoop.mapreduce.FileOutputFormatBuilder;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.FileOutputConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class FileOutputFormatBuilderImpl<T> implements FileOutputFormatBuilder,
    FileOutputFormatBuilder.PathParams<T>, FileOutputFormatBuilder.OutputOptions<T> {

  Class<?> callingClass;
  Path outputPath;
  Optional<String> comp = Optional.empty();
  Optional<Long> dataBlockSize = Optional.empty();
  Optional<Long> fileBlockSize = Optional.empty();
  Optional<Long> indexBlockSize = Optional.empty();
  Optional<Integer> replication = Optional.empty();
  Optional<SamplerConfiguration> sampler = Optional.empty();
  Collection<SummarizerConfiguration> summarizers = Collections.emptySet();

  public FileOutputFormatBuilderImpl(Class<?> callingClass) {
    this.callingClass = callingClass;
  }

  @Override
  public OutputOptions<T> outputPath(Path path) {
    this.outputPath = Objects.requireNonNull(path);
    return this;
  }

  @Override
  public OutputOptions<T> compression(String compressionType) {
    this.comp = Optional.of(compressionType);
    return this;
  }

  @Override
  public OutputOptions<T> dataBlockSize(long dataBlockSize) {
    this.dataBlockSize = Optional.of(dataBlockSize);
    return this;
  }

  @Override
  public OutputOptions<T> fileBlockSize(long fileBlockSize) {
    this.fileBlockSize = Optional.of(fileBlockSize);
    return this;
  }

  @Override
  public OutputOptions<T> indexBlockSize(long indexBlockSize) {
    this.indexBlockSize = Optional.of(indexBlockSize);
    return this;
  }

  @Override
  public OutputOptions<T> replication(int replication) {
    this.replication = Optional.of(replication);
    return this;
  }

  @Override
  public OutputOptions<T> sampler(SamplerConfiguration samplerConfig) {
    this.sampler = Optional.of(samplerConfig);
    return this;
  }

  @Override
  public OutputOptions<T> summarizers(SummarizerConfiguration... summarizerConfigs) {
    this.summarizers = Arrays.asList(Objects.requireNonNull(summarizerConfigs));
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
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outputPath);
    _store(job.getConfiguration());
  }

  private void _store(Configuration conf) {
    if (comp.isPresent()) {
      FileOutputConfigurator.setCompressionType(callingClass, conf, comp.get());
    }
    if (dataBlockSize.isPresent()) {
      FileOutputConfigurator.setDataBlockSize(callingClass, conf, dataBlockSize.get());
    }
    if (fileBlockSize.isPresent()) {
      FileOutputConfigurator.setFileBlockSize(callingClass, conf, fileBlockSize.get());
    }
    if (indexBlockSize.isPresent()) {
      FileOutputConfigurator.setIndexBlockSize(callingClass, conf, indexBlockSize.get());
    }
    if (replication.isPresent()) {
      FileOutputConfigurator.setReplication(callingClass, conf, replication.get());
    }
    if (sampler.isPresent()) {
      FileOutputConfigurator.setSampler(callingClass, conf, sampler.get());
    }
    if (!summarizers.isEmpty()) {
      FileOutputConfigurator.setSummarizers(callingClass, conf,
          summarizers.toArray(new SummarizerConfiguration[0]));
    }
  }

  private void store(JobConf job) {
    org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(job, outputPath);
    _store(job);
  }

}
