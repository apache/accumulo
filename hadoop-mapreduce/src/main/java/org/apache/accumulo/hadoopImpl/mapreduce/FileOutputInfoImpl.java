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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.hadoop.mapreduce.FileOutputInfo;
import org.apache.hadoop.fs.Path;

public class FileOutputInfoImpl implements FileOutputInfo {
  Path outputPath;
  Optional<String> comp;
  Optional<Long> dataBlockSize;
  Optional<Long> fileBlockSize;
  Optional<Long> indexBlockSize;
  Optional<Integer> replication;
  Optional<SamplerConfiguration> sampler;
  Collection<SummarizerConfiguration> summarizers;

  public FileOutputInfoImpl(Path outputPath, Optional<String> comp, Optional<Long> dataBlockSize,
      Optional<Long> fileBlockSize, Optional<Long> indexBlockSize, Optional<Integer> replication,
      Optional<SamplerConfiguration> sampler, Collection<SummarizerConfiguration> summarizers) {
    this.outputPath = outputPath;
    this.comp = comp;
    this.dataBlockSize = dataBlockSize;
    this.fileBlockSize = fileBlockSize;
    this.indexBlockSize = indexBlockSize;
    this.replication = replication;
    this.sampler = sampler;
    this.summarizers = summarizers;
  }

  @Override
  public Path getOutputPath() {
    return outputPath;
  }

  @Override
  public Optional<String> getCompressionType() {
    return comp;
  }

  @Override
  public Optional<Long> getDataBlockSize() {
    return dataBlockSize;
  }

  @Override
  public Optional<Long> getFileBlockSize() {
    return fileBlockSize;
  }

  @Override
  public Optional<Long> getIndexBlockSize() {
    return indexBlockSize;
  }

  @Override
  public Optional<Integer> getReplication() {
    return replication;
  }

  @Override
  public Optional<SamplerConfiguration> getSampler() {
    return sampler;
  }

  @Override
  public Collection<SummarizerConfiguration> getSummarizers() {
    return summarizers;
  }

  public static class FileOutputInfoBuilderImpl implements FileOutputInfoBuilder,
      FileOutputInfoBuilder.PathParams, FileOutputInfoBuilder.OutputOptions {
    Path outputPath;
    Optional<String> comp = Optional.empty();
    Optional<Long> dataBlockSize = Optional.empty();
    Optional<Long> fileBlockSize = Optional.empty();
    Optional<Long> indexBlockSize = Optional.empty();
    Optional<Integer> replication = Optional.empty();
    Optional<SamplerConfiguration> sampler = Optional.empty();
    Collection<SummarizerConfiguration> summarizers = Collections.emptySet();

    @Override
    public OutputOptions outputPath(Path path) {
      this.outputPath = Objects.requireNonNull(path);
      ;
      return this;
    }

    @Override
    public OutputOptions compressionType(String compressionType) {
      this.comp = Optional.of(compressionType);
      return this;
    }

    @Override
    public OutputOptions dataBlockSize(long dataBlockSize) {
      this.dataBlockSize = Optional.of(dataBlockSize);
      return this;
    }

    @Override
    public OutputOptions fileBlockSize(long fileBlockSize) {
      this.fileBlockSize = Optional.of(fileBlockSize);
      return this;
    }

    @Override
    public OutputOptions indexBlockSize(long indexBlockSize) {
      this.indexBlockSize = Optional.of(indexBlockSize);
      return this;
    }

    @Override
    public OutputOptions replication(int replication) {
      this.replication = Optional.of(replication);
      return this;
    }

    @Override
    public OutputOptions sampler(SamplerConfiguration samplerConfig) {
      this.sampler = Optional.of(samplerConfig);
      return this;
    }

    @Override
    public OutputOptions summarizers(SummarizerConfiguration... summarizerConfigs) {
      this.summarizers = Arrays.asList(Objects.requireNonNull(summarizerConfigs));
      return this;
    }

    @Override
    public FileOutputInfo build() {
      return new FileOutputInfoImpl(outputPath, comp, dataBlockSize, fileBlockSize, indexBlockSize,
          replication, sampler, summarizers);
    }
  }
}
