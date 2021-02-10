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

package org.apache.accumulo.core.client.rfile;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.accumulo.core.client.rfile.RFile.WriterFSOptions;
import org.apache.accumulo.core.client.rfile.RFile.WriterOptions;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

class RFileWriterBuilder implements RFile.OutputArguments, RFile.WriterFSOptions {

  private static class OutputArgs extends FSConfArgs {
    private Path path;
    private OutputStream out;

    OutputArgs(String filename) {
      this.path = new Path(filename);
    }

    OutputArgs(OutputStream out) {
      this.out = out;
    }

    OutputStream getOutputStream() {
      return out;
    }
  }

  private OutputArgs out;
  private SamplerConfiguration sampler = null;
  private Map<String,String> tableConfig = Collections.emptyMap();
  private int visCacheSize = 1000;

  @Override
  public WriterOptions withSampler(SamplerConfiguration samplerConf) {
    Objects.requireNonNull(samplerConf);
    SamplerConfigurationImpl.checkDisjoint(tableConfig, samplerConf);
    this.sampler = samplerConf;
    return this;
  }

  @Override
  public RFileWriter build() throws IOException {
    FileOperations fileops = FileOperations.getInstance();
    AccumuloConfiguration acuconf = AccumuloConfiguration.getDefaultConfiguration();
    HashMap<String,String> userProps = new HashMap<>();
    if (sampler != null) {
      userProps.putAll(new SamplerConfigurationImpl(sampler).toTablePropertiesMap());
    }
    userProps.putAll(tableConfig);

    if (userProps.size() > 0) {
      acuconf = new ConfigurationCopy(Iterables.concat(acuconf, userProps.entrySet()));
    }

    if (out.getOutputStream() != null) {
      FSDataOutputStream fsdo;
      if (out.getOutputStream() instanceof FSDataOutputStream) {
        fsdo = (FSDataOutputStream) out.getOutputStream();
      } else {
        fsdo = new FSDataOutputStream(out.getOutputStream(), new FileSystem.Statistics("foo"));
      }
      return new RFileWriter(fileops.newWriterBuilder().forOutputStream(".rf", fsdo, out.getConf())
          .withTableConfiguration(acuconf).build(), visCacheSize);
    } else {
      return new RFileWriter(fileops.newWriterBuilder()
          .forFile(out.path.toString(), out.getFileSystem(), out.getConf())
          .withTableConfiguration(acuconf).build(), visCacheSize);
    }
  }

  @Override
  public WriterOptions withFileSystem(FileSystem fs) {
    Objects.requireNonNull(fs);
    out.fs = fs;
    return this;
  }

  @Override
  public WriterFSOptions to(String filename) {
    Objects.requireNonNull(filename);
    if (!filename.endsWith(".rf")) {
      throw new IllegalArgumentException(
          "Provided filename (" + filename + ") does not end with '.rf'");
    }
    this.out = new OutputArgs(filename);
    return this;
  }

  @Override
  public WriterOptions to(OutputStream out) {
    Objects.requireNonNull(out);
    this.out = new OutputArgs(out);
    return this;
  }

  @Override
  public WriterOptions withTableProperties(Iterable<Entry<String,String>> tableConfig) {
    Objects.requireNonNull(tableConfig);
    HashMap<String,String> cfg = new HashMap<>();
    for (Entry<String,String> entry : tableConfig) {
      cfg.put(entry.getKey(), entry.getValue());
    }

    SamplerConfigurationImpl.checkDisjoint(cfg, sampler);
    this.tableConfig = cfg;
    return this;
  }

  @Override
  public WriterOptions withTableProperties(Map<String,String> tableConfig) {
    Objects.requireNonNull(tableConfig);
    return withTableProperties(tableConfig.entrySet());
  }

  @Override
  public WriterOptions withVisibilityCacheSize(int maxSize) {
    Preconditions.checkArgument(maxSize > 0);
    this.visCacheSize = maxSize;
    return this;
  }
}
