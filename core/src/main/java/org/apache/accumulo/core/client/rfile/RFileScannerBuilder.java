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
package org.apache.accumulo.core.client.rfile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile.ScannerFSOptions;
import org.apache.accumulo.core.client.rfile.RFile.ScannerOptions;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

class RFileScannerBuilder implements RFile.InputArguments, RFile.ScannerFSOptions {

  static class InputArgs extends FSConfArgs {
    private Path[] paths;
    private RFileSource[] sources;

    InputArgs(String... files) {
      this.paths = new Path[files.length];
      for (int i = 0; i < files.length; i++) {
        this.paths[i] = new Path(files[i]);
      }
    }

    InputArgs(RFileSource... sources) {
      this.sources = sources;
    }

    RFileSource[] getSources() throws IOException {
      if (sources == null) {
        sources = new RFileSource[paths.length];
        for (int i = 0; i < paths.length; i++) {
          sources[i] = new RFileSource(getFileSystem().open(paths[i]),
              getFileSystem().getFileStatus(paths[i]).getLen());
        }
      } else {
        for (int i = 0; i < sources.length; i++) {
          if (!(sources[i].getInputStream() instanceof FSDataInputStream)) {
            sources[i] = new RFileSource(new FSDataInputStream(sources[i].getInputStream()),
                sources[i].getLength());
          }
        }
      }

      return sources;
    }
  }

  private RFileScanner.Opts opts = new RFileScanner.Opts();

  @Override
  public ScannerOptions withoutSystemIterators() {
    opts.useSystemIterators = false;
    return this;
  }

  @Override
  public ScannerOptions withAuthorizations(Authorizations auths) {
    Objects.requireNonNull(auths);
    opts.auths = auths;
    return this;
  }

  @Override
  public ScannerOptions withDataCache(long cacheSize) {
    Preconditions.checkArgument(cacheSize > 0);
    opts.dataCacheSize = cacheSize;
    return this;
  }

  @Override
  public ScannerOptions withIndexCache(long cacheSize) {
    Preconditions.checkArgument(cacheSize > 0);
    opts.indexCacheSize = cacheSize;
    return this;
  }

  @Override
  public Scanner build() {
    return new RFileScanner(opts);
  }

  @Override
  public ScannerOptions withFileSystem(FileSystem fs) {
    Objects.requireNonNull(fs);
    opts.in.fs = fs;
    return this;
  }

  @Override
  public ScannerOptions from(RFileSource... inputs) {
    Objects.requireNonNull(inputs);
    opts.in = new InputArgs(inputs);
    return this;
  }

  @Override
  public ScannerFSOptions from(String... files) {
    Objects.requireNonNull(files);
    opts.in = new InputArgs(files);
    return this;
  }

  @Override
  public ScannerOptions withTableProperties(Iterable<Entry<String,String>> tableConfig) {
    Objects.requireNonNull(tableConfig);
    this.opts.tableConfig = new HashMap<>();
    for (Entry<String,String> entry : tableConfig) {
      this.opts.tableConfig.put(entry.getKey(), entry.getValue());
    }
    return this;
  }

  @Override
  public ScannerOptions withTableProperties(Map<String,String> tableConfig) {
    Objects.requireNonNull(tableConfig);
    this.opts.tableConfig = new HashMap<>(tableConfig);
    return this;
  }

  @Override
  public ScannerOptions withBounds(Range range) {
    Objects.requireNonNull(range);
    this.opts.bounds = range;
    return this;
  }
}
