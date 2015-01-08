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
package org.apache.accumulo.core.cli;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.ClientOpts.MemoryConverter;
import org.apache.accumulo.core.cli.ClientOpts.TimeConverter;
import org.apache.accumulo.core.client.BatchWriterConfig;

import com.beust.jcommander.Parameter;

public class BatchWriterOpts {
  private static final BatchWriterConfig BWDEFAULTS = new BatchWriterConfig();

  @Parameter(names = "--batchThreads", description = "Number of threads to use when writing large batches")
  public Integer batchThreads = BWDEFAULTS.getMaxWriteThreads();

  @Parameter(names = "--batchLatency", converter = TimeConverter.class, description = "The maximum time to wait before flushing data to servers when writing")
  public Long batchLatency = BWDEFAULTS.getMaxLatency(TimeUnit.MILLISECONDS);

  @Parameter(names = "--batchMemory", converter = MemoryConverter.class, description = "memory used to batch data when writing")
  public Long batchMemory = BWDEFAULTS.getMaxMemory();

  @Parameter(names = "--batchTimeout", converter = TimeConverter.class, description = "timeout used to fail a batch write")
  public Long batchTimeout = BWDEFAULTS.getTimeout(TimeUnit.MILLISECONDS);

  public BatchWriterConfig getBatchWriterConfig() {
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxWriteThreads(this.batchThreads);
    config.setMaxLatency(this.batchLatency, TimeUnit.MILLISECONDS);
    config.setMaxMemory(this.batchMemory);
    config.setTimeout(this.batchTimeout, TimeUnit.MILLISECONDS);
    return config;
  }

}
