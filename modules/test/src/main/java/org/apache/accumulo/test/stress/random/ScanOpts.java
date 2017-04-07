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
package org.apache.accumulo.test.stress.random;

import org.apache.accumulo.core.cli.ClientOnDefaultTable;

import com.beust.jcommander.Parameter;

class ScanOpts extends ClientOnDefaultTable {
  @Parameter(names = "--isolate", description = "true to turn on scan isolation, false to turn off. default is false.")
  boolean isolate = false;

  @Parameter(names = "--num-iterations", description = "number of scan iterations")
  int scan_iterations = 1024;

  @Parameter(names = "--continuous", description = "continuously scan the table. note that this overrides --num-iterations")
  boolean continuous;

  @Parameter(names = "--scan-seed", description = "seed for randomly choosing tablets to scan")
  int scan_seed = 1337;

  @Parameter(names = "--scan-batch-size", description = "scanner batch size")
  int batch_size = -1;

  public ScanOpts() {
    this(WriteOptions.DEFAULT_TABLE);
  }

  public ScanOpts(String table) {
    super(table);
  }
}
