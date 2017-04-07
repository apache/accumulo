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

import org.apache.accumulo.core.cli.ClientOpts.TimeConverter;

import com.beust.jcommander.Parameter;

public class BatchScannerOpts {
  @Parameter(names = "--scanThreads", description = "Number of threads to use when batch scanning")
  public Integer scanThreads = 10;

  @Parameter(names = "--scanTimeout", converter = TimeConverter.class, description = "timeout used to fail a batch scan")
  public Long scanTimeout = Long.MAX_VALUE;

}
