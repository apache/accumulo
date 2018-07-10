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
package org.apache.accumulo.core.spi.scan;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * If no options are given, then this will dispatch to an executor named {@code default}. This
 * dispatcher supports the following options.
 *
 * <UL>
 * <LI>{@code table.scan.dispatcher.opts.executor=<scan executor name>} : dispatches all scans to
 * the named executor.</LI>
 * <LI>{@code table.scan.dispatcher.opts.multi_executor=<scan executor name>} : dispatches batch
 * scans to the named executor.</LI>
 * <LI>{@code table.scan.dispatcher.opts.single_executor=<scan executor name>} : dispatches regular
 * scans to the named executor.</LI>
 * </UL>
 *
 * The {@code multi_executor} and {@code single_executor} options override the {@code executor}
 * option.
 */

public class SimpleScanDispatcher implements ScanDispatcher {

  private final Set<String> VALID_OPTS = ImmutableSet.of("executor", "multi_executor",
      "single_executor");
  private String multiExecutor;
  private String singleExecutor;

  public static final String DEFAULT_SCAN_EXECUTOR_NAME = "default";

  @Override
  public void init(Map<String,String> options) {
    Set<String> invalidOpts = Sets.difference(options.keySet(), VALID_OPTS);
    Preconditions.checkArgument(invalidOpts.size() == 0, "Invalid options : %s", invalidOpts);

    String base = options.getOrDefault("executor", DEFAULT_SCAN_EXECUTOR_NAME);
    multiExecutor = options.getOrDefault("multi_executor", base);
    singleExecutor = options.getOrDefault("single_executor", base);
  }

  @Override
  public String dispatch(ScanInfo scanInfo, Map<String,ScanExecutor> scanExecutors) {
    switch (scanInfo.getScanType()) {
      case MULTI:
        return multiExecutor;
      case SINGLE:
        return singleExecutor;
      default:
        throw new IllegalArgumentException("Unexpected scan type " + scanInfo.getScanType());

    }
  }
}
