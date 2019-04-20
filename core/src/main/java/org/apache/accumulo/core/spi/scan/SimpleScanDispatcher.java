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

import org.apache.accumulo.core.client.ScannerBase;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;

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
 * <LI>{@code table.scan.dispatcher.opts.executor.<type>=<scan executor name>} : dispatches scans
 * that set the hint {@code scan_type=<type>} to the named executor. If this setting matches then it
 * takes precedence over all other settings. See {@link ScannerBase#setExecutionHints(Map)}</LI>
 *
 * </UL>
 *
 * The {@code multi_executor} and {@code single_executor} options override the {@code executor}
 * option.
 */

public class SimpleScanDispatcher implements ScanDispatcher {

  private final String EXECUTOR_PREFIX = "executor.";

  private final Set<String> VALID_OPTS =
      ImmutableSet.of("executor", "multi_executor", "single_executor");
  private String multiExecutor;
  private String singleExecutor;

  private Map<String,String> typeExecutors;

  public static final String DEFAULT_SCAN_EXECUTOR_NAME = "default";

  @Override
  public void init(InitParameters params) {
    Map<String,String> options = params.getOptions();

    Builder<String,String> teb = ImmutableMap.builder();

    options.forEach((k, v) -> {
      if (k.startsWith(EXECUTOR_PREFIX)) {
        String type = k.substring(EXECUTOR_PREFIX.length());
        teb.put(type, v);
      } else if (!VALID_OPTS.contains(k)) {
        throw new IllegalArgumentException("Invalid option " + k);
      }
    });

    typeExecutors = teb.build();

    String base = options.getOrDefault("executor", DEFAULT_SCAN_EXECUTOR_NAME);
    multiExecutor = options.getOrDefault("multi_executor", base);
    singleExecutor = options.getOrDefault("single_executor", base);

  }

  @Override
  public String dispatch(DispatchParmaters params) {
    ScanInfo scanInfo = params.getScanInfo();

    if (!typeExecutors.isEmpty()) {
      String scanType = scanInfo.getExecutionHints().get("scan_type");
      if (scanType != null) {
        String executor = typeExecutors.get(scanType);
        if (executor != null) {
          return executor;
        }
      }
    }

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
