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
package org.apache.accumulo.core.spi.scan;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.spi.scan.ScanDispatch.CacheUsage;

/**
 * If no options are given, then this will default to an executor named {@code default} and
 * {@link CacheUsage#TABLE} for index and data cache. This dispatcher supports the following
 * options.
 *
 * <ul>
 * <li>{@code table.scan.dispatcher.opts.executor=<scan executor name>} : dispatches all scans to
 * the named executor.</li>
 * <li>{@code table.scan.dispatcher.opts.multi_executor=<scan executor name>} : dispatches batch
 * scans to the named executor.</li>
 * <li>{@code table.scan.dispatcher.opts.single_executor=<scan executor name>} : dispatches regular
 * scans to the named executor.</li>
 * <li>{@code table.scan.dispatcher.opts.executor.<type>=<scan executor name>} : dispatches scans
 * that set the hint {@code scan_type=<type>} to the named executor. If this setting matches then it
 * takes precedence over all other settings. See {@link ScannerBase#setExecutionHints(Map)}</li>
 * <li>{@code table.scan.dispatcher.opts.cacheUsage.<type>[.index|.data]=enabled|disabled|opportunistic|table}
 * : for scans that set the hint {@code scan_type=<type>} determines how the scan will use cache.
 * </ul>
 *
 * The {@code multi_executor} and {@code single_executor} options override the {@code executor}
 * option.
 */

public class SimpleScanDispatcher implements ScanDispatcher {

  private final String EXECUTOR_PREFIX = "executor.";

  private final Set<String> VALID_OPTS = Set.of("executor", "multi_executor", "single_executor");

  private ScanDispatch singleDispatch;
  private ScanDispatch multiDispatch;
  private Map<String,Map<ScanInfo.Type,ScanDispatch>> hintDispatch;

  private static Pattern CACHE_PATTERN = Pattern.compile("cacheUsage[.](\\w+)([.](index|data))?");

  public static final String DEFAULT_SCAN_EXECUTOR_NAME = "default";

  @Override
  public void init(InitParameters params) {
    Map<String,String> options = params.getOptions();

    Map<String,CacheUsage> indexCacheUsage = new HashMap<>();
    Map<String,CacheUsage> dataCacheUsage = new HashMap<>();
    Map<String,String> scanExecutors = new HashMap<>();
    Set<String> hintScanTypes = new HashSet<>();

    options.forEach((k, v) -> {

      Matcher cacheMatcher = CACHE_PATTERN.matcher(k);

      if (k.startsWith(EXECUTOR_PREFIX)) {
        String hintScanType = k.substring(EXECUTOR_PREFIX.length());
        scanExecutors.put(hintScanType, v);
        hintScanTypes.add(hintScanType);
      } else if (cacheMatcher.matches()) {
        String hintScanType = cacheMatcher.group(1);
        CacheUsage usage = CacheUsage.valueOf(v.toUpperCase());
        String cacheType = cacheMatcher.group(3);

        hintScanTypes.add(hintScanType);

        if ("index".equals(cacheType)) {
          indexCacheUsage.put(hintScanType, usage);
        } else if ("data".equals(cacheType)) {
          dataCacheUsage.put(hintScanType, usage);
        } else {
          indexCacheUsage.put(hintScanType, usage);
          dataCacheUsage.put(hintScanType, usage);
        }
      } else if (!VALID_OPTS.contains(k)) {
        throw new IllegalArgumentException("Invalid option " + k);
      }
    });

    // This method pre-computes all possible scan dispatch objects that could ever be needed.
    // This is done to make the dispatch method more efficient. If the number of config permutations
    // grows, this approach may have to be abandoned. For now its tractable.

    ScanDispatch baseDispatch = Optional.ofNullable(options.get("executor"))
        .map(name -> ScanDispatch.builder().setExecutorName(name).build())
        .orElse(DefaultScanDispatch.DEFAULT_SCAN_DISPATCH);
    singleDispatch = Optional.ofNullable(options.get("single_executor"))
        .map(name -> ScanDispatch.builder().setExecutorName(name).build()).orElse(baseDispatch);
    multiDispatch = Optional.ofNullable(options.get("multi_executor"))
        .map(name -> ScanDispatch.builder().setExecutorName(name).build()).orElse(baseDispatch);

    hintDispatch = hintScanTypes.stream()
        .collect(Collectors.toUnmodifiableMap(Function.identity(), hintScanType -> {
          EnumMap<ScanInfo.Type,ScanDispatch> precomupted = new EnumMap<>(ScanInfo.Type.class);
          CacheUsage iCacheUsage = indexCacheUsage.getOrDefault(hintScanType, CacheUsage.TABLE);
          CacheUsage dCacheUsage = dataCacheUsage.getOrDefault(hintScanType, CacheUsage.TABLE);
          precomupted.put(ScanInfo.Type.SINGLE,
              ScanDispatch.builder()
                  .setExecutorName(
                      scanExecutors.getOrDefault(hintScanType, singleDispatch.getExecutorName()))
                  .setIndexCacheUsage(iCacheUsage).setDataCacheUsage(dCacheUsage).build());
          precomupted.put(ScanInfo.Type.MULTI,
              ScanDispatch.builder()
                  .setExecutorName(
                      scanExecutors.getOrDefault(hintScanType, multiDispatch.getExecutorName()))
                  .setIndexCacheUsage(iCacheUsage).setDataCacheUsage(dCacheUsage).build());
          return precomupted;
        }));
  }

  @Override
  public ScanDispatch dispatch(DispatchParameters params) {
    ScanInfo scanInfo = params.getScanInfo();

    if (!hintDispatch.isEmpty()) {
      String hintScanType = scanInfo.getExecutionHints().get("scan_type");
      if (hintScanType != null) {
        var precomputedDispatch = hintDispatch.get(hintScanType);
        if (precomputedDispatch != null) {
          return precomputedDispatch.get(scanInfo.getScanType());
        }
      }
    }

    switch (scanInfo.getScanType()) {
      case MULTI:
        return multiDispatch;
      case SINGLE:
        return singleDispatch;
      default:
        throw new IllegalArgumentException("Unexpected scan type " + scanInfo.getScanType());
    }

  }
}
