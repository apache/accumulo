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
import org.slf4j.LoggerFactory;

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
 * <LI>{@code table.scan.dispatcher.opts.heed_hints=true|false} : This option defaults to false, so
 * by default execution hints are ignored. When set to true, the executor can be set on the scanner.
 * This is done by putting the key/value {@code executor=<scan executor name>} in the map passed to
 * {@link ScannerBase#setExecutionHints(Map)}
 * <LI>{@code table.scan.dispatcher.opts.bad_hint_action=none|log|error} : When
 * {@code heed_hints=true}, this option determines what to do if the executor in a hint does not
 * exist. The possible values for this option are {@code none}, {@code log}, or {@code error}.
 * Setting {@code none} will silently ignore invalid hints. Setting {@code log} will log a warning
 * for invalid hints. Setting {@code error} will throw an exception likely causing the scan to fail.
 * For {@code log} and {@code none}, when there is an invalid hint it will fall back to the table
 * configuration. The default is {@code none}.
 * <LI>{@code table.scan.dispatcher.opts.ignored_hint_action=none|log|error} : When
 * {@code heed_hints=false}, this option determines what to do if a hint specifies an executor. The
 * possible values for this option are {@code none}, {@code log}, or {@code error}. The default is
 * {@code none}.
 *
 * </UL>
 *
 * The {@code multi_executor} and {@code single_executor} options override the {@code executor}
 * option.
 */

public class SimpleScanDispatcher implements ScanDispatcher {

  private final Set<String> VALID_OPTS = ImmutableSet.of("executor", "multi_executor",
      "single_executor", "heed_hints", "bad_hint_action", "ignored_hint_action");
  private String multiExecutor;
  private String singleExecutor;
  private boolean heedHints;
  private HintProblemAction badHintAction;
  private HintProblemAction ignoredHintHaction;

  public static final String DEFAULT_SCAN_EXECUTOR_NAME = "default";

  private enum HintProblemAction {
    NONE, LOG, ERROR
  }

  @Override
  public void init(Map<String,String> options) {
    Set<String> invalidOpts = Sets.difference(options.keySet(), VALID_OPTS);
    Preconditions.checkArgument(invalidOpts.size() == 0, "Invalid options : %s", invalidOpts);

    String base = options.getOrDefault("executor", DEFAULT_SCAN_EXECUTOR_NAME);
    multiExecutor = options.getOrDefault("multi_executor", base);
    singleExecutor = options.getOrDefault("single_executor", base);
    heedHints = Boolean.parseBoolean(options.getOrDefault("heed_hints", "false"));
    badHintAction = HintProblemAction.valueOf(
        options.getOrDefault("bad_hint_action", HintProblemAction.NONE.name()).toUpperCase());
    ignoredHintHaction = HintProblemAction.valueOf(
        options.getOrDefault("ignored_hint_action", HintProblemAction.NONE.name()).toUpperCase());
  }

  @Override
  public String dispatch(ScanInfo scanInfo, Map<String,ScanExecutor> scanExecutors) {
    if (heedHints) {
      String executor = scanInfo.getExecutionHints().get("executor");
      if (executor != null) {
        if (scanExecutors.containsKey(executor)) {
          LoggerFactory.getLogger(SimpleScanDispatcher.class).info("executor {} ", executor);
          return executor;
        } else {
          switch (badHintAction) {
            case ERROR:
              throw new IllegalArgumentException(
                  "Scan execution hint contained unknown executor " + executor);
            case LOG:
              LoggerFactory.getLogger(SimpleScanDispatcher.class)
                  .warn("Scan execution hint contained unknown executor {} ", executor);
              break;
            case NONE:
              break;
            default:
              throw new IllegalStateException();
          }
        }
      }
    } else if (ignoredHintHaction != HintProblemAction.NONE
        && scanInfo.getExecutionHints().containsKey("executor")) {
      String executor = scanInfo.getExecutionHints().get("executor");
      switch (ignoredHintHaction) {
        case ERROR:
          throw new IllegalArgumentException(
              "Scan execution hint contained executor " + executor + " when heed_hints=false");
        case LOG:
          LoggerFactory.getLogger(SimpleScanDispatcher.class)
              .warn("Scan execution hint contained executor {} when heed_hints=false", executor);
          break;
        default:
          throw new IllegalStateException();
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
