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

import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.Comparator;
import java.util.Map;

import org.apache.accumulo.core.client.ScannerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When configured for a scan executor, this prioritizer allows scanners to set priorities as
 * integers. Lower integers result in higher priority.
 *
 * <p>
 * Scanners can put the key/values {@code priority=<integer>} and/or {@code scan_type=<type>} in the
 * map passed to {@link ScannerBase#setExecutionHints(Map)} to set the priority. When a
 * {@code priority} hint is set it takes precedence and the value is used as the priority. When a
 * {@code scan_type} hint is set the priority is looked up using the value.
 *
 * <p>
 * This prioritizer accepts the option {@code default_priority=<integer>} which determines what
 * priority to use for scans without a hint. If not set, then {@code default_priority} is
 * {@link Integer#MAX_VALUE}.
 *
 * <p>
 * This prioritizer accepts the option {@code bad_hint_action=fail|log|none}. This option determines
 * what happens when a priority hint is not an integer. It defaults to {@code log} which logs a
 * warning. The {@code fail} option throws an exception which may fail the scan. The {@code none}
 * option silently ignores invalid hints.
 *
 * <p>
 * This prioritizer accepts the option {@code priority.<type>=<integer>} which maps a scan type hint
 * to a priority.
 *
 * <p>
 * When two scans have the same priority, the scan is prioritized based on last run time and then
 * creation time.
 *
 * <p>
 * If a secondary or tertiary priority is needed, this can be done with bit shifting. For example
 * assume a primary priority of 1 to 3 is desired followed by a secondary priority of 1 to 10 . This
 * can be encoded as {@code int priority = primary << 4 | secondary}. When the primary bits are
 * equal the comparison naturally falls back to the secondary bits. The example does not handle the
 * case where the primary of secondary priorities are outside expected ranges.
 *
 * @since 2.0.0
 */
public class HintScanPrioritizer implements ScanPrioritizer {

  private static final Logger log = LoggerFactory.getLogger(HintScanPrioritizer.class);

  private final String PRIO_PREFIX = "priority.";

  private enum HintProblemAction {
    NONE, LOG, FAIL
  }

  private static int getPriority(ScanInfo si, int defaultPriority, HintProblemAction hpa,
      Map<String,Integer> typePriorities) {
    String prio = si.getExecutionHints().get("priority");
    if (prio != null) {
      try {
        return Integer.parseInt(prio);
      } catch (NumberFormatException nfe) {
        switch (hpa) {
          case FAIL:
            throw nfe;
          case LOG:
            log.warn("Unable to parse priority hint {}, falling back to default {}.", prio,
                defaultPriority);
            break;
          case NONE:
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }

    if (!typePriorities.isEmpty()) {
      String scanType = si.getExecutionHints().get("scan_type");
      if (scanType != null) {
        Integer typePrio = typePriorities.get(scanType);
        if (typePrio != null) {
          return typePrio;
        }
      }
    }

    return defaultPriority;
  }

  @Override
  public Comparator<ScanInfo> createComparator(CreateParameters params) {
    int defaultPriority = Integer
        .parseInt(params.getOptions().getOrDefault("default_priority", Integer.MAX_VALUE + ""));

    Map<String,Integer> typePriorities =
        params.getOptions().entrySet().stream().filter(e -> e.getKey().startsWith(PRIO_PREFIX))
            .collect(toUnmodifiableMap(e -> e.getKey().substring(PRIO_PREFIX.length()),
                e -> Integer.parseInt(e.getValue())));

    HintProblemAction hpa = HintProblemAction.valueOf(params.getOptions()
        .getOrDefault("bad_hint_action", HintProblemAction.LOG.name()).toUpperCase());

    Comparator<ScanInfo> cmp =
        Comparator.comparingInt(si -> getPriority(si, defaultPriority, hpa, typePriorities));

    return cmp.thenComparingLong(si -> si.getLastRunTime().orElse(0))
        .thenComparingLong(ScanInfo::getCreationTime);
  }
}
