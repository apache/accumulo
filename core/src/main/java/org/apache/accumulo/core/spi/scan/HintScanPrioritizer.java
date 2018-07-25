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

import java.util.Comparator;
import java.util.Map;

import org.apache.accumulo.core.client.ScannerBase;
import org.slf4j.LoggerFactory;

/**
 * When configured for a scan executor, this prioritizer allows scanners to set priorities as
 * integers.
 *
 * <p>
 * Scanners should put the key/value {@code priority=<integer>} in the map passed to
 * {@link ScannerBase#setExecutionHints(Map)} to set the priority. Lower integers result in higher
 * priority.
 *
 * <p>
 * This prioritizer accepts the option {@code default_priority=<integer>} which determines what
 * priority to use for scans without a hint. If not set, then {@code default_priority} is
 * {@link Integer#MAX_VALUE}.
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
 * case there the primary of secondary priorities are outside expected ranges.
 *
 * @since 2.0.0
 */
public class HintScanPrioritizer implements ScanPrioritizer {

  private static int getPriority(ScanInfo si, int defaultPriority) {
    String prio = si.getExecutionHints().get("priority");
    if (prio == null) {
      return defaultPriority;
    } else {
      LoggerFactory.getLogger(HintScanPrioritizer.class).info("priority=" + prio);
      return Integer.parseInt(prio);
    }
  }

  @Override
  public Comparator<ScanInfo> createComparator(Map<String,String> options) {
    int defaultPriority = Integer
        .parseInt(options.getOrDefault("default_priority", Integer.MAX_VALUE + ""));

    Comparator<ScanInfo> cmp = Comparator.comparingInt(si -> getPriority(si, defaultPriority));

    return cmp.thenComparingLong(si -> si.getLastRunTime().orElse(0))
        .thenComparingLong(si -> si.getCreationTime());
  }
}
