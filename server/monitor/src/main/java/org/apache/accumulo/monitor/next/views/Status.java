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
package org.apache.accumulo.monitor.next.views;

import java.util.ArrayList;
import java.util.List;

/**
 * all the data needed for the Server status indicator(s)
 */
public record Status(boolean hasServers, boolean hasProblemServers, boolean hasMissingMetrics,
    long serverCount, long problemServerCount, long missingMetricServerCount, String level,
    String message) {

  private static final String LEVEL_OK = "OK";
  private static final String LEVEL_ERROR = "ERROR";
  private static final String LEVEL_WARN = "WARN";

  public static Status buildStatus(int serverCount, long problemServerCount,
      int serversMissingMetrics) {
    return buildStatus(serverCount, problemServerCount, serversMissingMetrics, false);
  }

  public static Status buildStatus(int serverCount, long problemServerCount,
      int serversMissingMetrics, boolean errorWhenAllServersUnavailable) {
    final boolean hasServers = serverCount > 0;
    final boolean hasProblemServers = problemServerCount > 0;
    final boolean hasMissingMetrics = serversMissingMetrics > 0;
    final boolean allServersUnavailable =
        errorWhenAllServersUnavailable && hasServers && problemServerCount >= serverCount;

    if (allServersUnavailable) {
      return new Status(true, true, hasMissingMetrics, serverCount, problemServerCount,
          serversMissingMetrics, LEVEL_ERROR, "ERROR: All servers are unavailable.");
    }

    List<String> warnings = new ArrayList<>(2);
    if (hasProblemServers) {
      warnings.add("One or more servers are unavailable");
    }
    if (hasMissingMetrics) {
      warnings.add("Metrics are not present (are metrics enabled?)");
    }

    if (warnings.isEmpty()) {
      // no warnings, set status to OK
      return new Status(hasServers, false, false, serverCount, 0, 0, LEVEL_OK, null);
    }

    final String message = "WARN: " + String.join("; ", warnings) + ".";
    return new Status(hasServers, hasProblemServers, hasMissingMetrics, serverCount,
        problemServerCount, serversMissingMetrics, LEVEL_WARN, message);
  }

}
