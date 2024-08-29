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
package org.apache.accumulo.core.metrics;

public enum Metric {
  SERVER_IDLE("accumulo.server.idle", MetricType.GAUGE,
      "Indicates if the server is idle or not. The value will be 1 when idle and 0 when not idle.",
      MetricCategory.GENERAL_SERVER),
  LOW_MEMORY("accumulo.detected.low.memory", MetricType.GAUGE,
      "reports 1 when process memory usage is above threshold, 0 when memory is okay",
      MetricCategory.GENERAL_SERVER),
  SCAN_BUSY_TIMEOUT_COUNT("accumulo.scan.busy.timeout.count", MetricType.COUNTER,
      "Count of the scans where a busy timeout happened", MetricCategory.SCAN_SERVER);

  private final String name;
  private final MetricType type;
  private final String description;
  private final MetricCategory category;

  Metric(String name, MetricType type, String description, MetricCategory category) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.category = category;
  }

  public String getName() {
    return name;
  }

  public MetricType getType() {
    return type;
  }

  public String getDescription() {
    return description;
  }

  public MetricCategory getCategory() {
    return category;
  }

  public enum MetricType {
    GAUGE, COUNTER, TIMER, FUNCTION_COUNTER, LONG_TASK_TIMER
  }

  public enum MetricCategory {
    GENERAL_SERVER, COMPACTOR, SCAN_SERVER, FATE
  }

}
