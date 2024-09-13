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

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Prior to 2.1.0 Accumulo used the <a href=
 * "https://hadoop.apache.org/docs/current/api/org/apache/hadoop/metrics2/package-summary.html">Hadoop
 * Metrics2</a> framework. In 2.1.0 Accumulo migrated away from the Metrics2 framework to
 * <a href="https://micrometer.io/">Micrometer</a>. Micrometer suggests using a particular
 * <a href="https://micrometer.io/docs/concepts#_naming_meters">naming convention</a> for the
 * metrics.
 *
 * @since 2.1.0
 */
public interface MetricsProducer {
  /**
   * Build Micrometer Meter objects and register them with the registry
   */
  void registerMetrics(MeterRegistry registry);

}
