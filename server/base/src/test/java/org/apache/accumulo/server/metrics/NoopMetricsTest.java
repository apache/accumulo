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
package org.apache.accumulo.server.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;

public class NoopMetricsTest {
  @Test
  public void testNames() {
    Counter c1 = NoopMetrics.useNoopCounter();
    Counter c2 = NoopMetrics.useNoopCounter();

    Gauge g1 = NoopMetrics.useNoopGauge();

    assertNotEquals(c1.getId(), c2.getId());
    assertNotEquals(c1.getId(), g1.getId());
  }

  @Test
  public void testNoOp() {
    DistributionSummary noop = NoopMetrics.useNoopDistributionSummary();
    assertDoesNotThrow(() -> noop.getId());
    assertDoesNotThrow(() -> noop.takeSnapshot());
    assertDoesNotThrow(() -> noop.max());
    assertDoesNotThrow(() -> noop.count());
    assertDoesNotThrow(() -> noop.totalAmount());
  }
}
