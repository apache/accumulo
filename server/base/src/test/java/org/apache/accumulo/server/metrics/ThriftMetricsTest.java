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

import static org.apache.accumulo.core.metrics.MetricsProducer.METRICS_THRIFT_EXECUTE;
import static org.apache.accumulo.core.metrics.MetricsProducer.METRICS_THRIFT_IDLE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.noop.NoopDistributionSummary;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class ThriftMetricsTest {
  private final static Logger LOG = LoggerFactory.getLogger(ThriftMetricsTest.class);

  @Test
  void testNoNPE() {
    ThriftMetrics tm = new ThriftMetrics();
    assertDoesNotThrow(() -> tm.addIdle(1));
    assertDoesNotThrow(() -> tm.addExecute(1));
  }

  @Test
  void registerMetrics() {
    MeterRegistry registry = new SimpleMeterRegistry();
    ThriftMetrics tm = new ThriftMetrics();
    tm.registerMetrics(registry);
    tm.addExecute(1000);
    tm.addIdle(1000);

    registry.forEachMeter(m -> {
      LOG.trace("meter: {}", m.getId());
      assertInstanceOf(DistributionSummary.class, m);
      assertFalse(m instanceof NoopDistributionSummary);
    });
    assertTrue(registry.get(METRICS_THRIFT_IDLE).summary().count() > 0);
    assertTrue(registry.get(METRICS_THRIFT_EXECUTE).summary().count() > 0);
  }
}
