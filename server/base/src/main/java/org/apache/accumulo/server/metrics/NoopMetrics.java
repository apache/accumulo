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

import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.noop.NoopCounter;
import io.micrometer.core.instrument.noop.NoopDistributionSummary;
import io.micrometer.core.instrument.noop.NoopFunctionTimer;
import io.micrometer.core.instrument.noop.NoopGauge;
import io.micrometer.core.instrument.noop.NoopTimer;

/**
 * Convenience utility class to return micrometer noop metrics. Initialization of the metrics system
 * registry can be delayed so that common tags with values determined at runtime (for example, port
 * numbers). Initializing meters that are created from the registry at initialization with a noop
 * implementation prevents NPEs if something tries to record a metric value before the
 * initialization has run.
 */
public class NoopMetrics {
  private final static AtomicInteger idCount = new AtomicInteger(0);

  private NoopMetrics() {}

  public static Counter useNoopCounter() {
    return new NoopCounter(noopId(Meter.Type.COUNTER));
  }

  public static Gauge useNoopGauge() {
    return new NoopGauge(noopId(Meter.Type.GAUGE));
  }

  public static DistributionSummary useNoopDistributionSummary() {
    return new NoopDistributionSummary(noopId(Meter.Type.DISTRIBUTION_SUMMARY));
  }

  public static Timer useNoopTimer() {
    return new NoopTimer(noopId(Meter.Type.TIMER));
  }

  public static FunctionTimer useNoopFunctionTimer() {
    return new NoopFunctionTimer(noopId(Meter.Type.TIMER));
  }

  /**
   * provide a unique id to guard against the id being used as a key.
   */
  private static Meter.Id noopId(final Meter.Type type) {
    return new Meter.Id(uniqueName(), Tags.of("noop", "none"), "", "", type);
  }

  private static String uniqueName() {
    return "noop" + idCount.incrementAndGet();
  }

}
