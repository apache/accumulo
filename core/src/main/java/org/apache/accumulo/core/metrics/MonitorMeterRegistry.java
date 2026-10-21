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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.step.StepRegistryConfig;

public class MonitorMeterRegistry extends StepMeterRegistry {

  public static final Duration STEP = Duration.ofSeconds(30);

  private static final StepRegistryConfig CONFIG = new StepRegistryConfig() {

    @Override
    public String prefix() {
      return "monitor";
    }

    @Override
    public String get(String key) {
      return null;
    }

    @Override
    public Duration step() {
      return STEP;
    }
  };

  public MonitorMeterRegistry() {
    super(CONFIG, Clock.SYSTEM);
  }

  @Override
  protected void publish() {

  }

  @Override
  protected TimeUnit getBaseTimeUnit() {
    return TimeUnit.MILLISECONDS;
  }
}
