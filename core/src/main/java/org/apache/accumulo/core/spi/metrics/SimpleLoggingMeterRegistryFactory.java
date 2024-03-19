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
package org.apache.accumulo.core.spi.metrics;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;

/**
 * Example implementation of enabling a metrics provider by implementing the
 * {@link org.apache.accumulo.core.spi.metrics.MeterRegistryFactory} interface. When enabled though
 * properties by enabling {@code Property.GENERAL_MICROMETER_ENABLED} and providing this class for
 * the {@code Property.GENERAL_MICROMETER_FACTORY}
 * <p>
 * The metrics will appear in the normal service logs with a named logger of
 * {@code org.apache.accumulo.METRICS} at the INFO level. The metrics output can be directed to a
 * file using standard logging configuration properties.
 */
public class SimpleLoggingMeterRegistryFactory implements MeterRegistryFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(SimpleLoggingMeterRegistryFactory.class);

  // named logger that can be configured using standard logging properties.
  private static final Logger METRICS = LoggerFactory.getLogger("org.apache.accumulo.METRICS");

  private final Consumer<String> metricConsumer = METRICS::info;

  // defines the metrics update period, default is 60 seconds.
  private final LoggingRegistryConfig lconf = c -> {
    if (c.equals("logging.step")) {
      return "60s";
    }
    return null;
  };

  @Override
  public MeterRegistry create() {
    LOG.info("starting metrics registration.");
    return LoggingMeterRegistry.builder(lconf).loggingSink(metricConsumer).build();
  }
}
