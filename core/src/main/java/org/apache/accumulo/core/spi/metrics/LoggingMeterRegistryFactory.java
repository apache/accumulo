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

import java.util.HashMap;
import java.util.Map;
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
 * file using standard logging configuration properties by configuring the log4j2-service.properties
 * file.
 * <p>
 * Properties can be passed in the Accumulo properties files using the prefix
 * {@code general.custom.metrics.opts}
 * <p>
 * For example, the default polling rate is 60 sec. To modify the update frequency set
 * {@code general.custom.metrics.opts.logging.step} in the Accumulo configuration.
 *
 * <pre>
 *     general.custom.metrics.opts.logging.step = 10s
 * </pre>
 */
public class LoggingMeterRegistryFactory implements MeterRegistryFactory {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingMeterRegistryFactory.class);

  // named logger that can be configured using standard logging properties.
  private static final Logger METRICS = LoggerFactory.getLogger("org.apache.accumulo.METRICS");

  public LoggingMeterRegistryFactory() {
    // needed for classloader
  }

  @Override
  public MeterRegistry create(final InitParameters params) {
    final Consumer<String> metricConsumer = METRICS::info;
    final Map<String,String> metricsProps = new HashMap<>();

    // defines the metrics update period, default is 60 seconds.
    final LoggingRegistryConfig lconf = c -> {
      if (c.equals("logging.step")) {
        return metricsProps.getOrDefault("logging.step", "60s");
      }
      return null;
    };

    LOG.info("Creating logging metrics registry with params: {}", params);
    metricsProps.putAll(params.getOptions());
    return LoggingMeterRegistry.builder(lconf).loggingSink(metricConsumer).build();
  }
}
