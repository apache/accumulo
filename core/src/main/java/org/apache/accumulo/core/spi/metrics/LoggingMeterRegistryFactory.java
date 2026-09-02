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

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
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
 *
 * To filter out meters that start with any particular prefix, set
 * {@code general.custom.metrics.opts.meter.id.filters} in the Accumulo configuration.
 *
 * <pre>
 *      general.custom.metrics.opts.meter.id.filters = prefix1,prefix2,prefix3
 *
 * </pre>
 *
 * To assign a custom delimiter to filters, set
 * {@code general.custom.metrics.opts.meter.id.delimiter} in the Accumulo configuration.
 *
 * <pre>
 * general.custom.metrics.opts.meter.id.delimiter = delimiter
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

    MeterRegistry registry =
        LoggingMeterRegistry.builder(lconf).loggingSink(metricConsumer).build();
    String filters = metricsProps.get("meter.id.filters");
    // Sets the default delimiter if none is specified
    String delimiter = metricsProps.getOrDefault("meter.id.delimiter", ",");

    if (filters != null && delimiter != null) {
      registry.config().meterFilter(getMeterFilter(filters, delimiter));
    }
    return registry;
  }

  /**
   * This function uses terms specified in the patternList parameter to filter out specific metrics
   * that the user doesn't want.
   *
   * @param patternList a delimited set of terms that will filter out meters that start with any one
   *        of those terms.
   * @param delimiter that contains either a user specified delimiter, or the default comma
   *        delimiter to split the patternList.
   * @return a predicate with the type of MeterFilter, that describes which metrics to deny and
   *         subsequently filter out.
   */
  public static MeterFilter getMeterFilter(String patternList, String delimiter) {
    requireNonNull(patternList, "patternList must not be null");

    // Trims whitespace and all other non-visible characters.
    patternList = patternList.replaceAll("\\s+", "");

    // Gets the default delimiter or the delimiter the user supplied
    String[] patterns = patternList.split(delimiter);
    Predicate<Meter.Id> finalPredicate = id -> false;

    if (patternList.isEmpty()) {
      return MeterFilter.deny(finalPredicate);
    }

    for (String prefix : patterns) {
      Predicate<Meter.Id> predicate = id -> id.getName().startsWith(prefix);
      finalPredicate = finalPredicate.or(predicate);

    }
    return MeterFilter.deny(finalPredicate);
  }
}
