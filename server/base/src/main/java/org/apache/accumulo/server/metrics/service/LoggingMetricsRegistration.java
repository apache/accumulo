/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.metrics.service;

import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;

@AutoService(MetricsRegistrationService.class)
public class LoggingMetricsRegistration implements MetricsRegistrationService {

  private static final Logger metricsLogger =
      LoggerFactory.getLogger("org.apache.accumulo.metrics");

  private static final Logger log = LoggerFactory.getLogger(LoggingMetricsRegistration.class);

  private final LoggingRegistryConfig lconf = c -> {
    if (c.equals("logging.step")) {
      return "5s";
    }
    return null;
  };

  private final Consumer<String> metricConsumer = metricsLogger::info;

  @Override
  public void register(String name, Map<String,String> properties,
      CompositeMeterRegistry registry) {
    log.info("Loading metrics service name: {}, to {}, with props: {}", name, registry, properties);
    if ("true".equals(properties.get("enabled"))) {
      log.info("enabled");

      log.warn("ML: {} - level: {}", metricsLogger.getName(), metricsLogger.isDebugEnabled());

      LoggingMeterRegistry lmr =
          LoggingMeterRegistry.builder(lconf).loggingSink(metricConsumer).build();
      registry.add(lmr);

    }
  }

}
