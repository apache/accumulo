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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

public class MicrometerMetricsFactory {

  private static final Pattern MATCH_ON_NAME =
      Pattern.compile("metrics.service.(?<index>\\d+).name");

  private static final Property METRICS_CONFIG_PROPERTIES =
      Property.GENERAL_METRICS_CONFIGURATION_PROPERTIES_FILE;

  private static final Logger log = LoggerFactory.getLogger(MicrometerMetricsFactory.class);

  private final CompositeMeterRegistry registry;

  private MicrometerMetricsFactory(final ServerContext context, final String appName) {

    registry = new CompositeMeterRegistry();

    var propKey = METRICS_CONFIG_PROPERTIES.getKey();

    var filename = context.getConfiguration().get(propKey);

    log.debug("Load metrics configuration properties  {}", filename);

    try {

      Map<String,Map<String,String>> serviceProperties = loadFromClasspath(filename);

      log.trace("Props: {}", serviceProperties);

      ServiceLoader<MetricsRegistrationService> loader =
          ServiceLoader.load(MetricsRegistrationService.class);
      for (MetricsRegistrationService metrics : loader) {
        for (Map.Entry<String,Map<String,String>> entry : serviceProperties.entrySet()) {
          if (entry.getValue().get("classname").equals(metrics.getClass().getName())) {
            metrics.register(context, entry.getKey(), entry.getValue(), registry);
          }
        }
      }

    } catch (IOException ex) {
      throw new IllegalStateException("Failed to load metrics configuration properties: " + propKey,
          ex);
    }

  }

  public MeterRegistry getRegistry() {
    return registry;
  }

  private static MicrometerMetricsFactory _instance = null;

  public static synchronized MicrometerMetricsFactory create(final ServerContext context,
      final String appName) {
    if (Objects.isNull(_instance)) {
      _instance = new MicrometerMetricsFactory(context, appName);
    }
    return _instance;
  }

  /**
   * Load the metrics configurations from the classpath.
   *
   * @param filename
   *          the metrics configurations properties filename.
   * @return multimap, keyed by service name of metrics configuration properties.
   * @throws IOException
   *           if an error occurs reading file.
   */
  private Map<String,Map<String,String>> loadFromClasspath(final String filename)
      throws IOException {

    log.warn("Looking for: {}", filename);

    ClassLoader clazz = this.getClass().getClassLoader();
    InputStream in = clazz.getResourceAsStream(filename);
    Properties p = new Properties();
    p.load(in);

    return getServiceProps(p);
  }

  /**
   * Returns a multimap - keyed by service name (defined in properties file) - the inner map is the
   * properties (String,String) pair for the service.
   *
   * @param properties
   *          a metrics properties configuration file.
   * @return A multimap of properties maps by service.
   */
  private Map<String,Map<String,String>> getServiceProps(final Properties properties) {

    Map<String,Map<String,String>> propsByService = new HashMap<>();

    // process the properties name extract the service name, index value.
    // using the metrics.service.##.name=serviceName
    // The resulting map is serviceName, index
    Map<String,String> byIndex = new HashMap<>();
    properties.stringPropertyNames().forEach(n -> {
      Matcher m = MATCH_ON_NAME.matcher(n);
      if (m.matches()) {
        byIndex.put(properties.getProperty(n), m.group("index"));
      }
    });

    byIndex.forEach((s, n) -> {
      var serviceMap = propsByService.computeIfAbsent(s, m -> new HashMap<>());
      var prefix = "metrics.service." + n + ".";
      properties.stringPropertyNames().forEach(p -> {
        if (p.startsWith(prefix)) {
          String v = properties.getProperty(p);
          String k = p.substring(prefix.length());
          serviceMap.put(k, v);
        }
      });
    });

    return propsByService;
  }

}
