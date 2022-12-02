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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;

public class MetricsUtil {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsUtil.class);

  private static JvmGcMetrics gc;
  private static List<Tag> commonTags;

  public static void initializeMetrics(final AccumuloConfiguration conf, final String appName,
      final HostAndPort address) throws ClassNotFoundException, InstantiationException,
      IllegalAccessException, IllegalArgumentException, InvocationTargetException,
      NoSuchMethodException, SecurityException {
    initializeMetrics(conf.getBoolean(Property.GENERAL_MICROMETER_ENABLED),
        conf.getBoolean(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED),
        conf.get(Property.GENERAL_MICROMETER_FACTORY), appName, address);
  }

  private static void initializeMetrics(boolean enabled, boolean jvmMetricsEnabled,
      String factoryClass, String appName, HostAndPort address) throws ClassNotFoundException,
      InstantiationException, IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, NoSuchMethodException, SecurityException {

    LOG.info("initializing metrics, enabled:{}, class:{}", enabled, factoryClass);

    if (enabled && factoryClass != null && !factoryClass.isEmpty()) {

      String processName = appName;
      String serviceInstance = System.getProperty("accumulo.metrics.service.instance", "");
      if (!serviceInstance.isBlank()) {
        processName += serviceInstance;
      }

      List<Tag> tags = new ArrayList<>();
      tags.add(Tag.of("process.name", processName));

      if (address != null) {
        tags.add(Tag.of("Address", address.toString()));
      }
      commonTags = Collections.unmodifiableList(tags);

      Class<? extends MeterRegistryFactory> clazz =
          ClassLoaderUtil.loadClass(factoryClass, MeterRegistryFactory.class);
      MeterRegistryFactory factory = clazz.getDeclaredConstructor().newInstance();

      MeterRegistry registry = factory.create();
      registry.config().commonTags(commonTags);
      Metrics.addRegistry(registry);

      if (jvmMetricsEnabled) {
        new ClassLoaderMetrics(commonTags).bindTo(Metrics.globalRegistry);
        new JvmMemoryMetrics(commonTags).bindTo(Metrics.globalRegistry);
        gc = new JvmGcMetrics(commonTags);
        gc.bindTo(Metrics.globalRegistry);
        new ProcessorMetrics(commonTags).bindTo(Metrics.globalRegistry);
        new JvmThreadMetrics(commonTags).bindTo(Metrics.globalRegistry);
      }
    }
  }

  public static void initializeProducers(MetricsProducer... producer) {
    for (MetricsProducer p : producer) {
      p.registerMetrics(Metrics.globalRegistry);
      LOG.info("Metric producer {} initialize", p.getClass().getSimpleName());
    }
  }

  public static void addExecutorServiceMetrics(ExecutorService executor, String name) {
    new ExecutorServiceMetrics(executor, name, commonTags).bindTo(Metrics.globalRegistry);
  }

  public static List<Tag> getCommonTags() {
    return commonTags;
  }

  public static void close() {
    if (gc != null) {
      gc.close();
    }
  }

}
