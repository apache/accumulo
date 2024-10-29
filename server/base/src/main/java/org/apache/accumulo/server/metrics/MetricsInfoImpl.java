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

import static org.apache.hadoop.util.StringUtils.getTrimmedStrings;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.server.ServerContext;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;

public class MetricsInfoImpl implements MetricsInfo {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsInfoImpl.class);

  private final ServerContext context;

  private final Lock lock = new ReentrantLock();

  private final Map<String,Tag> commonTags;

  // JvmGcMetrics are declared with AutoCloseable - keep reference to use with close()
  private JvmGcMetrics jvmGcMetrics;

  private final boolean metricsEnabled;

  private MeterRegistry meterRegistry = null;

  private final List<MetricsProducer> producers = new ArrayList<>();

  public MetricsInfoImpl(final ServerContext context) {
    this.context = context;
    metricsEnabled = context.getConfiguration().getBoolean(Property.GENERAL_MICROMETER_ENABLED);
    printMetricsConfig();
    commonTags = new HashMap<>();
    Tag t = MetricsInfo.instanceNameTag(context.getInstanceName());
    commonTags.put(t.getKey(), t);
  }

  private void printMetricsConfig() {
    final boolean jvmMetricsEnabled =
        context.getConfiguration().getBoolean(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED);
    LOG.info("micrometer metrics enabled: {}", metricsEnabled);
    if (!metricsEnabled) {
      return;
    }
    if (jvmMetricsEnabled) {
      LOG.info("detailed jvm metrics enabled: {}", jvmMetricsEnabled);
    }
    LOG.info("metrics registry factories: {}",
        context.getConfiguration().get(Property.GENERAL_MICROMETER_FACTORY));
  }

  @Override
  public boolean isMetricsEnabled() {
    return metricsEnabled;
  }

  /**
   * Common tags for all services.
   */
  @Override
  public void addServiceTags(final String applicationName, final HostAndPort hostAndPort,
      final String resourceGroupName) {
    if (!metricsEnabled) {
      return;
    }
    List<Tag> tags = new ArrayList<>();

    tags.add(MetricsInfo.processTag(applicationName));
    tags.addAll(MetricsInfo.addressTags(hostAndPort));
    tags.add(MetricsInfo.resourceGroupTag(resourceGroupName));

    addCommonTags(tags);
  }

  @Override
  public void addCommonTags(List<Tag> updates) {
    if (!metricsEnabled) {
      return;
    }
    lock.lock();
    try {
      if (meterRegistry != null) {
        LOG.warn(
            "Common tags after registry has been initialized may be ignored. Current common tags: {}",
            commonTags);
        return;
      }
      updates.forEach(t -> commonTags.put(t.getKey(), t));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Collection<Tag> getCommonTags() {
    lock.lock();
    try {
      return Collections.unmodifiableCollection(commonTags.values());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void addMetricsProducers(MetricsProducer... producer) {
    if (!metricsEnabled) {
      return;
    }
    if (producer.length == 0) {
      LOG.debug(
          "called addMetricsProducers() without providing at least one producer - this has no effect");
      return;
    }
    lock.lock();
    try {
      if (meterRegistry == null) {
        producers.addAll(Arrays.asList(producer));
      } else {
        Arrays.stream(producer).forEach(p -> p.registerMetrics(meterRegistry));
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void init() {
    if (!metricsEnabled) {
      LOG.info("Metrics not initialized, metrics are disabled.");
      return;
    }
    lock.lock();
    try {
      if (meterRegistry != null) {
        LOG.warn("metrics registry has already been initialized");
        return;
      }

      LOG.info("Metrics initialization. common tags: {}", commonTags);

      boolean jvmMetricsEnabled =
          context.getConfiguration().getBoolean(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED);

      MeterFilter replicationFilter = new MeterFilter() {
        @Override
        public DistributionStatisticConfig configure(Meter.Id id,
            @NonNull DistributionStatisticConfig config) {
          if (id.getName().equals("replicationQueue")) {
            return DistributionStatisticConfig.builder().percentiles(0.5, 0.75, 0.9, 0.95, 0.99)
                .expiry(Duration.ofMinutes(10)).build().merge(config);
          }
          return config;
        }
      };

      // user specified registries
      String userRegistryFactories =
          context.getConfiguration().get(Property.GENERAL_MICROMETER_FACTORY);

      List<MeterRegistry> registries = new ArrayList<>();

      for (String factoryName : getTrimmedStrings(userRegistryFactories)) {
        try {
          MeterRegistry registry = getRegistryFromFactory(factoryName, context);
          registry.config().commonTags(commonTags.values());
          registry.config().meterFilter(replicationFilter);
          registries.add(registry);
        } catch (ReflectiveOperationException ex) {
          LOG.warn("Could not load registry {}", factoryName, ex);
        }
      }

      if (registries.size() == 1) {
        meterRegistry = registries.get(0);
      } else {
        var composite = new CompositeMeterRegistry();
        composite.config().commonTags(commonTags.values());
        registries.forEach(composite::add);
        meterRegistry = composite;
      }

      if (jvmMetricsEnabled) {
        LOG.info("enabling detailed jvm, classloader, jvm gc and process metrics");
        new ClassLoaderMetrics().bindTo(meterRegistry);
        new JvmMemoryMetrics().bindTo(meterRegistry);
        jvmGcMetrics = new JvmGcMetrics();
        jvmGcMetrics.bindTo(meterRegistry);
        new ProcessorMetrics().bindTo(meterRegistry);
        new JvmThreadMetrics().bindTo(meterRegistry);
      }

      LOG.info("Metrics initialization. Register producers: {}", producers);
      producers.forEach(p -> p.registerMetrics(meterRegistry));

      Metrics.globalRegistry.add(meterRegistry);

    } finally {
      lock.unlock();
    }
  }

  // support for org.apache.accumulo.core.metrics.MeterRegistryFactory can be removed in 3.1
  @VisibleForTesting
  @SuppressWarnings("deprecation")
  static MeterRegistry getRegistryFromFactory(final String factoryName, final ServerContext context)
      throws ReflectiveOperationException {
    try {
      LOG.info("look for meter spi registry factory {}", factoryName);
      Class<? extends org.apache.accumulo.core.spi.metrics.MeterRegistryFactory> clazz =
          ClassLoaderUtil.loadClass(factoryName,
              org.apache.accumulo.core.spi.metrics.MeterRegistryFactory.class);
      org.apache.accumulo.core.spi.metrics.MeterRegistryFactory factory =
          clazz.getDeclaredConstructor().newInstance();
      org.apache.accumulo.core.spi.metrics.MeterRegistryFactory.InitParameters initParameters =
          new MeterRegistryEnvPropImpl(context);
      return factory.create(initParameters);
    } catch (ClassCastException ex) {
      // empty. On exception try deprecated version
    }
    try {
      LOG.info("find legacy meter registry factory {}", factoryName);
      Class<? extends org.apache.accumulo.core.metrics.MeterRegistryFactory> clazz = ClassLoaderUtil
          .loadClass(factoryName, org.apache.accumulo.core.metrics.MeterRegistryFactory.class);
      org.apache.accumulo.core.metrics.MeterRegistryFactory factory =
          clazz.getDeclaredConstructor().newInstance();
      return factory.create();
    } catch (ClassCastException ex) {
      // empty. No valid metrics factory, fall through and then throw exception.
    }
    throw new ClassNotFoundException(
        "Could not find appropriate class implementing a MetricsFactory for: " + factoryName);
  }

  @Override
  public synchronized void close() {
    LOG.info("Closing metrics registry");
    if (jvmGcMetrics != null) {
      jvmGcMetrics.close();
      jvmGcMetrics = null;
    }
    if (meterRegistry != null) {
      meterRegistry.close();
      meterRegistry = null;
    }
  }

  @Override
  public String toString() {
    return "MetricsCommonTags{tags=" + getCommonTags() + '}';
  }
}
