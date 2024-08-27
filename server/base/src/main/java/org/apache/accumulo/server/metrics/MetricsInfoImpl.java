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

  private CompositeMeterRegistry composite = null;
  private final List<MeterRegistry> pendingRegistries = new ArrayList<>();

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
    if (jvmMetricsEnabled) {
      if (metricsEnabled) {
        LOG.info("detailed jvm metrics enabled: {}", jvmMetricsEnabled);
      } else {
        LOG.info("requested jvm metrics, but micrometer metrics are disabled.");
      }
    }
    if (metricsEnabled) {
      LOG.info("metrics registry factories: {}",
          context.getConfiguration().get(Property.GENERAL_MICROMETER_FACTORY));
    }
  }

  @Override
  public boolean isMetricsEnabled() {
    return metricsEnabled;
  }

  /**
   * Common tags for all services.
   */
  @Override
  public void addServiceTags(final String applicationName, final HostAndPort hostAndPort) {
    List<Tag> tags = new ArrayList<>();

    if (applicationName != null && !applicationName.isEmpty()) {
      tags.add(MetricsInfo.processTag(applicationName));
    }
    if (hostAndPort != null) {
      tags.addAll(MetricsInfo.addressTags(hostAndPort));
    }
    addCommonTags(tags);
  }

  @Override
  public void addCommonTags(List<Tag> updates) {
    lock.lock();
    try {
      if (composite != null) {
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
  public void addRegistry(MeterRegistry registry) {
    lock.lock();
    try {
      if (composite != null) {
        composite.add(registry);
      } else {
        // defer until composite is initialized
        pendingRegistries.add(registry);
      }

    } finally {
      lock.unlock();
    }
  }

  @Override
  public void addMetricsProducers(MetricsProducer... producer) {
    if (producer.length == 0) {
      LOG.debug(
          "called addMetricsProducers() without providing at least one producer - this has no effect");
      return;
    }
    lock.lock();
    try {
      if (composite == null) {
        producers.addAll(Arrays.asList(producer));
      } else {
        Arrays.stream(producer).forEach(p -> p.registerMetrics(composite));
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public MeterRegistry getRegistry() {
    lock.lock();
    try {
      if (composite == null) {
        throw new IllegalStateException("metrics have not been initialized, call init() first");
      }
    } finally {
      lock.unlock();
    }
    return composite;
  }

  @Override
  public void init() {
    lock.lock();
    try {
      if (composite != null) {
        LOG.warn("metrics registry has already been initialized");
        return;
      }
      composite = new CompositeMeterRegistry();
      composite.config().commonTags(commonTags.values());

      LOG.info("Metrics initialization. common tags: {}", commonTags);

      boolean jvmMetricsEnabled =
          context.getConfiguration().getBoolean(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED);

      if (jvmMetricsEnabled) {
        LOG.info("enabling detailed jvm, classloader, jvm gc and process metrics");
        new ClassLoaderMetrics().bindTo(composite);
        new JvmMemoryMetrics().bindTo(composite);
        jvmGcMetrics = new JvmGcMetrics();
        jvmGcMetrics.bindTo(composite);
        new ProcessorMetrics().bindTo(composite);
        new JvmThreadMetrics().bindTo(composite);
      }

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

      if (isMetricsEnabled()) {
        // user specified registries
        String userRegistryFactories =
            context.getConfiguration().get(Property.GENERAL_MICROMETER_FACTORY);

        for (String factoryName : getTrimmedStrings(userRegistryFactories)) {
          try {
            MeterRegistry registry = getRegistryFromFactory(factoryName, context);
            registry.config().commonTags(commonTags.values());
            registry.config().meterFilter(replicationFilter);
            addRegistry(registry);
          } catch (ReflectiveOperationException ex) {
            LOG.warn("Could not load registry {}", factoryName, ex);
          }
        }
      }

      pendingRegistries.forEach(registry -> composite.add(registry));

      LOG.info("Metrics initialization. Register producers: {}", producers);
      producers.forEach(p -> p.registerMetrics(composite));

      Metrics.globalRegistry.add(composite);

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
    if (composite != null) {
      composite.close();
      composite = null;
    }
  }

  @Override
  public String toString() {
    return "MetricsCommonTags{tags=" + getCommonTags() + '}';
  }
}
