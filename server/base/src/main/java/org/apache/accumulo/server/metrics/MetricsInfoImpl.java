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

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.util.StringUtils.getTrimmedStrings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.metrics.MeterRegistryFactory;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.commons.lang3.StringUtils;
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
import io.micrometer.core.instrument.binder.logging.Log4j2Metrics;
import io.micrometer.core.instrument.binder.logging.LogbackMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.config.MeterFilter;

public class MetricsInfoImpl implements MetricsInfo {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsInfoImpl.class);

  private final ServerContext context;

  private List<Tag> commonTags = null;

  // JvmGcMetrics are declared with AutoCloseable - keep reference to use with close()
  private JvmGcMetrics jvmGcMetrics;
  // Log4j2Metrics and LogbackMetrics are declared with AutoCloseable - keep reference to use with
  // close()
  private AutoCloseable logMetrics;

  private final boolean metricsEnabled;

  private final List<MetricsProducer> producers = new ArrayList<>();

  public MetricsInfoImpl(final ServerContext context) {
    this.context = context;
    metricsEnabled = context.getConfiguration().getBoolean(Property.GENERAL_MICROMETER_ENABLED);
    printMetricsConfig();
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

  @Override
  public synchronized void addMetricsProducers(MetricsProducer... producer) {
    if (!metricsEnabled) {
      return;
    }
    if (producer.length == 0) {
      LOG.debug(
          "called addMetricsProducers() without providing at least one producer - this has no effect");
      return;
    }

    if (commonTags == null) {
      producers.addAll(Arrays.asList(producer));
    } else {
      Arrays.stream(producer).forEach(p -> p.registerMetrics(Metrics.globalRegistry));
    }
  }

  @Override
  public synchronized void init(Collection<Tag> tags) {
    Objects.requireNonNull(tags);

    if (!metricsEnabled) {
      ThreadPools.getServerThreadPools().disableThreadPoolMetrics();
      LOG.info("Metrics not initialized, metrics are disabled.");
      return;
    }

    if (commonTags != null) {
      LOG.warn("metrics registry has already been initialized");
      return;
    }

    var userTags = context.getConfiguration().get(Property.GENERAL_MICROMETER_USER_TAGS);
    if (!userTags.isEmpty()) {
      tags = new ArrayList<>(tags);
      String[] userTagList = userTags.split(",");
      for (String userTag : userTagList) {
        String[] tagParts = userTag.split("=");
        if (tagParts.length == 2) {
          Tag tag = Tag.of(tagParts[0], tagParts[1]);
          tags.add(tag);
        } else {
          LOG.warn("Malformed user metric tag: {} in property {}", userTag,
              Property.GENERAL_MICROMETER_USER_TAGS.getKey());
        }
      }
    }

    commonTags = List.copyOf(tags);

    LOG.info("Metrics initialization. common tags: {}", commonTags);

    Metrics.globalRegistry.config().commonTags(commonTags);

    boolean jvmMetricsEnabled =
        context.getConfiguration().getBoolean(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED);

    // user specified registries
    String userRegistryFactories =
        context.getConfiguration().get(Property.GENERAL_MICROMETER_FACTORY);

    // Fetch the patterns to filter from the meter registry.
    String filterPatterns = context.getConfiguration().get(Property.GENERAL_MICROMETER_ID_FILTERS);
    MeterFilter meterFilter = null;
    if (StringUtils.isNotEmpty(filterPatterns)) {
      meterFilter = getMeterFilter(filterPatterns);
    }

    for (String factoryName : getTrimmedStrings(userRegistryFactories)) {
      try {
        MeterRegistry registry = getRegistryFromFactory(factoryName, context);
        registry.config().commonTags(commonTags);
        if (meterFilter != null) {
          registry.config().meterFilter(meterFilter);
        }
        Metrics.globalRegistry.add(registry);
      } catch (ReflectiveOperationException ex) {
        LOG.warn("Could not load registry {}", factoryName, ex);
      }
    }

    // Set the MeterRegistry on the ThreadPools
    ThreadPools.getServerThreadPools().setMeterRegistry(Metrics.globalRegistry);

    if (jvmMetricsEnabled) {
      LOG.info("enabling detailed jvm, classloader, jvm gc and process metrics");
      new ClassLoaderMetrics().bindTo(Metrics.globalRegistry);
      new JvmMemoryMetrics().bindTo(Metrics.globalRegistry);
      jvmGcMetrics = new JvmGcMetrics();
      jvmGcMetrics.bindTo(Metrics.globalRegistry);
      new ProcessorMetrics().bindTo(Metrics.globalRegistry);
      new JvmThreadMetrics().bindTo(Metrics.globalRegistry);
    }

    String loggingMetrics = context.getConfiguration().get(Property.GENERAL_MICROMETER_LOG_METRICS);
    switch (loggingMetrics) {
      case "none":
        LOG.info("Log metrics are disabled.");
        break;
      case "log4j2":
        Log4j2Metrics l2m = new Log4j2Metrics();
        l2m.bindTo(Metrics.globalRegistry);
        logMetrics = l2m;
        break;
      case "logback":
        LogbackMetrics lb = new LogbackMetrics();
        lb.bindTo(Metrics.globalRegistry);
        logMetrics = lb;
        break;
      default:
        LOG.info("Log metrics misconfigured, valid values for {} are 'none', 'log4j2' or 'logback'",
            Property.GENERAL_MICROMETER_LOG_METRICS.getKey());
    }

    LOG.info("Metrics initialization. Register producers: {}", producers);
    producers.forEach(p -> p.registerMetrics(Metrics.globalRegistry));
  }

  @VisibleForTesting
  static MeterRegistry getRegistryFromFactory(final String factoryName, final ServerContext context)
      throws ReflectiveOperationException {
    try {
      LOG.info("look for meter spi registry factory {}", factoryName);
      Class<? extends MeterRegistryFactory> clazz =
          ClassLoaderUtil.loadClass(factoryName, MeterRegistryFactory.class);
      MeterRegistryFactory factory = clazz.getDeclaredConstructor().newInstance();
      MeterRegistryFactory.InitParameters initParameters = new MeterRegistryEnvPropImpl(context);
      return factory.create(initParameters);
    } catch (ClassCastException ex) {
      // empty. On exception try deprecated version
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

    if (logMetrics != null) {
      try {
        logMetrics.close();
      } catch (Exception e) {
        LOG.info("Exception when closing log metrics", e);
      }
    }

    Metrics.globalRegistry.close();
  }

  /**
   * This function uses patterns specified in the patternList parameter to filter out specific
   * metrics that the user doesn't want.
   *
   * @param patternList, a comma-delimited String of regext patterns that getMeterFilter uses to
   *        filter metrics.
   * @return a predicate with the type of MeterFilter, that describes which metrics to filter.
   */
  public static MeterFilter getMeterFilter(String patternList) {
    requireNonNull(patternList, "patternList must not be null");

    // Trims whitespace and all other non-visible characters.
    patternList = patternList.replaceAll("\\s+", "");

    String[] patterns = patternList.split(",");
    Predicate<Meter.Id> finalPredicate = id -> false;

    for (String pattern : patterns) {
      // Compile the pattern.
      // Will throw PatternSyntaxException if invalid pattern.
      Pattern compiledPattern = Pattern.compile(pattern);
      // Create a predicate that will return true if the ID's name matches the pattern.
      Predicate<Meter.Id> predicate = id -> compiledPattern.matcher(id.getName()).matches();
      // Conjoin the pattern into the final predicates. The final predicate will return true if
      // the name of the ID matches any of its conjoined predicates.
      finalPredicate = finalPredicate.or(predicate);
    }

    // Assert that meter filter reply == MeterFilterReply.DENY;
    return MeterFilter.deny(finalPredicate);
  }

  @Override
  public synchronized String toString() {
    return "MetricsCommonTags{tags=" + commonTags + '}';
  }
}
