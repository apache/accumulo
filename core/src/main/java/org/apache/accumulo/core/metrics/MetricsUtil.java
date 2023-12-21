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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

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
  private static Pattern camelCasePattern = Pattern.compile("[a-z][A-Z][a-z]");

  public static void initializeMetrics(final AccumuloConfiguration conf, final String appName,
      final HostAndPort address, final String instanceName, final String resourceGroup)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException,
      IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
      SecurityException {
    initializeMetrics(conf.getBoolean(Property.GENERAL_MICROMETER_ENABLED),
        conf.getBoolean(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED),
        conf.get(Property.GENERAL_MICROMETER_FACTORY), appName, address, instanceName,
        resourceGroup, conf.get(Property.GENERAL_MICROMETER_USER_TAGS));
  }

  private static void initializeMetrics(boolean enabled, boolean jvmMetricsEnabled,
      String factoryClass, String appName, HostAndPort address, String instanceName,
      String resourceGroup, String userTags) throws ClassNotFoundException, InstantiationException,
      IllegalAccessException, IllegalArgumentException, InvocationTargetException,
      NoSuchMethodException, SecurityException {

    LOG.info("initializing metrics, enabled:{}, class:{}", enabled, factoryClass);

    if (enabled && factoryClass != null && !factoryClass.isEmpty()) {

      String processName = appName;
      String serviceInstance = System.getProperty("accumulo.metrics.service.instance", "");
      if (!serviceInstance.isBlank()) {
        processName += serviceInstance;
      }

      List<Tag> tags = new ArrayList<>();
      tags.add(Tag.of("instance.name", instanceName));
      tags.add(Tag.of("process.name", processName));
      tags.add(Tag.of("resource.group", resourceGroup));
      if (address != null) {
        if (!address.getHost().isEmpty()) {
          tags.add(Tag.of("host", address.getHost()));
        }
        if (address.getPort() > 0) {
          tags.add(Tag.of("port", Integer.toString(address.getPort())));
        }
      }

      if (!userTags.isEmpty()) {
        String[] userTagList = userTags.split(",");
        for (String userTag : userTagList) {
          String[] tagParts = userTag.split("=");
          if (tagParts.length == 2) {
            tags.add(Tag.of(tagParts[0], tagParts[1]));
          } else {
            LOG.warn("Malformed user metric tag: {} in property {}", userTag,
                Property.GENERAL_MICROMETER_USER_TAGS.getKey());
          }
        }
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
      LOG.info("Metric producer {} initialized", p.getClass().getSimpleName());
    }
  }

  public static void addExecutorServiceMetrics(ExecutorService executor, String name) {
    new ExecutorServiceMetrics(executor, name, commonTags).bindTo(Metrics.globalRegistry);
  }

  public static List<Tag> getCommonTags() {
    return commonTags;
  }

  /**
   * Centralize any specific string formatting for metric names and/or tags. Ensure strings match
   * the micrometer naming convention.
   */
  public static String formatString(String name) {

    // Handle spaces
    name = name.replace(" ", ".");
    // Handle snake_case notation
    name = name.replace("_", ".");
    // Handle Hyphens
    name = name.replace("-", ".");

    // Handle camelCase notation
    Matcher matcher = camelCasePattern.matcher(name);
    StringBuilder output = new StringBuilder(name);
    int insertCount = 0;
    while (matcher.find()) {
      // Pattern matches at a lowercase letter, but the insert is at the second position.
      output.insert(matcher.start() + 1 + insertCount, ".");
      // The correct index position will shift as inserts occur.
      insertCount++;
    }
    name = output.toString();
    return name.toLowerCase();
  }

  public static void close() {
    if (gc != null) {
      gc.close();
    }
  }

}
