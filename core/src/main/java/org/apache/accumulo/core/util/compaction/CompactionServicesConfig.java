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
package org.apache.accumulo.core.util.compaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;

import com.google.common.collect.Sets;

/**
 * This class serves to configure compaction services from an {@link AccumuloConfiguration} object.
 *
 * Specifically, compaction service properties (those prefixed by "tserver.compaction.major
 * .service") are used.
 */
public class CompactionServicesConfig {

  private final Map<String,String> planners = new HashMap<>();
  private final Map<String,Long> rateLimits = new HashMap<>();
  private final Map<String,Map<String,String>> options = new HashMap<>();
  long defaultRateLimit;
  private final Consumer<String> deprecationWarningConsumer;

  public static final CompactionServiceId DEFAULT_SERVICE = CompactionServiceId.of("default");

  @SuppressWarnings("removal")
  private long getDefaultThroughput(AccumuloConfiguration aconf) {
    if (aconf.isPropertySet(Property.TSERV_MAJC_THROUGHPUT)) {
      return aconf.getAsBytes(Property.TSERV_MAJC_THROUGHPUT);
    }

    return ConfigurationTypeHelper
        .getMemoryAsBytes(Property.TSERV_COMPACTION_SERVICE_DEFAULT_RATE_LIMIT.getDefaultValue());
  }

  @SuppressWarnings("removal")
  private Map<String,String> getConfiguration(AccumuloConfiguration aconf) {

    Map<String,String> configs =
        aconf.getAllPropertiesWithPrefix(Property.TSERV_COMPACTION_SERVICE_PREFIX);

    // check if deprecated properties for compaction executor are set
    if (aconf.isPropertySet(Property.TSERV_MAJC_MAXCONCURRENT)) {

      String defaultServicePrefix =
          Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + DEFAULT_SERVICE.canonical() + ".";

      // check if any properties for the default compaction service are set
      boolean defaultServicePropsSet = configs.keySet().stream()
          .filter(key -> key.startsWith(defaultServicePrefix)).map(Property::getPropertyByKey)
          .anyMatch(prop -> prop == null || aconf.isPropertySet(prop));

      if (defaultServicePropsSet) {

        String warning = "The deprecated property " + Property.TSERV_MAJC_MAXCONCURRENT.getKey()
            + " was set. Properties with the prefix " + defaultServicePrefix
            + " were also set which replace the deprecated properties. The deprecated property "
            + "was therefore ignored.";

        deprecationWarningConsumer.accept(warning);

      } else {
        String numThreads = aconf.get(Property.TSERV_MAJC_MAXCONCURRENT);

        // Its possible a user has configured the other compaction services, but not the default
        // service. In this case want to produce a config with the default service configs
        // overridden using deprecated configs.

        HashMap<String,String> configsCopy = new HashMap<>(configs);

        Map<String,String> defaultServiceConfigs =
            Map.of(defaultServicePrefix + "planner", DefaultCompactionPlanner.class.getName(),
                defaultServicePrefix + "planner.opts.executors",
                "[{'name':'deprecated', 'numThreads':" + numThreads + "}]");

        configsCopy.putAll(defaultServiceConfigs);

        String warning = "The deprecated property " + Property.TSERV_MAJC_MAXCONCURRENT.getKey()
            + " was set. Properties with the prefix " + defaultServicePrefix
            + " were not set, these should replace the deprecated properties. The old properties "
            + "were automatically mapped to the new properties in process creating : "
            + defaultServiceConfigs + ".";

        deprecationWarningConsumer.accept(warning);

        configs = Map.copyOf(configsCopy);
      }
    }

    return configs;

  }

  public CompactionServicesConfig(AccumuloConfiguration aconf,
      Consumer<String> deprecationWarningConsumer) {
    this.deprecationWarningConsumer = deprecationWarningConsumer;
    Map<String,String> configs = getConfiguration(aconf);

    configs.forEach((prop, val) -> {

      var suffix = prop.substring(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey().length());
      String[] tokens = suffix.split("\\.");
      if (tokens.length == 4 && tokens[1].equals("planner") && tokens[2].equals("opts")) {
        options.computeIfAbsent(tokens[0], k -> new HashMap<>()).put(tokens[3], val);
      } else if (tokens.length == 2 && tokens[1].equals("planner")) {
        planners.put(tokens[0], val);
      } else if (tokens.length == 3 && tokens[1].equals("rate") && tokens[2].equals("limit")) {
        var eprop = Property.getPropertyByKey(prop);
        if (eprop == null || aconf.isPropertySet(eprop) || !isDeprecatedThroughputSet(aconf)) {
          rateLimits.put(tokens[0], ConfigurationTypeHelper.getFixedMemoryAsBytes(val));
        }
      } else {
        throw new IllegalArgumentException("Malformed compaction service property " + prop);
      }
    });

    defaultRateLimit = getDefaultThroughput(aconf);

    var diff = Sets.difference(options.keySet(), planners.keySet());

    if (!diff.isEmpty()) {
      throw new IllegalArgumentException(
          "Incomplete compaction service definitions, missing planner class " + diff);
    }

  }

  @SuppressWarnings("removal")
  private boolean isDeprecatedThroughputSet(AccumuloConfiguration aconf) {
    return aconf.isPropertySet(Property.TSERV_MAJC_THROUGHPUT);
  }

  public long getRateLimit(String serviceName) {
    return getRateLimits().getOrDefault(serviceName, defaultRateLimit);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CompactionServicesConfig) {
      var oc = (CompactionServicesConfig) o;
      return getPlanners().equals(oc.getPlanners()) && getOptions().equals(oc.getOptions())
          && getRateLimits().equals(oc.getRateLimits());
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPlanners(), getOptions(), getRateLimits());
  }

  public Map<String,String> getPlanners() {
    return planners;
  }

  public Map<String,Long> getRateLimits() {
    return rateLimits;
  }

  public Map<String,Map<String,String>> getOptions() {
    return options;
  }
}
