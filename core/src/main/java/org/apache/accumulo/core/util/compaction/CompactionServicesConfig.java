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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * This class serves to configure compaction services from an {@link AccumuloConfiguration} object.
 *
 * Specifically, compaction service properties (those prefixed by "tserver.compaction.major
 * .service" or "compaction.major.service") are used.
 */
@SuppressWarnings("deprecation")
public class CompactionServicesConfig {

  private static final Logger log = LoggerFactory.getLogger(CompactionServicesConfig.class);
  private final Map<String,String> planners = new HashMap<>();
  private final Map<String,Long> rateLimits = new HashMap<>();
  private final Map<String,Map<String,String>> options = new HashMap<>();
  private final Property oldPrefix = Property.TSERV_COMPACTION_SERVICE_PREFIX;
  private final Property newPrefix = Property.COMPACTION_SERVICE_PREFIX;
  long defaultRateLimit;

  public static final CompactionServiceId DEFAULT_SERVICE = CompactionServiceId.of("default");

  @SuppressWarnings("removal")
  private long getDefaultThroughput() {
    return ConfigurationTypeHelper
        .getMemoryAsBytes(Property.COMPACTION_SERVICE_DEFAULT_RATE_LIMIT.getDefaultValue());
  }

  private Map<String,Map<String,String>> getConfiguration(AccumuloConfiguration aconf) {
    Map<String,Map<String,String>> properties = new HashMap<>();
    properties.put(newPrefix.getKey(), aconf.getAllPropertiesWithPrefixStripped(newPrefix));

    Map<String,String> oldProps = new HashMap<>();
    for (Map.Entry<String,String> entry : aconf.getAllPropertiesWithPrefixStripped(oldPrefix)
        .entrySet()) {
      // Discard duplicate property definitions
      if (properties.get(newPrefix.getKey()).containsKey(entry.getKey())) {
        log.warn("Duplicate property exists for compaction planner: '{}'. "
            + "Using the value of property '{}'", entry.getKey(), newPrefix + entry.getKey());
      } else {
        oldProps.put(entry.getKey(), entry.getValue());
      }
    }
    properties.put(oldPrefix.getKey(), oldProps);
    // Return unmodifiable map
    return Map.copyOf(properties);
  }

  public CompactionServicesConfig(AccumuloConfiguration aconf) {
    Map<String,Map<String,String>> configs = getConfiguration(aconf);

    Map<String,String> validOpts = new HashMap<>();

    // Find compaction planner defs first.
    configs.forEach((prefix, props) -> {
      props.forEach((prop, val) -> {
        String[] tokens = prop.split("\\.");
        if (tokens.length == 2 && tokens[1].equals("planner")) {
          if (prefix.equals(oldPrefix.getKey())) {
            log.warn(
                "Found compaction planner '{}' using a deprecated prefix. Please update property to use the '{}' prefix",
                tokens[0], newPrefix);
          }
          validOpts.put(tokens[0], prefix);
          planners.put(tokens[0], val);
        }
      });
    });

    // Now find all compaction planner options.
    configs.forEach((prefix, props) -> {
      props.forEach((prop, val) -> {
        String[] tokens = prop.split("\\.");
        if (!validOpts.containsKey(tokens[0])) {
          throw new IllegalArgumentException(
              "Incomplete compaction service definition, missing planner class" + prop);
        }
        // Only add the compaction options if defined with the new prefix
        // or match the planner's defined prefix.
        if (prefix.equals(newPrefix.getKey()) || validOpts.get(tokens[0]).equals(prefix)) {
          if (tokens.length == 4 && tokens[1].equals("planner") && tokens[2].equals("opts")) {
            options.computeIfAbsent(tokens[0], k -> new HashMap<>()).put(tokens[3], val);
          } else if (tokens.length == 3 && tokens[1].equals("rate") && tokens[2].equals("limit")) {
            var eprop = Property.getPropertyByKey(prop);
            if (eprop == null || aconf.isPropertySet(eprop)) {
              rateLimits.put(tokens[0], ConfigurationTypeHelper.getFixedMemoryAsBytes(val));
            }
          } else if (!(tokens.length == 2 && tokens[1].equals("planner"))) {
            throw new IllegalArgumentException(
                "Malformed compaction service property " + prefix + prop);
          }
        } else {
          log.warn(
              "Ignoring compaction property {}, property either uses a deprecated prefix"
                  + " or does not match the prefix used by the referenced planner definition",
              prop);
        }
      });
    });
    defaultRateLimit = getDefaultThroughput();

    var diff = Sets.difference(options.keySet(), planners.keySet());

    if (!diff.isEmpty()) {
      throw new IllegalArgumentException(
          "Incomplete compaction service definitions, missing planner class " + diff);
    }

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
