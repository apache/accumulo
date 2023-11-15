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

  public static final CompactionServiceId DEFAULT_SERVICE = CompactionServiceId.of("default");

  @SuppressWarnings("removal")
  private long getDefaultThroughput() {
    return ConfigurationTypeHelper
        .getMemoryAsBytes(Property.TSERV_COMPACTION_SERVICE_DEFAULT_RATE_LIMIT.getDefaultValue());
  }

  private Map<String,String> getConfiguration(AccumuloConfiguration aconf) {
    return aconf.getAllPropertiesWithPrefix(Property.TSERV_COMPACTION_SERVICE_PREFIX);
  }

  public CompactionServicesConfig(AccumuloConfiguration aconf) {
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
        if (eprop == null || aconf.isPropertySet(eprop)) {
          rateLimits.put(tokens[0], ConfigurationTypeHelper.getFixedMemoryAsBytes(val));
        }
      } else {
        throw new IllegalArgumentException("Malformed compaction service property " + prop);
      }
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
