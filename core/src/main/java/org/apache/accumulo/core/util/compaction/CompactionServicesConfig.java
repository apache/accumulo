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

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;

import com.google.common.collect.Sets;

/**
 * This class serves to configure compaction services from an {@link AccumuloConfiguration} object.
 * Specifically, compaction service properties (those prefixed by "compaction.service") are used.
 */
public class CompactionServicesConfig {

  private final Map<String,String> planners = new HashMap<>();
  private final Map<String,String> plannerPrefixes = new HashMap<>();
  private final Map<String,Map<String,String>> options = new HashMap<>();

  private static final Property prefix = Property.COMPACTION_SERVICE_PREFIX;

  private interface ConfigIndirection {
    Map<String,String> getAllPropertiesWithPrefixStripped(Property p);
  }

  private static Map<String,Map<String,String>> getConfiguration(ConfigIndirection aconf) {
    Map<String,Map<String,String>> properties = new HashMap<>();

    var newProps = aconf.getAllPropertiesWithPrefixStripped(prefix);
    properties.put(prefix.getKey(), newProps);
    // Return unmodifiable map
    return Map.copyOf(properties);
  }

  public CompactionServicesConfig(PluginEnvironment.Configuration conf) {
    this(getConfiguration(prefix -> {
      var props = conf.getWithPrefix(prefix.getKey());
      Map<String,String> stripped = new HashMap<>();
      props.forEach((k, v) -> stripped.put(k.substring(prefix.getKey().length()), v));
      return stripped;
    }));
  }

  public CompactionServicesConfig(AccumuloConfiguration aconf) {
    this(getConfiguration(aconf::getAllPropertiesWithPrefixStripped));
  }

  private CompactionServicesConfig(Map<String,Map<String,String>> configs) {
    configs.forEach((prefix, props) -> {
      props.forEach((prop, val) -> {
        String[] tokens = prop.split("\\.");
        if (tokens.length == 2 && tokens[1].equals("planner")) {
          plannerPrefixes.put(tokens[0], prefix);
          planners.put(tokens[0], val);
        } else if (tokens.length == 4 && tokens[1].equals("planner") && tokens[2].equals("opts")) {
          options.computeIfAbsent(tokens[0], k -> new HashMap<>()).put(tokens[3], val);
        } else {
          throw new IllegalArgumentException(
              "Malformed compaction service property " + prefix + prop);
        }
      });
    });
    var diff = Sets.difference(options.keySet(), planners.keySet());

    if (!diff.isEmpty()) {
      throw new IllegalArgumentException(
          "Incomplete compaction service definitions, missing planner class " + diff);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CompactionServicesConfig) {
      var oc = (CompactionServicesConfig) o;
      return getPlanners().equals(oc.getPlanners()) && getOptions().equals(oc.getOptions());
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPlanners(), getOptions());
  }

  public Map<String,String> getPlanners() {
    return planners;
  }

  public String getPlannerPrefix(String service) {
    return plannerPrefixes.get(service);
  }

  public Map<String,Map<String,String>> getOptions() {
    return options;
  }
}
