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
package org.apache.accumulo.core.util;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpectedProcessCounts {

  private static final Logger log = LoggerFactory.getLogger(ExpectedProcessCounts.class);

  private static final Map<String,ServerId.Type> SUPPORTED_TYPES =
      Map.of("compactor", ServerId.Type.COMPACTOR, "sserver", ServerId.Type.SCAN_SERVER);

  private final Map<ServerId.Type,Map<ResourceGroupId,Integer>> counts;

  private ExpectedProcessCounts(Map<ServerId.Type,Map<ResourceGroupId,Integer>> counts) {
    this.counts = counts;
  }

  public static ExpectedProcessCounts parse(String propertyValue) {
    Map<ServerId.Type,Map<ResourceGroupId,Integer>> result = new EnumMap<>(ServerId.Type.class);

    if (propertyValue == null || propertyValue.isBlank()) {
      return new ExpectedProcessCounts(result);
    }

    for (String entry : propertyValue.split(",")) {
      entry = entry.trim();
      if (entry.isEmpty()) {
        continue;
      }

      int eqIdx = entry.lastIndexOf('=');
      if (eqIdx < 0) {
        log.warn("Ignoring malformed entry in {} (missing '='): {}",
            "general.expected.process.counts", entry);
        continue;
      }

      String key = entry.substring(0, eqIdx).trim();
      String valueStr = entry.substring(eqIdx + 1).trim();

      int dotIdx = key.indexOf('.');
      if (dotIdx < 0) {
        log.warn("Ignoring malformed key in {} (expected '<type>.<resourceGroup>'): {}",
            "general.expected.process.counts", key);
        continue;
      }

      String typeName = key.substring(0, dotIdx).trim().toLowerCase();
      String groupName = key.substring(dotIdx + 1).trim();

      ServerId.Type serverType = SUPPORTED_TYPES.get(typeName);
      if (serverType == null) {
        log.warn("Ignoring unknown server type '{}' in general.expected.process.counts."
            + " Supported types: {}", typeName, SUPPORTED_TYPES.keySet());
        continue;
      }

      if (groupName.isEmpty()) {
        log.warn("Ignoring entry with empty resource group name in"
            + " general.expected.process.counts: {}", entry);
        continue;
      }

      int count;
      try {
        count = Integer.parseInt(valueStr);
        if (count < 0) {
          throw new NumberFormatException("count must be non-negative");
        }
      } catch (NumberFormatException e) {
        log.warn("Ignoring entry with invalid count '{}' in general.expected.process.counts: {}",
            valueStr, entry);
        continue;
      }

      result.computeIfAbsent(serverType, t -> new HashMap<>()).put(ResourceGroupId.of(groupName),
          count);
    }

    return new ExpectedProcessCounts(Collections.unmodifiableMap(result));
  }

  public OptionalInt getExpectedCount(ServerId.Type type, ResourceGroupId group) {
    var groupMap = counts.get(type);
    if (groupMap == null) {
      return OptionalInt.empty();
    }
    Integer count = groupMap.get(group);
    return count == null ? OptionalInt.empty() : OptionalInt.of(count);
  }

  public Map<ServerId.Type,Map<ResourceGroupId,Integer>> all() {
    return counts;
  }

  public boolean isEmpty() {
    return counts.isEmpty();
  }
}
