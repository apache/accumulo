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
package org.apache.accumulo.core.summary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration.Builder;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.thrift.TSummarizerConfiguration;

public class SummarizerConfigurationUtil {

  public static Map<String,String> toTablePropertiesMap(List<SummarizerConfiguration> summarizers) {
    if (summarizers.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String,String> props = new HashMap<>();

    for (SummarizerConfiguration sconf : summarizers) {
      String cid = sconf.getPropertyId();
      String prefix = Property.TABLE_SUMMARIZER_PREFIX.getKey() + cid;

      if (props.containsKey(prefix)) {
        throw new IllegalArgumentException("Duplicate summarizer config id : " + cid);
      }

      props.put(prefix, sconf.getClassName());
      Set<Entry<String,String>> es = sconf.getOptions().entrySet();
      StringBuilder sb = new StringBuilder(prefix + ".opt.");
      int resetLen = sb.length();
      for (Entry<String,String> entry : es) {
        sb.append(entry.getKey());
        props.put(sb.toString(), entry.getValue());
        sb.setLength(resetLen);
      }
    }

    return props;
  }

  public static List<SummarizerConfiguration>
      getSummarizerConfigs(Iterable<Entry<String,String>> props) {
    TreeMap<String,String> filteredMap = new TreeMap<>();
    for (Entry<String,String> entry : props) {
      if (entry.getKey().startsWith(Property.TABLE_SUMMARIZER_PREFIX.getKey())) {
        filteredMap.put(entry.getKey(), entry.getValue());
      }
    }

    return getSummarizerConfigsFiltered(filteredMap);
  }

  public static List<SummarizerConfiguration> getSummarizerConfigs(AccumuloConfiguration aconf) {
    Map<String,String> sprops = aconf.getAllPropertiesWithPrefix(Property.TABLE_SUMMARIZER_PREFIX);
    return getSummarizerConfigsFiltered(new TreeMap<>(sprops));
  }

  private static List<SummarizerConfiguration>
      getSummarizerConfigsFiltered(SortedMap<String,String> sprops) {
    if (sprops.isEmpty()) {
      return Collections.emptyList();
    }

    SummarizerConfiguration.Builder builder = null;

    List<SummarizerConfiguration> configs = new ArrayList<>();

    final int preLen = Property.TABLE_SUMMARIZER_PREFIX.getKey().length();
    for (Entry<String,String> entry : sprops.entrySet()) {
      String k = entry.getKey().substring(preLen);

      String[] tokens = k.split("\\.");

      String id = tokens[0];
      if (tokens.length == 1) {
        if (builder != null) {
          configs.add(builder.build());
        }

        builder = SummarizerConfiguration.builder(entry.getValue()).setPropertyId(id);

      } else if (tokens.length == 3 || tokens[1].equals("opt")) {
        builder.addOption(tokens[2], entry.getValue());
      } else {
        throw new IllegalArgumentException("Unable to parse summarizer property : " + k);
      }
    }

    configs.add(builder.build());

    return configs;
  }

  public static TSummarizerConfiguration toThrift(SummarizerConfiguration sc) {
    return new TSummarizerConfiguration(sc.getClassName(), sc.getOptions(), sc.getPropertyId());
  }

  public static SummarizerConfiguration fromThrift(TSummarizerConfiguration config) {
    Builder builder = SummarizerConfiguration.builder(config.getClassname());
    builder.setPropertyId(config.getConfigId());
    builder.addOptions(config.getOptions());
    return builder.build();
  }
}
