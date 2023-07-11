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
package org.apache.accumulo.core.conf.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.yaml.snakeyaml.Yaml;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ClusterConfigParser {

  private static final String PROPERTY_FORMAT = "%s=\"%s\"%n";
  private static final String COMPACTOR_PREFIX = "compactor.";
  private static final String GC_KEY = "gc";
  private static final String MANAGER_KEY = "manager";
  private static final String MONITOR_KEY = "monitor";
  private static final String SSERVER_PREFIX = "sserver.";
  private static final String SSERVERS_PER_HOST_KEY = "sservers_per_host";
  private static final String TSERVER_PREFIX = "tserver.";
  private static final String TSERVERS_PER_HOST_KEY = "tservers_per_host";

  private static final String[] UNGROUPED_SECTIONS =
      new String[] {MANAGER_KEY, MONITOR_KEY, GC_KEY};

  private static final Set<String> VALID_CONFIG_KEYS =
      Set.of(MANAGER_KEY, MONITOR_KEY, GC_KEY, SSERVERS_PER_HOST_KEY, TSERVERS_PER_HOST_KEY);

  private static final Set<String> VALID_CONFIG_PREFIXES =
      Set.of(COMPACTOR_PREFIX, SSERVER_PREFIX, TSERVER_PREFIX);

  private static final Predicate<String> VALID_CONFIG_SECTIONS =
      section -> VALID_CONFIG_KEYS.contains(section)
          || VALID_CONFIG_PREFIXES.stream().anyMatch(section::startsWith);

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
  public static Map<String,String> parseConfiguration(String configFile) throws IOException {
    Map<String,String> results = new HashMap<>();
    try (InputStream fis = Files.newInputStream(Paths.get(configFile), StandardOpenOption.READ)) {
      Yaml y = new Yaml();
      Map<String,Object> config = y.load(fis);
      config.forEach((k, v) -> flatten("", k, v, results));
    }
    return results;
  }

  private static String addTheDot(String key) {
    return key.endsWith(".") ? "" : ".";
  }

  private static void flatten(String parentKey, String key, Object value,
      Map<String,String> results) {
    String parent =
        (parentKey == null || parentKey.equals("")) ? "" : parentKey + addTheDot(parentKey);
    if (value instanceof String) {
      results.put(parent + key, (String) value);
    } else if (value instanceof List) {
      ((List<?>) value).forEach(l -> {
        if (l instanceof String) {
          // remove the [] at the ends of toString()
          String val = value.toString();
          results.put(parent + key, val.substring(1, val.length() - 1).replace(", ", " "));
        } else {
          flatten(parent, key, l, results);
        }
      });
    } else if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String,Object> map = (Map<String,Object>) value;
      map.forEach((k, v) -> flatten(parent + key, k, v, results));
    } else if (value instanceof Number) {
      results.put(parent + key, value.toString());
    } else {
      throw new IllegalStateException("Unhandled object type: " + value.getClass());
    }
  }

  public static void outputShellVariables(Map<String,String> config, PrintStream out) {

    // find invalid config sections and point the user to the first one
    config.keySet().stream().filter(VALID_CONFIG_SECTIONS.negate()).findFirst()
        .ifPresent(section -> {
          throw new IllegalArgumentException("Unknown configuration section : " + section);
        });

    for (String section : UNGROUPED_SECTIONS) {
      if (config.containsKey(section)) {
        out.printf(PROPERTY_FORMAT, section.toUpperCase() + "_HOSTS", config.get(section));
      } else {
        if (section.equals(MANAGER_KEY)) {
          throw new IllegalStateException("Manager is required in the configuration");
        }
        System.err.println("WARN: " + section + " is missing");
      }
    }

    List<String> compactorGroups =
        config.keySet().stream().filter(k -> k.startsWith(COMPACTOR_PREFIX))
            .map(k -> k.substring(COMPACTOR_PREFIX.length())).sorted().collect(Collectors.toList());

    if (!compactorGroups.isEmpty()) {
      out.printf(PROPERTY_FORMAT, "COMPACTOR_GROUPS",
          compactorGroups.stream().collect(Collectors.joining(" ")));
      for (String group : compactorGroups) {
        out.printf(PROPERTY_FORMAT, "COMPACTOR_HOSTS_" + group,
            config.get(COMPACTOR_PREFIX + group));
      }
    }

    List<String> sserverGroups = config.keySet().stream().filter(k -> k.startsWith(SSERVER_PREFIX))
        .map(k -> k.substring(SSERVER_PREFIX.length())).sorted().collect(Collectors.toList());

    if (!sserverGroups.isEmpty()) {
      out.printf(PROPERTY_FORMAT, "SSERVER_GROUPS",
          sserverGroups.stream().collect(Collectors.joining(" ")));
      sserverGroups.forEach(ssg -> out.printf(PROPERTY_FORMAT, "SSERVER_HOSTS_" + ssg,
          config.get(SSERVER_PREFIX + ssg)));
    }

    List<String> tserverGroups = config.keySet().stream().filter(k -> k.startsWith(TSERVER_PREFIX))
        .map(k -> k.substring(TSERVER_PREFIX.length())).sorted().collect(Collectors.toList());

    if (!tserverGroups.isEmpty()) {
      out.printf(PROPERTY_FORMAT, "TSERVER_GROUPS",
          tserverGroups.stream().collect(Collectors.joining(" ")));
      tserverGroups.forEach(tsg -> out.printf(PROPERTY_FORMAT, "TSERVER_HOSTS_" + tsg,
          config.get(TSERVER_PREFIX + tsg)));
    }

    String numTservers = config.getOrDefault("tservers_per_host", "1");
    out.print("NUM_TSERVERS=\"${NUM_TSERVERS:=" + numTservers + "}\"\n");

    String numSservers = config.getOrDefault("sservers_per_host", "1");
    out.print("NUM_SSERVERS=\"${NUM_SSERVERS:=" + numSservers + "}\"\n");

    out.flush();
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "Path provided for output file is intentional")
  public static void main(String[] args) throws IOException {
    if (args == null || args.length < 1 || args.length > 2) {
      System.err.println("Usage: ClusterConfigParser <configFile> [<outputFile>]");
      System.exit(1);
    }

    if (args.length == 2) {
      // Write to a file instead of System.out if provided as an argument
      try (OutputStream os = Files.newOutputStream(Paths.get(args[1]), StandardOpenOption.CREATE);
          PrintStream out = new PrintStream(os)) {
        outputShellVariables(parseConfiguration(args[0]), new PrintStream(out));
      }
    } else {
      outputShellVariables(parseConfiguration(args[0]), System.out);
    }
  }

}
