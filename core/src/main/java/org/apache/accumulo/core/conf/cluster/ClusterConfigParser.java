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

import static org.apache.accumulo.core.Constants.DEFAULT_RESOURCE_GROUP_NAME;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ClusterConfigParser {

  private static final Pattern GROUP_NAME_PATTERN =
      Pattern.compile("^[a-zA-Z_]{1,}[a-zA-Z0-9_]{0,}$");

  public static void validateGroupNames(List<String> names) {
    for (String name : names) {
      if (!GROUP_NAME_PATTERN.matcher(name).matches()) {
        throw new RuntimeException("Group name: " + name + " contains invalid characters");
      }
    }
  }

  private static final String PROPERTY_FORMAT = "%s=\"%s\"%n";

  private static final String COMPACTOR_PREFIX = "compactor.";
  private static final String GC_PREFIX = "gc.";
  private static final String MANAGER_PREFIX = "manager.";
  private static final String MONITOR_PREFIX = "monitor.";
  private static final String SSERVER_PREFIX = "sserver.";
  private static final String TSERVER_PREFIX = "tserver.";

  private static final String HOSTS_SUFFIX = ".hosts";
  private static final String SERVERS_PER_HOST_SUFFIX = ".servers_per_host";
  private static final String CONF_DIR_SUFFIX = ".conf_dir";
  private static final String EXTRA_ARGS_SUFFIX = ".extra_args";

  private static final Set<String> VALID_SECTIONS = Set.of(MANAGER_PREFIX, GC_PREFIX,
      MONITOR_PREFIX, COMPACTOR_PREFIX, SSERVER_PREFIX, TSERVER_PREFIX);

  private static final Predicate<String> VALID_CONFIG_SECTIONS =
      section -> VALID_SECTIONS.stream().anyMatch(section::startsWith);

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
  public static Map<String,String> parseConfiguration(String configFile) throws IOException {
    Map<String,String> results = new TreeMap<>();
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
      throw new IllegalStateException(
          "Unhandled object type: " + ((value == null) ? "null" : value.getClass()));
    }
  }

  private static List<String> parseGroup(Map<String,String> config, String prefix) {
    Preconditions.checkArgument(prefix.equals(COMPACTOR_PREFIX) || prefix.equals(SSERVER_PREFIX)
        || prefix.equals(TSERVER_PREFIX));
    List<String> groups = config.keySet().stream().filter(k -> k.startsWith(prefix)).map(k -> {
      int periods = StringUtils.countMatches(k, '.');
      if (periods == 1) {
        return k.substring(prefix.length());
      } else if (periods == 2) {
        if (k.endsWith(HOSTS_SUFFIX)) {
          return k.substring(prefix.length(), k.indexOf(HOSTS_SUFFIX));
        } else if (k.endsWith(SERVERS_PER_HOST_SUFFIX)) {
          return k.substring(prefix.length(), k.indexOf(SERVERS_PER_HOST_SUFFIX));
        } else if (k.endsWith(CONF_DIR_SUFFIX)) {
          return k.substring(prefix.length(), k.indexOf(CONF_DIR_SUFFIX));
        } else if (k.endsWith(EXTRA_ARGS_SUFFIX)) {
          return k.substring(prefix.length(), k.indexOf(EXTRA_ARGS_SUFFIX));
        } else {
          throw new IllegalArgumentException("Unknown group suffix for: " + k + ". Only "
              + HOSTS_SUFFIX + ", " + SERVERS_PER_HOST_SUFFIX + ", " + CONF_DIR_SUFFIX + " and "
              + EXTRA_ARGS_SUFFIX + " are allowed.");
        }
      } else {
        throw new IllegalArgumentException("Malformed configuration, has too many levels: " + k);
      }
    }).sorted().distinct().collect(Collectors.toList());
    validateGroupNames(groups);
    return groups;
  }

  private static void printProcessGroup(Map<String,String> config, PrintStream out,
      String processName, String propertyPrefix, String groupName) {

    String hosts = config.get(propertyPrefix + groupName + HOSTS_SUFFIX);
    if (hosts == null || hosts.isEmpty()) {
      throw new IllegalArgumentException("Group " + groupName + " found for " + processName
          + ", but no hosts. Check the format of your cluster.yaml file.");
    }
    out.printf(PROPERTY_FORMAT, processName + "_HOSTS_" + groupName, hosts);

    out.printf(PROPERTY_FORMAT, processName + "S_PER_HOST_" + groupName,
        config.getOrDefault(propertyPrefix + groupName + SERVERS_PER_HOST_SUFFIX, "1"));

    out.printf(PROPERTY_FORMAT, processName + "_CONF_DIR_" + groupName,
        config.getOrDefault(propertyPrefix + groupName + CONF_DIR_SUFFIX, ""));

    out.printf(PROPERTY_FORMAT, processName + "_EXTRA_ARGS_" + groupName,
        config.getOrDefault(propertyPrefix + groupName + EXTRA_ARGS_SUFFIX, ""));

  }

  public static void outputShellVariables(Map<String,String> config, PrintStream out) {

    // find invalid config sections and point the user to the first one
    config.keySet().stream().filter(VALID_CONFIG_SECTIONS.negate()).findFirst()
        .ifPresent(section -> {
          throw new IllegalArgumentException("Unknown configuration section : " + section);
        });

    // Process Manager
    printProcessGroup(config, out, "MANAGER", MANAGER_PREFIX, DEFAULT_RESOURCE_GROUP_NAME);

    // Process Garbage Collector
    printProcessGroup(config, out, "GC", GC_PREFIX, DEFAULT_RESOURCE_GROUP_NAME);

    // Process Monitor
    printProcessGroup(config, out, "MONITOR", MONITOR_PREFIX, DEFAULT_RESOURCE_GROUP_NAME);

    // Process Compactor groups
    List<String> compactorGroups = parseGroup(config, COMPACTOR_PREFIX);
    if (compactorGroups.isEmpty()) {
      throw new IllegalArgumentException(
          "No compactor groups found, at least one compactor group is required to compact the system tables.");
    } else {
      out.printf(PROPERTY_FORMAT, "COMPACTOR_GROUPS",
          compactorGroups.stream().collect(Collectors.joining(" ")));
      for (String group : compactorGroups) {
        printProcessGroup(config, out, "COMPACTOR", COMPACTOR_PREFIX, group);
      }
    }

    // Process ScanServer groups
    List<String> sserverGroups = parseGroup(config, SSERVER_PREFIX);
    if (!sserverGroups.isEmpty()) {
      out.printf(PROPERTY_FORMAT, "SSERVER_GROUPS",
          sserverGroups.stream().collect(Collectors.joining(" ")));
      for (String group : sserverGroups) {
        printProcessGroup(config, out, "SSERVER", SSERVER_PREFIX, group);
      }
    }

    // Process TabletServer groups
    List<String> tserverGroups = parseGroup(config, TSERVER_PREFIX);
    if (tserverGroups.isEmpty()) {
      throw new IllegalArgumentException(
          "No tserver groups found, at least one tserver group is required to host the system tables.");
    } else {
      out.printf(PROPERTY_FORMAT, "TSERVER_GROUPS",
          tserverGroups.stream().collect(Collectors.joining(" ")));
      for (String group : tserverGroups) {
        printProcessGroup(config, out, "TSERVER", TSERVER_PREFIX, group);
      }
    }

    out.flush();
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "Path provided for output file is intentional")
  public static void main(String[] args) throws IOException {
    if (args == null || args.length < 1 || args.length > 2) {
      System.err.println("Usage: ClusterConfigParser <configFile> [<outputFile>]");
      System.exit(1);
    }

    try {
      if (args.length == 2) {
        // Write to a file instead of System.out if provided as an argument
        try (OutputStream os = Files.newOutputStream(Paths.get(args[1]), StandardOpenOption.CREATE);
            PrintStream out = new PrintStream(os)) {
          outputShellVariables(parseConfiguration(args[0]), new PrintStream(out));
        }
      } else {
        outputShellVariables(parseConfiguration(args[0]), System.out);
      }
    } catch (Exception e) {
      System.err.println("Processing error: " + e.getMessage());
      System.exit(1);
    }
  }

}
