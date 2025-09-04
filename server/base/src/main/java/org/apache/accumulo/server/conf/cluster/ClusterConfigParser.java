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
package org.apache.accumulo.server.conf.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.ResourceGroupPropKey;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = {"LI_LAZY_INIT_STATIC", "PA_PUBLIC_PRIMITIVE_ATTRIBUTE"},
    justification = "visible for testing")
public class ClusterConfigParser {

  static void validateGroupNames(List<String> names) {
    for (String name : names) {
      if (!ResourceGroupId.GROUP_NAME_PATTERN.matcher(name).matches()) {
        throw new IllegalArgumentException("Group name: " + name + " contains invalid characters");
      }
    }
  }

  // visible for testing
  public static SiteConfiguration siteConf = null;

  private static final String PROPERTY_FORMAT = "%s=\"%s\"%n";
  private static final String COMPACTOR_PREFIX = "compactor.";
  private static final String GC_KEY = "gc";
  private static final String MANAGER_KEY = "manager";
  private static final String MONITOR_KEY = "monitor";
  private static final String SSERVER_PREFIX = "sserver.";
  private static final String TSERVER_PREFIX = "tserver.";

  private static final String HOSTS_SUFFIX = ".hosts";
  private static final String SERVERS_PER_HOST_SUFFIX = ".servers_per_host";

  private static final String[] UNGROUPED_SECTIONS =
      new String[] {MANAGER_KEY, MONITOR_KEY, GC_KEY};

  private static final Set<String> VALID_CONFIG_KEYS = Set.of(MANAGER_KEY, MONITOR_KEY, GC_KEY);

  private static final Set<String> VALID_CONFIG_PREFIXES =
      Set.of(COMPACTOR_PREFIX, SSERVER_PREFIX, TSERVER_PREFIX);

  private static final Predicate<String> VALID_CONFIG_SECTIONS =
      section -> VALID_CONFIG_KEYS.contains(section)
          || VALID_CONFIG_PREFIXES.stream().anyMatch(section::startsWith);

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
  public static Map<String,String> parseConfiguration(Path configFile) throws IOException {
    Map<String,String> results = new HashMap<>();
    try (InputStream fis = Files.newInputStream(configFile, StandardOpenOption.READ)) {
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
        } else {
          throw new IllegalArgumentException("Unknown group suffix for: " + k + ". Only "
              + HOSTS_SUFFIX + " or " + SERVERS_PER_HOST_SUFFIX + " are allowed.");
        }
      } else {
        throw new IllegalArgumentException("Malformed configuration, has too many levels: " + k);
      }
    }).sorted().distinct().collect(Collectors.toList());
    validateGroupNames(groups);
    return groups;
  }

  private static void validateConfiguredGroups(final ServerContext ctx, final Set<String> zkGroups,
      final List<String> configuredGroups, boolean createMissingRG) {
    for (String cg : configuredGroups) {
      if (!zkGroups.contains(cg)) {
        if (createMissingRG) {
          try {
            // cant use API as servers may not be up when this is called
            // from accumulo-cluster
            final ResourceGroupId rgid = ResourceGroupId.of(cg);
            final ResourceGroupPropKey key = ResourceGroupPropKey.of(rgid);
            key.createZNode(ctx.getZooSession().asReaderWriter());
          } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException("Error creating resource group: " + cg, e);
          }
        } else {
          throw new IllegalStateException(
              "Resource group configured that does not exist in ZooKeeper. ZK: " + zkGroups
                  + ", configured: " + cg);
        }
      }
    }
  }

  public static void outputShellVariables(ServerContext ctx, Map<String,String> config,
      Set<String> zkGroups, boolean createMissingRG, PrintStream out) {

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

    List<String> compactorGroups = parseGroup(config, COMPACTOR_PREFIX);
    if (compactorGroups.isEmpty()) {
      throw new IllegalArgumentException(
          "No compactor groups found, at least one compactor group is required to compact the system tables.");
    }
    validateConfiguredGroups(ctx, zkGroups, compactorGroups, createMissingRG);
    if (!compactorGroups.isEmpty()) {
      out.printf(PROPERTY_FORMAT, "COMPACTOR_GROUPS",
          compactorGroups.stream().collect(Collectors.joining(" ")));
      for (String group : compactorGroups) {
        out.printf(PROPERTY_FORMAT, "COMPACTOR_HOSTS_" + group,
            config.get(COMPACTOR_PREFIX + group + HOSTS_SUFFIX));
        String numCompactors =
            config.getOrDefault(COMPACTOR_PREFIX + group + SERVERS_PER_HOST_SUFFIX, "1");
        out.printf(PROPERTY_FORMAT, "COMPACTORS_PER_HOST_" + group, numCompactors);
      }
    }

    List<String> sserverGroups = parseGroup(config, SSERVER_PREFIX);
    validateConfiguredGroups(ctx, zkGroups, sserverGroups, createMissingRG);
    if (!sserverGroups.isEmpty()) {
      out.printf(PROPERTY_FORMAT, "SSERVER_GROUPS",
          sserverGroups.stream().collect(Collectors.joining(" ")));
      sserverGroups.forEach(ssg -> out.printf(PROPERTY_FORMAT, "SSERVER_HOSTS_" + ssg,
          config.get(SSERVER_PREFIX + ssg + HOSTS_SUFFIX)));
      sserverGroups.forEach(ssg -> out.printf(PROPERTY_FORMAT, "SSERVERS_PER_HOST_" + ssg,
          config.getOrDefault(SSERVER_PREFIX + ssg + SERVERS_PER_HOST_SUFFIX, "1")));
    }

    List<String> tserverGroups = parseGroup(config, TSERVER_PREFIX);
    if (tserverGroups.isEmpty()) {
      throw new IllegalArgumentException(
          "No tserver groups found, at least one tserver group is required to host the system tables.");
    }
    validateConfiguredGroups(ctx, zkGroups, tserverGroups, createMissingRG);
    AtomicBoolean foundTServer = new AtomicBoolean(false);
    if (!tserverGroups.isEmpty()) {
      out.printf(PROPERTY_FORMAT, "TSERVER_GROUPS",
          tserverGroups.stream().collect(Collectors.joining(" ")));
      tserverGroups.forEach(tsg -> {
        String hosts = config.get(TSERVER_PREFIX + tsg + HOSTS_SUFFIX);
        foundTServer.compareAndSet(false, hosts != null && !hosts.isEmpty());
        out.printf(PROPERTY_FORMAT, "TSERVER_HOSTS_" + tsg, hosts);
      });
      tserverGroups.forEach(tsg -> out.printf(PROPERTY_FORMAT, "TSERVERS_PER_HOST_" + tsg,
          config.getOrDefault(TSERVER_PREFIX + tsg + SERVERS_PER_HOST_SUFFIX, "1")));
    }

    if (!foundTServer.get()) {
      throw new IllegalArgumentException(
          "Tablet Server group found, but no hosts. Check the format of your cluster.yaml file.");
    }
    out.flush();
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "Path provided for output file is intentional")
  public static void main(String[] args) throws IOException {
    if (args == null || args.length < 1 || args.length > 3) {
      System.err.println(
          "Usage: ClusterConfigParser <createMissingResourceGroups> <configFile> [<outputFile>]");
      System.exit(1);
    }

    if (siteConf == null) {
      siteConf = SiteConfiguration.auto();
    }
    try (var context = new ServerContext(siteConf)) {
      // Login as the server on secure HDFS
      if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        SecurityUtil.serverLogin(siteConf);
      }
      final Set<String> zkGroups = new HashSet<>();
      context.resourceGroupOperations().list().forEach(rg -> zkGroups.add(rg.canonical()));
      if (!zkGroups.contains(ResourceGroupId.DEFAULT.canonical())) {
        throw new IllegalStateException("Default resource group not found in ZooKeeper");
      }
      boolean createMissingRG = args[0].equals("true") ? true : false;
      try {
        if (args.length == 3) {
          // Write to a file instead of System.out if provided as an argument
          try (OutputStream os = Files.newOutputStream(Path.of(args[2]));
              PrintStream out = new PrintStream(os)) {
            outputShellVariables(context, parseConfiguration(Path.of(args[1])), zkGroups,
                createMissingRG, out);
          }
        } else {
          outputShellVariables(context, parseConfiguration(Path.of(args[1])), zkGroups,
              createMissingRG, System.out);
        }
      } catch (Exception e) {
        System.err.println("Processing error: " + e.getMessage());
        throw e;
      }
    }
  }

}
