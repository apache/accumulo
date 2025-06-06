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
package org.apache.accumulo.core.cli;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.PropertyType;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.IParameterSplitter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ConfigOpts extends Help {

  private static final Logger log = LoggerFactory.getLogger(ConfigOpts.class);

  @Parameter(names = {"-p", "-props", "--props"}, description = "Sets path to accumulo.properties."
      + "The classpath will be searched if this property is not set")
  private String propsPath;

  public synchronized String getPropertiesPath() {
    return propsPath;
  }

  public static class NullSplitter implements IParameterSplitter {
    @Override
    public List<String> split(String value) {
      return Collections.singletonList(value);
    }
  }

  @Parameter(names = "-o", splitter = NullSplitter.class,
      description = "Overrides configuration set in accumulo.properties (but NOT system-wide config"
          + " set in Zookeeper). Expected format: -o <key>=<value>")
  private List<String> overrides = new ArrayList<>();

  private SiteConfiguration siteConfig = null;

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "process runs in same security context as admin who provided path")
  public synchronized SiteConfiguration getSiteConfiguration() {
    if (siteConfig == null) {
      String propsPath = getPropertiesPath();
      siteConfig = (propsPath == null ? SiteConfiguration.fromEnv()
          : SiteConfiguration.fromFile(new File(propsPath))).withOverrides(getOverrides()).build();
    }
    return siteConfig;
  }

  public Map<String,String> getOverrides() {
    return getOverrides(overrides);
  }

  public static Map<String,String> getOverrides(List<String> args) {
    Map<String,String> config = new HashMap<>();
    for (String prop : args) {
      String[] propArgs = prop.split("=", 2);
      String key = propArgs[0].trim();
      String value;
      if (propArgs.length == 2) {
        value = propArgs[1].trim();
      } else { // if property is boolean then its mere existence assumes true
        value = Property.isValidBooleanPropertyKey(key) ? "true" : "";
      }
      if (key.isEmpty() || value.isEmpty()) {
        throw new IllegalArgumentException("Invalid command line -o option: " + prop);
      }
      config.put(key, value);
    }
    return config;
  }

  @Override
  public void parseArgs(String programName, String[] args, Object... others) {
    super.parseArgs(programName, args, others);
    if (!getOverrides().isEmpty()) {
      log.info("The following configuration was set on the command line:");
      for (Map.Entry<String,String> entry : getOverrides().entrySet()) {
        String key = entry.getKey();
        log.info(key + " = " + (Property.isSensitive(key) ? "<hidden>" : entry.getValue()));
      }
    }
  }

  @Override
  public String getAdditionalHelpInformation(String programName) {

    final Set<String> validPrefixes = new HashSet<>();

    switch (programName) {
      case "compactor":
        validPrefixes.add(Property.COMPACTOR_PREFIX.getKey());
        break;
      case "compaction-coordinator":
        validPrefixes.add(Property.COMPACTION_COORDINATOR_PREFIX.getKey());
        break;
      case "gc":
        validPrefixes.add(Property.GC_PREFIX.getKey());
        break;
      case "manager":
        validPrefixes.add(Property.MANAGER_PREFIX.getKey());
        break;
      case "monitor":
        validPrefixes.add(Property.MONITOR_PREFIX.getKey());
        break;
      case "sserver":
        validPrefixes.add(Property.SSERV_PREFIX.getKey());
        break;
      case "tserver":
        validPrefixes.add(Property.TSERV_PREFIX.getKey());
        break;
      default:
        break;
    }

    if (validPrefixes.isEmpty()) {
      // We only provide extra help information for server processes
      return null;
    }

    // print out possible property overrides for the -o argument.
    validPrefixes.add(Property.GENERAL_PREFIX.getKey());
    validPrefixes.add(Property.RPC_PREFIX.getKey());

    // Determine format lengths based on property names and default values
    int maxPropLength =
        Stream.of(Property.values()).mapToInt(p -> p.getKey().length()).max().orElse(0);
    int maxDefaultLength = Stream.of(Property.values()).map(Property::getDefaultValue)
        .filter(Objects::nonNull).mapToInt(String::length).max().orElse(0);

    final String propOnlyFormat =
        "%1$-" + maxPropLength + "s %2$-" + Math.min(52, maxDefaultLength) + "s";
    final String deprecatedOnlyFormat = propOnlyFormat + " (deprecated)";
    final String replacedFormat = propOnlyFormat + " (deprecated - replaced by %3$s)";

    StringBuilder sb = new StringBuilder();
    sb.append(
        "\tBelow are the properties, and their default values, that can be used with the '-o' (overrides) option.\n");
    sb.append("\tLong default values will be truncated.\n");
    sb.append(
        "\tSee the user guide at https://accumulo.apache.org/ for more information about each property.\n");
    sb.append("\n");

    final SortedSet<Property> sortedProperties =
        new TreeSet<>(Comparator.comparing(Property::getKey));
    sortedProperties.addAll(EnumSet.allOf(Property.class));

    for (Property prop : sortedProperties) {
      if (prop.getType() == PropertyType.PREFIX) {
        continue;
      }
      final String key = prop.getKey();
      boolean valid = false;
      for (String prefix : validPrefixes) {
        if (key.startsWith(prefix)) {
          valid = true;
          break;
        }
      }
      if (!valid) {
        continue;
      }
      String value = prop.getDefaultValue();
      if (value.length() > 40) {
        value = value.substring(0, 40) + " (truncated)";
      }
      if (!prop.isDeprecated() && !prop.isReplaced()) {
        sb.append(String.format(propOnlyFormat, key, value));
      } else if (prop.isDeprecated() && !prop.isReplaced()) {
        sb.append(String.format(deprecatedOnlyFormat, key, value));
      } else {
        sb.append(String.format(replacedFormat, key, value, prop.replacedBy().getKey()));
      }
      sb.append("\n");
    }
    return sb.toString();
  }

}
