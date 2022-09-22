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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
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

  // catch all for string based dropped options, including those specific to subclassed extensions
  // uncomment below if needed
  // @Parameter(names = {}, hidden=true)
  private String legacyOpts = null;

  // catch all for boolean dropped options, including those specific to subclassed extensions
  @Parameter(names = {"-s", "--safemode"}, hidden = true)
  private boolean legacyOptsBoolean = false;

  // holds information on dealing with dropped options
  // option -> Message describing replacement option or property
  private static Map<String,String> LEGACY_OPTION_MSG = new HashMap<>();
  static {
    // garbage collector legacy options
    LEGACY_OPTION_MSG.put("-s", "Replaced by configuration property " + Property.GC_SAFEMODE);
    LEGACY_OPTION_MSG.put("--safemode",
        "Replaced by configuration property " + Property.GC_SAFEMODE);

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
    if (legacyOpts != null || legacyOptsBoolean) {
      String errMsg = "";
      for (String option : args) {
        if (LEGACY_OPTION_MSG.containsKey(option)) {
          errMsg +=
              "Option " + option + " has been dropped - " + LEGACY_OPTION_MSG.get(option) + "\n";
        }
      }
      errMsg += "See '-o' property override option";
      // prints error to console if ran from the command line otherwise there is no way to know that
      // an error occurred
      System.err.println(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    if (!getOverrides().isEmpty()) {
      log.info("The following configuration was set on the command line:");
      for (Map.Entry<String,String> entry : getOverrides().entrySet()) {
        String key = entry.getKey();
        log.info(key + " = " + (Property.isSensitive(key) ? "<hidden>" : entry.getValue()));
      }
    }
  }
}
