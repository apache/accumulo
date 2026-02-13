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

public class ServerOpts extends Help {

  private static final Logger log = LoggerFactory.getLogger(ServerOpts.class);

  public static final String BIND_ALL_ADDRESSES = "0.0.0.0";

  public static class NullSplitter implements IParameterSplitter {
    @Override
    public List<String> split(String value) {
      return Collections.singletonList(value);
    }
  }

  @Parameter(names = "-o", splitter = NullSplitter.class,
      description = "Overrides configuration set in accumulo.properties (but NOT system-wide config"
          + " set in Zookeeper). This is useful when you have process specific configuration items"
          + " that are one-offs from a shared common configuration. Setting the bind address,"
          + " for example, can be done with the arguments \"-o general.process.bind.addr=127.0.0.1\"."
          + " Expected format: -o <key>=<value> [-o <key>=<value>]")
  private List<String> overrides = new ArrayList<>();

  private SiteConfiguration siteConfig = null;

  public synchronized SiteConfiguration getSiteConfiguration() {
    if (siteConfig == null) {
      String configFile = System.getProperty(SiteConfiguration.ACCUMULO_PROPERTIES_PROPERTY);
      if (configFile == null) {
        log.debug(
            "Creating SiteConfiguration from classpath. To use a specific file, set the system property 'accumulo.properties'");
      }
      siteConfig = SiteConfiguration.fromEnv().withOverrides(getOverrides()).build();
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
  public void validateArgs() {
    if (!getOverrides().isEmpty()) {
      log.info("The following configuration was set on the command line:");
      for (Map.Entry<String,String> entry : getOverrides().entrySet()) {
        String key = entry.getKey();
        log.info(key + " = " + (Property.isSensitive(key) ? "<hidden>" : entry.getValue()));
      }
    }
  }
}
