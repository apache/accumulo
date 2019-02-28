/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    if (propsPath == null) {
      propsPath = SiteConfiguration.getAccumuloPropsLocation().getFile();
    }
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
      siteConfig = new SiteConfiguration(new File(getPropertiesPath()), getOverrides());
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
      if (propArgs.length == 2) {
        String key = propArgs[0].trim();
        String value = propArgs[1].trim();
        if (key.isEmpty() || value.isEmpty()) {
          throw new IllegalArgumentException("Invalid command line -o option: " + prop);
        } else {
          config.put(key, value);
        }
      } else {
        throw new IllegalArgumentException("Invalid command line -o option: " + prop);
      }
    }
    return config;
  }

  @Override
  public void parseArgs(String programName, String[] args, Object... others) {
    super.parseArgs(programName, args, others);
    if (getOverrides().size() > 0) {
      log.info("The following configuration was set on the command line:");
      for (Map.Entry<String,String> entry : getOverrides().entrySet()) {
        String key = entry.getKey();
        log.info(key + " = " + (Property.isSensitive(key) ? "<hidden>" : entry.getValue()));
      }
    }
  }
}
