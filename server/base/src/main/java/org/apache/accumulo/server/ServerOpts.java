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
package org.apache.accumulo.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.CliConfiguration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.IParameterSplitter;

public class ServerOpts extends Help {
  @Parameter(names = {"-a", "--address"}, description = "address to bind to")
  private String address = null;

  public static class NullSplitter implements IParameterSplitter {
    @Override
    public List<String> split(String value) {
      return Collections.singletonList(value);
    }
  }

  @Parameter(names = "-o", splitter = NullSplitter.class,
      description = "Overrides configuration set in accumulo-site.xml (but NOT system-wide config set in Zookeeper). Expected format: -o <key>=<value>")
  private List<String> properties = new ArrayList<>();

  public String getAddress() {
    if (address != null)
      return address;
    return "0.0.0.0";
  }

  public List<String> getProperties() {
    return properties;
  }

  public Map<String,String> getConfig() {
    Map<String,String> config = new HashMap<>();
    for (String prop : getProperties()) {
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
    CliConfiguration.set(getConfig());
    if (getConfig().size() > 0) {
      CliConfiguration.print();
    }
  }
}
