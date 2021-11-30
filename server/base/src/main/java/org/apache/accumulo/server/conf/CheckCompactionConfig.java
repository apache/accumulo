/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.conf;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.clientImpl.ClientInfoImpl;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.ExecutorManager;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;
import com.google.common.collect.Iterables;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@AutoService(KeywordExecutable.class)
public class CheckCompactionConfig implements KeywordExecutable {

  private final static Logger log = LoggerFactory.getLogger(CheckCompactionConfig.class);

  static class Opts extends Help {
    @Parameter(description = "<path/to/props/file>")
    String filePath;
  }

  @Override
  public String keyword() {
    return "check-compaction-config";
  }

  @Override
  public String description() {
    return "Checks compaction config";
  }

  public static void main(String[] args) throws IOException {
    new CheckCompactionConfig().execute(args);
  }

  @SuppressFBWarnings(value = "DM_EXIT",
      justification = "System.exit is fine here because it's a utility class executed by a main()")
  @Override
  public void execute(String[] args) throws IOException {
    Opts opts = new Opts();
    opts.parseArgs("accumulo check-compaction-config", args);

    if (opts.filePath == null) {
      System.err.println("No properties file was given");
      System.exit(1);
    }

    Path path = Path.of(opts.filePath);
    if (!path.toFile().exists()) {
      System.err.println("File at given path could not be found");
      System.exit(1);
    }

    // Extract properties from props file at given path
    Properties allProps = ClientInfoImpl.toProperties(path);
    log.debug("All props: {}", allProps);

    // Extract server props from set of all props
    Map<String,String> serverPropsMap = new HashMap<>();
    String suffix = ".accumulo.server.props";
    allProps.forEach((k, v) -> {
      if (k.toString().endsWith(suffix))
        serverPropsMap.put(k.toString(), v.toString());
    });

    // Ensure there is exactly one server prop in the map and get its value
    // The value should be compaction properties
    String compactionPropertiesString = "";
    try {
      compactionPropertiesString = Iterables.getOnlyElement(serverPropsMap.values());
    } catch (IllegalArgumentException e) {
      System.err.println("There should be exactly one server property. "+e.getMessage());
      System.exit(1);
    }

    // Create a props object for compaction props
    StringReader sr = new StringReader(compactionPropertiesString.replace(' ', '\n'));
    Properties serverProps = new Properties();
    serverProps.load(sr);

    // Extract executors options from compactions props
    Map<String,String> executorsProperties =
        getPropertiesWithSuffix(serverProps, ".planner.opts.executors");

    // Ensure there is exactly one executor config in the map and get its value
    String executorJson = "";
    try {
      executorJson = Iterables.getOnlyElement(executorsProperties.values());
    } catch (IllegalArgumentException e) {
      System.err.println("There should be exactly one executor config. "+e.getMessage());
      System.exit(1);
    }

    final String finalExecutorJson = executorJson;
    CompactionPlanner.InitParameters params = new CompactionPlanner.InitParameters() {
      @Override
      public ServiceEnvironment getServiceEnvironment() {
        return null;
      }

      @Override
      public Map<String,String> getOptions() {
        return Map.of("executors", finalExecutorJson);
      }

      @Override
      public String getFullyQualifiedOption(String key) {
        return null;
      }

      @Override
      public ExecutorManager getExecutorManager() {
        return new ExecutorManager() {
          @Override
          public CompactionExecutorId createExecutor(String name, int threads) {
            return CompactionExecutorIdImpl.externalId(name);
          }

          @Override
          public CompactionExecutorId getExternalExecutor(String name) {
            return CompactionExecutorIdImpl.externalId(name);
          }
        };
      }
    };

    try {
      new DefaultCompactionPlanner().parseExecutors(params);
    } catch (Exception e) {
      System.err.println("failed check: " + e.getMessage());
      System.exit(1);
    }
    System.out.println("Properties file has passed all checks.");
  }

  private static Map<String,String> getPropertiesWithSuffix(Properties serverProps, String suffix) {
    final Map<String,String> map = new HashMap<>();
    log.debug("Retrieving properties that end with '{}'", suffix);
    serverProps.forEach((k, v) -> {
      log.debug("{}={}", k, v);
      if (k.toString().endsWith(suffix))
        map.put((String) k, (String) v);
    });
    return map;
  }
}
