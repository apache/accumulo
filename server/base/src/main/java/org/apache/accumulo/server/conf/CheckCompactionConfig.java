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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.accumulo.core.clientImpl.ClientInfoImpl;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;

@AutoService(KeywordExecutable.class)
public class CheckCompactionConfig implements KeywordExecutable {

  private final static Logger log = LoggerFactory.getLogger(CheckCompactionConfig.class);

  public static void main(String[] args) throws IOException {
    if (args.length != 1)
      throw new IllegalArgumentException("Only one argument is accepted (path to properties file");

    Path path = Path.of(args[0]);
    if (!path.toFile().exists())
      throw new FileNotFoundException("Given input file was not found");

    // Extract server props from set of all props
    Properties serverProps = getPropertiesFromPath(path, "test.ci.common.accumulo.server.props");

    // Extract executors options from server props
    Map<String,String> executorsProperties =
        getPropertiesWithSuffix(serverProps, ".planner.opts.executors");

    // Ensure there is exactly one executor config in the file
    var executorPropsIterator = executorsProperties.entrySet().iterator();
    String executorJson = executorPropsIterator.next().getValue();
    if (executorPropsIterator.hasNext() || Objects.isNull(executorJson)) {
      throw new RuntimeException("Expected to find a single planner.opts.executors property");
    }

    // Convert json to array of ExecutorConfig objects
    var executorConfigs =
        new Gson().fromJson(executorJson, DefaultCompactionPlanner.ExecutorConfig[].class);
    if (Objects.isNull(executorConfigs)) {
      throw new RuntimeException("Compaction executors could not be parsed from given file");
    }

    boolean hasSeenNullMaxSize = false;

    for (var executorConfig : executorConfigs) {

      // If not supplied, GSON will leave type null.
      if (executorConfig.getType() == null) {
        throw new IllegalArgumentException("WARNING: 'type' is null. Please specify type.");
      }

      // check requirements for internal vs. external
      // lots of overlap with DefaultCompactionPlanner.init
      // TODO: Maybe refactor to extract commonalities
      switch (executorConfig.getType()) {
        case "internal":
          Preconditions.checkArgument(null == executorConfig.getQueue(),
              "'queue' should not be specified for internal compactions");
          Objects.requireNonNull(executorConfig.getNumThreads(),
              "'numThreads' must be specified for internal type");
          break;
        case "external":
          Preconditions.checkArgument(null == executorConfig.getNumThreads(),
              "'numThreads' should not be specified for external compactions");
          Objects.requireNonNull(executorConfig.getQueue(),
              "'queue' must be specified for external type");
          break;
        default:
          throw new IllegalArgumentException("type must be 'internal' or 'external'");
      }

      // Ensure maxSize is only seen once
      if (executorConfig.getMaxSize() == null) {
        if (hasSeenNullMaxSize) {
          throw new IllegalArgumentException(
              "Can only have one executor w/o a maxSize." + executorConfig);
        } else {
          hasSeenNullMaxSize = true;
        }
      }
    }
  }

  private static Map<String,String> getPropertiesWithSuffix(Properties serverProps, String suffix) {
    final Map<String,String> map = new HashMap<>();
    log.info("Retrieving properties that end with '{}'", suffix);
    serverProps.forEach((k, v) -> {
      log.info("{}={}", k, v);
      if (k.toString().endsWith(suffix))
        map.put((String) k, (String) v);
    });
    return map;
  }

  private static Properties getPropertiesFromPath(Path path, String property) throws IOException {
    Properties allProps = ClientInfoImpl.toProperties(path);
    String serverPropsString = (String) allProps.get(property);
    StringReader sr = new StringReader(serverPropsString.replace(' ', '\n'));
    Properties serverProps = new Properties();
    serverProps.load(sr);
    return serverProps;
  }

  @Override
  public String keyword() {
    return "check-compaction-config";
  }

  @Override
  public String description() {
    return "Checks compaction config";
  }

  @Override
  public void execute(String[] args) throws Exception {
    main(args);
  }
}
