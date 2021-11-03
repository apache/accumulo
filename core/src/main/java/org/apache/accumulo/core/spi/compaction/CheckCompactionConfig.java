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
package org.apache.accumulo.core.spi.compaction;

import java.io.IOException;
import java.util.Objects;

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
    Preconditions.checkArgument(args.length == 1, "only one argument is accepted");

    String inputJSON = args[0];
    log.debug("Provided input: {}", inputJSON);

    DefaultCompactionPlanner.ExecutorConfig[] executorConfigs =
        new Gson().fromJson(inputJSON, DefaultCompactionPlanner.ExecutorConfig[].class);
    Objects.requireNonNull(executorConfigs);
    log.debug("parsed input: {}", executorConfigs.length);

    boolean hasSeenNullMaxSize = false;

    for (var executorConfig : executorConfigs) {
      System.out.println(executorConfig);

      // If not supplied, GSON will leave type null. Default to internal
      if (executorConfig.type == null) {
        System.out.println("WARNING: type is null. Defaulting to 'internal'. Please specify type.");
        executorConfig.type = "internal";
      }

      // check requirements for internal vs. external
      // lots of overlap with DefaultCompactionPlanner.init
      // TODO: Maybe refactor to extract commonalities
      switch (executorConfig.type) {
        case "internal":
          Preconditions.checkArgument(null == executorConfig.queue,
              "'queue' should not be specified for internal compactions");
          Objects.requireNonNull(executorConfig.numThreads,
              "'numThreads' must be specified for internal type");
          break;
        case "external":
          Preconditions.checkArgument(null == executorConfig.numThreads,
              "'numThreads' should not be specified for external compactions");
          Objects.requireNonNull(executorConfig.queue,
              "'queue' must be specified for external type");
          break;
        default:
          throw new IllegalArgumentException("type must be 'internal' or 'external'");
      }

      if (executorConfig.maxSize == null) {
        if (hasSeenNullMaxSize) {
          throw new IllegalArgumentException(
              "Can only have one executor w/o a maxSize." + executorConfig);
        } else {
          hasSeenNullMaxSize = true;
        }
      }
    }

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
