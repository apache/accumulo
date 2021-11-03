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
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import org.apache.accumulo.start.spi.KeywordExecutable;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;

@AutoService(KeywordExecutable.class)
public class CheckCompactionConfig implements KeywordExecutable {

  public static void main(String[] args) throws IOException {
    Preconditions.checkArgument(args.length == 1, "only one argument is accepted");
    Path propsPath = Paths.get(args[0]);
    System.out.println("file path provided: " + propsPath);

    DefaultCompactionPlanner.ExecutorConfig[] executorConfigs = null;
    try (Reader reader = Files.newBufferedReader(propsPath)) {
      System.out.println("Creating json from file");
      executorConfigs =
          new Gson().fromJson(reader, DefaultCompactionPlanner.ExecutorConfig[].class);
    } catch (IOException e) {
      e.printStackTrace();
    }

    assert executorConfigs != null;
    System.out.println("map size: " + executorConfigs.length);

    boolean hasSeenNullMaxSize = false;

    for (var executorConfig : executorConfigs) {
      System.out.println(executorConfig);

      // If not supplied, GSON will leave type null. Default to internal
      // warn user
      if (executorConfig.type == null) {
        System.out.println("WARNING: type is null. Defaulting to 'internal'. Please specify type.");
        executorConfig.type = "internal";
      }

      // check requirements for internal vs. external
      // lots of overlap with DefaultCompactionPlanner.init - Maybe refactor to extract commonality
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
