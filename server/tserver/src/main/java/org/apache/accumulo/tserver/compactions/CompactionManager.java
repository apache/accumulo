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
package org.apache.accumulo.tserver.compactions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.LarsmaCompactionPlanner;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class CompactionManager {

  private static final Logger log = LoggerFactory.getLogger(CompactionManager.class);

  private Iterable<Compactable> compactables;
  private Map<CompactionServiceId,CompactionService> services;

  private LinkedBlockingQueue<Compactable> compactablesToCheck = new LinkedBlockingQueue<>();

  private void mainLoop() {
    long lastCheckAllTime = System.nanoTime();
    long maxTimeBetweenChecks = TimeUnit.SECONDS.toNanos(30); // TODO is this correct? TODO
                                                              // configurable
    while (true) {
      try {
        long passed = System.nanoTime() - lastCheckAllTime;
        if (passed >= maxTimeBetweenChecks) {
          for (Compactable compactable : compactables) {
            compact(compactable);
            // TODO come up with a better way to link these
            compactable.registerNewFilesCallback(compactablesToCheck::add);
          }
          lastCheckAllTime = System.nanoTime();
        } else {
          var compactable =
              compactablesToCheck.poll(maxTimeBetweenChecks - passed, TimeUnit.NANOSECONDS);
          if (compactable != null) {
            // TODO only run system compaction?
            compact(compactable);
          }
        }

      } catch (Exception e) {
        // TODO
        log.error("Loop failed ", e);
      }
    }
  }

  private void compact(Compactable compactable) {
    for (CompactionKind ctype : CompactionKind.values()) {
      services.get(compactable.getConfiguredService(ctype)).compact(ctype, compactable,
          compactablesToCheck::add);
    }
  }

  public CompactionManager(Iterable<Compactable> compactables, ServerContext ctx) {
    this.compactables = compactables;

    Map<String,String> configs =
        ctx.getConfiguration().getAllPropertiesWithPrefix(Property.TSERV_COMPACTION_SERVICE_PREFIX);

    Map<CompactionServiceId,CompactionService> tmpServices = new HashMap<>();

    Map<String,String> planners = new HashMap<>();
    Map<String,Map<String,String>> options = new HashMap<>();

    configs.forEach((prop, val) -> {
      var suffix = prop.substring(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey().length());
      String[] tokens = suffix.split("\\.");
      if (tokens[1].equals("opts")) {
        Preconditions.checkArgument(tokens.length == 3);
        options.computeIfAbsent(tokens[0], k -> new HashMap<>()).put(tokens[2], val);
      } else if (tokens[1].equals("planner")) {
        planners.put(tokens[0], val);
      } else {
        // TODO
      }
    });

    options.forEach((serviceName, serviceOptions) -> {
      tmpServices.put(CompactionServiceId.of(serviceName),
          new CompactionServiceImpl(serviceName,
              planners.getOrDefault(serviceName, LarsmaCompactionPlanner.class.getName()),
              serviceOptions, ctx));
    });

    this.services = Map.copyOf(tmpServices);
  }

  public void compactableChanged(Compactable compactable) {
    compactablesToCheck.add(compactable);
  }

  public void start() {
    // TODO deamon thread
    // TODO stop method
    log.info("Started compaction manager");
    new Thread(() -> mainLoop()).start();

    // TODO remove
    // new Thread(() -> printStats()).start();
  }
}
