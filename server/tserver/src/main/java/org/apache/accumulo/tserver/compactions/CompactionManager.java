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
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactionServices;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.fate.util.Retry;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.tserver.TabletServerResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionManager {

  private static final Logger log = LoggerFactory.getLogger(CompactionManager.class);

  private Iterable<Compactable> compactables;
  private Map<CompactionServiceId,CompactionService> services;

  private LinkedBlockingQueue<Compactable> compactablesToCheck = new LinkedBlockingQueue<>();

  private long maxTimeBetweenChecks;

  private void mainLoop() {
    long lastCheckAllTime = System.nanoTime();

    long increment = Math.max(1, maxTimeBetweenChecks / 10);

    var retryFactory = Retry.builder().infiniteRetries()
        .retryAfter(increment, TimeUnit.MILLISECONDS).incrementBy(increment, TimeUnit.MILLISECONDS)
        .maxWait(maxTimeBetweenChecks, TimeUnit.MILLISECONDS).backOffFactor(1.07)
        .logInterval(1, TimeUnit.MINUTES).createFactory();
    var retry = retryFactory.createRetry();
    Compactable last = null;

    while (true) {
      try {
        long passed = TimeUnit.MILLISECONDS.convert(System.nanoTime() - lastCheckAllTime,
            TimeUnit.NANOSECONDS);
        if (passed >= maxTimeBetweenChecks) {
          for (Compactable compactable : compactables) {
            last = compactable;
            compact(compactable);
          }
          lastCheckAllTime = System.nanoTime();
        } else {
          var compactable =
              compactablesToCheck.poll(maxTimeBetweenChecks - passed, TimeUnit.MILLISECONDS);
          if (compactable != null) {
            last = compactable;
            compact(compactable);
          }
        }

        last = null;
        if (retry.hasRetried())
          retry = retryFactory.createRetry();

      } catch (Exception e) {
        var extent = last == null ? null : last.getExtent();
        log.warn("Failed to compact {} ", extent, e);
        retry.useRetry();
        try {
          retry.waitForNextAttempt();
        } catch (InterruptedException e1) {
          log.debug("Retry interrupted", e1);
        }
      }
    }
  }

  private void compact(Compactable compactable) {
    for (CompactionKind ctype : CompactionKind.values()) {
      services.get(compactable.getConfiguredService(ctype)).compact(ctype, compactable,
          compactablesToCheck::add);
    }
  }

  public CompactionManager(Iterable<Compactable> compactables, ServerContext ctx,
      TabletServerResourceManager resourceManager) {
    this.compactables = compactables;

    Map<String,String> configs =
        ctx.getConfiguration().getAllPropertiesWithPrefix(Property.TSERV_COMPACTION_SERVICE_PREFIX);

    Map<CompactionServiceId,CompactionService> tmpServices = new HashMap<>();

    Map<String,String> planners = new HashMap<>();
    Map<String,Map<String,String>> options = new HashMap<>();

    configs.forEach((prop, val) -> {
      var suffix = prop.substring(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey().length());
      String[] tokens = suffix.split("\\.");
      if (tokens.length == 4 && tokens[1].equals("planner") && tokens[2].equals("opts")) {
        options.computeIfAbsent(tokens[0], k -> new HashMap<>()).put(tokens[3], val);
      } else if (tokens.length == 2 && tokens[1].equals("planner")) {
        planners.put(tokens[0], val);
      } else {
        throw new IllegalArgumentException("Malformed compaction service property " + prop);
      }
    });

    options.forEach((serviceName, serviceOptions) -> {
      tmpServices.put(CompactionServiceId.of(serviceName),
          new CompactionService(serviceName,
              planners.getOrDefault(serviceName, DefaultCompactionPlanner.class.getName()),
              serviceOptions, ctx, resourceManager));
    });

    this.services = Map.copyOf(tmpServices);

    this.maxTimeBetweenChecks = ctx.getConfiguration().getTimeInMillis(Property.TSERV_MAJC_DELAY);
  }

  public void compactableChanged(Compactable compactable) {
    compactablesToCheck.add(compactable);
  }

  public void start() {
    log.debug("Started compaction manager");
    new NamingThreadFactory("Compaction Manager").newThread(() -> mainLoop()).start();
  }

  public CompactionServices getServices() {
    return new CompactionServices() {
      @Override
      public Set<CompactionServiceId> getIds() {
        return services.keySet();
      }
    };
  }

  public boolean isCompactionQueued(KeyExtent extent, Set<CompactionServiceId> servicesUsed) {
    return servicesUsed.stream().map(services::get)
        .anyMatch(compactionService -> compactionService.isCompactionQueued(extent));
  }
}
